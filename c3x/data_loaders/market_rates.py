"""
market_rates: defines market prices. 
These are prices that are set from market data (bsgip server). There are two types of rates:
 - passive revenue rates are "addons" to other rates and generate revenue (market_fcas_rate)
 - energy price rates are energy costs that are defined by market price (market_energy_rate)
"""

import json
import importlib
import inspect
import sys
import traceback
import datetime
import math
import pandas as pd
import numpy as np
import pytz
import c3x.data_loaders.base_rates
from c3x.data_cleaning import unit_conversion
from sqlalchemy import create_engine


class base_market_price(c3x.data_loaders.base_rates.rate):
    """
    Base market price defines some convinience fucntions for makret rates (FCAS, energy)
    """
    def __init__(self,options):
        """
        Initialisation 
        Sets some default vars
        INPUTS:
        options: Options dict
        
        OUTPUTS:
        None
        """
        #first, init super
        super(base_market_price,self).__init__(options)
        #and region is genreal - we always need this
        self.region=options["region"]
        #and create a connection to the remote (market data) db
        try:
            connectionstring=options["connectionstring"]
        except:
            print("Rate {}: No connection string specified! Aborting".format(self.name))
            return
        try:
            self._engine=create_engine("postgresql://{}".format(connectionstring))
            self._connection=self._engine.connect()
        except:
            print("Rate {}: Error connecting to database with string {}".format(self.name,options["connectionstring"]))
            print("Rate {}: Have you SSH'ed into the server?")
            raise
        #we locally cache data in a database to make getting it quicker (we don't need to ask the remote server for data all
        #the time). We will have a dict with the key of the remote db table 
        try:
            self._cacheen=options["cache"]
        except:
            self._cacheen=True
        self._cache={}
        self._availbletime=None #will be a list of [start,end]
        
    def _getblock(self,start,end,table,qstr,tz):
        """
        Get a block of data from the remote database
        
        INPUTS:
        start: start datetime (local/no timezone)
        end: end datetime (local/no timezone)
        table: remote table
        qstr: Columns to get
        tz: Timezone of of data
        
        OUTPUTS:
        dataframe of data
        """
        starttime_loc=tz.localize(start)
        endtime_loc=tz.localize(end)
        #get the data
        rmd=pd.read_sql(
            con=self._connection,
            sql="SELECT {} FROM {} WHERE regionid='{}' AND settlementdate BETWEEN '{}' AND '{}'".format(
                qstr,table,self.region,starttime_loc.strftime("%Y-%m-%d %H:%M%z"),endtime_loc.strftime("%Y-%m-%d %H:%M%z")
            )
        )
        #now set datetime index.
        rmd['settlementdate']=pd.to_datetime(rmd['settlementdate'],format='%Y-%m-%d %H:%M:%S')
        #and convert to appropriate timezone
        rmd['settlementdate']=rmd['settlementdate'].dt.tz_convert(tz)
        #and make naiive again as the rest of our datetimes are naiive
        rmd['settlementdate']=rmd['settlementdate'].dt.tz_localize(None)
        #and make index
        rmd=rmd.set_index('settlementdate',drop=True)
        #and remove duplicates
        rmd=rmd[~rmd.index.duplicated(keep='first')]
        return rmd        

class fcas_band:
    """
    This class helps the market fcas rate assess value
    It is the "single band" value assessor. It takes in:
     - conversion between kW headroom and FCAS bid amount
     - price trace
    and outputs a timeseries revenue
    """
    def __init__(self,band,datacolumn,maxpcolumn,minpcolumn,table="dispatchis_price"):
        """
        Initialisation. Save the band we are bidding into        
        """
        #store source, max and min columns (columns in the supplied dataframe)
        self.datacolumn=datacolumn
        self.maxpcolumn=maxpcolumn
        self.minpcolumn=minpcolumn
        #store band name as supplied
        self.band=band
        self.mappedband=""
        #and work out the table/column in market data
        self.table=table
        self._raise=False
        if band.upper()=="R6":
            self.marketcolumn="raise6secrrp"
            self.mappedband="Fast Raise"
            self._raise=True
        elif band.upper()=="R60":
            self.marketcolumn="raise60secrrp"
            self.mappedband="Slow Raise"
            self._raise=True
        elif band.upper()=="R5":
            self.marketcolumn="raise5minrrp"
            self.mappedband="Delayed Raise"
            self._raise=True
        elif band.upper()=="RREG":
            self.marketcolumn="raiseregrrp"
            self.mappedband="Regulation Raise"
            self._raise=True
        elif band.upper()=="L6":
            self.marketcolumn="lower6secrrp"
            self.mappedband="Fast Lower"
            self._raise=True
        elif band.upper()=="L60":
            self.marketcolumn="lower60secrrp"
            self.mappedband="Slow Lower"
            self._raise=True
        elif band.upper()=="L5":
            self.marketcolumn="lower5minrrp"
            self.mappedband="Delayed Lower"
            self._raise=True
        elif band.upper()=="LREG":
            self.marketcolumn="lowerregrrp"
            self.mappedband="Regulation Lower"
            self._raise=True
        else:
            raise IndexError("Unknown Band: {}".format(band))
            
            
    def getdirection(self):
        """
        Get the direction information for this band
        We return:
         - the column of data we want
         - a function used to return the capacity
        """
        if self._raise:
            def fcn(power,cap):
                return cap-power
            return self.maxpcolumn,fcn
        else:
            def fcn(power, cap):
                return power-cap
            return self.minpcolumn,fcn
            

class market_fcas_rate(base_market_price):
    """
    Market FCAS rate defines revenue for resources bid into the FCAS market. This rate assesses the price 
    for a resource bid into all markets specified in configuration. 
    
    Configuration:
    This rate is specified by two key variables: 
     - which markets the resource is bid into
     - which columns define the raise and lower capacity
    """
    def __init__(self,options):
        """
        Initialisation, set the parameters and init data structures 
        
        INPUTS:
        options: Options dict
        
        OUTPUTS:
        None
        """
        #first, init super
        super(market_fcas_rate,self).__init__(options)
        #and tariff type
        self.type="revenue"
        #FCAS is in 5m dispatch time periods but priced as 30m average, which our time might not. So we have a multiplier to fix
        try:
            self._multiplier=options["intervalmultiplier"]
        except:
            self._multiplier=1
        #Column is saved in the superclass, but be aware its there for per unitising
        #now make a list ofg "bands"
        #bands are the individual FCAS bands the device is enabled for. 
        try:
            bands=options["bands"]
        except:
            self._print("Rate {}: Warning: no registered FCAS bands".format(self.name))
            bands=[]
        #now create the band objects
        self._bands=[]
        for name,band in bands.items():
            try:
                self._bands.append(fcas_band(**band))
                self._print("Rate {}: Band {} registered".format(self.name,self._bands[-1].mappedband))
            except IndexError:
                self._print("Rate {}: Error registering band: {}".format(self.name,name))
        
        
        
    def generate_price(self, meas: pd.DataFrame, local_tz: str = 'Australia/Brisbane', args: dict = {}) -> pd.Series:
        """
        Generate timeseries price for FCAS market value.
        This generates a series that is the timeseries total revenue for FCAS provided in the time windows 
        
        INPUTS:
        meas (pd.dataframe): Measurement data for which tariff is required (time index required)
        local_tz (str): timezone specified from the pytz list (pytz.all_timezones). (unclear of need)
        args: additional args. Unused in this method
        
        returns:
        tariff (pd.Series): A time-indexed series of pricing
        """
        #first, get the bounds of time we want data for
        starttime=min(meas.index)
        endtime=max(meas.index)
        #and a timezone object
        tz=pytz.timezone(local_tz)
        #We dont support overrides for simplicity. there is a lot of columns and it gets too complex
        #now get all the columns and the like for the data we need        
        querylist={} #is of the form: table:columns,...}
        bandslist={} #is of the form: band:{table:table,srccol:srccol,datacol:datacol,bandcol:bandcol,capfcn:capfcn}
        for band in self._bands:
            #now we need to save the data
            try:
                #is in the query list
                querylist[band.table].append(band.marketcolumn)
            except:
                querylist[band.table]=[band.marketcolumn]
            #and band data
            col,fcn=band.getdirection()
            bandslist[band.band]={
                "table":band.table,"srccol":band.marketcolumn,
                "datacol":band.datacolumn,"bandcol":col,"capfcn":fcn
            }
            
        #now we can get the data
        marketdata={}
        for table,columns in querylist.items():
            #first, see what data we already have. Assume we have all the required columns already
            #which is sensible as these are the same each run
            if self._availbletime is None or not self._cacheen or not table in self._cache.keys():
                #grab all data if first time, cache is disabled, or we dont have table data yet
                startblock=[starttime,endtime]
                endblock=None     
                self._availbletime=[starttime,endtime]
            else:
                #we have some data for this table. 
                #there amy be some data missing at the beginning and end
                #data is stored int eh cahce in timestamp corrected form
                #we assume there are no middle blocks missing, which may bite us.
                if self._availbletime[0]>starttime:
                    #have start data to get
                    startblock=[starttime,min(self._cache[table].index)]
                else:
                    #is OK
                    startblock=None
                if self._availbletime[1]<endtime:
                    #have end data to get
                    endblock=[max(self._cache[table].index),endtime]
                else:
                    #is OK
                    endblock=None
                #and save
                self._availbletime=[
                    min([starttime,self._availbletime[0]]),
                    max([endtime,self._availbletime[1]])
                ]
            #get the data
            #get list of columns
            qstr=",".join(["settlementdate"]+columns)
            #and read data
            if not startblock is None:
                self._print(
                    "Rate {}: Getting data for start block {} - {}".format(self.name,startblock[0].isoformat(),startblock[1].isoformat())) 
                data=self._getblock(startblock[0],startblock[1],table,qstr,tz)
                #and add to the data frame
                if table in self._cache.keys():
                    #adding to existing
                    self._cache[table]=self._cache[table].append(data)
                    #remove any dupes
                    self._cache[table]=self._cache[table][~self._cache[table].index.duplicated(keep='first')]
                    #and fix index
                    self._cache[table]=self._cache[table].sort_index()
                else:
                    self._cache[table]=data
                    self._cache[table]=self._cache[table].sort_index()
            if not endblock is None:
                self._print(
                    "Rate {}: Getting data for end block {} - {}".format(self.name,endblock[0].isoformat(),endblock[1].isoformat())) 
                data=self._getblock(endblock[0],endblock[1],table,qstr,tz)
                #and add to the data frame
                if table in self._cache.keys():
                    #adding to existing
                    self._cache[table]=self._cache[table].append(data)
                    #remove any dupes
                    self._cache[table]=self._cache[table][~self._cache[table].index.duplicated(keep='first')]
                    #and fix index
                    self._cache[table]=self._cache[table].sort_index()
                else:
                    self._cache[table]=data
                    self._cache[table]=self._cache[table].sort_index()
        #now calculate the revenue. We need the power trace, the market price trace (aligned) and the approrpiate capability trace
        total=pd.Series(0.0,index=meas.index)
        for bandname,banddata in bandslist.items():
            #Create a dataframe to put the results in
            cdf=pd.DataFrame({"power":meas[banddata["datacol"]].copy(),"cap":meas[banddata["bandcol"]]})
            #we want data in power, nto energy so we need to do the inverse of the normal kWh/kW conversion
            if not self._convertokwh:
                #get timestep. assume it's consistent
                timestep=np.timedelta64(cdf.index[1] - cdf.index[0]).total_seconds() 
                #now multiply. Data is in kWh and we want it in kW, so we divide by hours 
                cdf["power"]=cdf["power"]/(timestep/(60*60))                
            #now we need to add the market data. first as a series
            md=self._cache[banddata["table"]][banddata["srccol"]]
            #now redindex to get closest price to actual data.            
            md=md.reindex(cdf.index,method="nearest")
            #and add to the dataframe
            cdf["price"]=md
            #now create a capability column
            def fcn(row):
                return banddata["capfcn"](row["power"],row["cap"])
            cdf["bid"]=cdf.apply(fcn,axis=1)
            #now we create the return trace. 
            price=cdf["bid"]*cdf["price"]*self._multiplier/1000 #cos price is per MW, data is per kW
            #and add. remember that revenue is always negative price
            total-=price
        #and clear cache if we are not caching locally
        if not self._cacheen:
            self._cache={}
        return total

          

class market_energy_rate(base_market_price):
    """
    Market price rate is used where market price is used as part of a pricing structure.
    
    USAGE:
    This rate needs to have a region and price column. 
    
    """
    def __init__(self,options):
        """
        Initialisation, set the parameters and init data structures 
        
        INPUTS:
        options: Options dict
        
        OUTPUTS:
        None
        """
        #first, init super
        super(market_energy_rate,self).__init__(options)
        #and tariff type
        self.type="energy"
        #table
        try:
            self._table=options["table"]
        except KeyError:
            self._table="dispatchis_price"
        #and region
        self._marketcolumn=options["marketcolumn"] #which price trace to use
        #Market priuces are in energy ($/MWh) so conversion is already handled.
        #Also input data column is saved in the superclass, but be aware its there for per unitising
        
    def generate_price(self, meas: pd.DataFrame, local_tz: str = 'Australia/Sydney', args: dict = {}) -> pd.Series:
        """
        Generate timeseries price for the current rate. 
        This generates a series that is the timeseries total cost for electricity consumed during the
        period of the timestep. 
        
        INPUTS:
        meas (pd.dataframe): Measurement data for which tariff is required (time index required)
        local_tz (str): timezone specified from the pytz list (pytz.all_timezones).
        args: additional args. Unused in this method
        
        returns:
        tariff (pd.Series): A time-indexed series of pricing
        """
        try:
            column=args['column']
        except:
            column=self._column
        #first, get the bounds of time we want data for
        starttime=min(meas.index)
        endtime=max(meas.index)
        #and a timezone object
        tz=pytz.timezone(local_tz)
        #now we need to get the blocks of data
        marketdata={}
        #first, see what data we already have. Assume we have all the required columns already
        #which is sensible as these are the same each run
        if self._availbletime is None or not self._cacheen or not self._table in self._cache.keys():
            #grab all data if first time, cache is disabled, or we dont have table data yet
            startblock=[starttime,endtime]
            endblock=None     
            self._availbletime=[starttime,endtime]
        else:
            #we have some data for this table. 
            #there amy be some data missing at the beginning and end
            #data is stored int eh cahce in timestamp corrected form
            #we assume there are no middle blocks missing, which may bite us.
            if self._availbletime[0]>starttime:
                #have start data to get
                startblock=[starttime,min(self._cache[self._table].index)]
            else:
                #is OK
                startblock=None
            if self._availbletime[1]<endtime:
                #have end data to get
                endblock=[max(self._cache[self._table].index),endtime]
            else:
                #is OK
                endblock=None
            #and save
            self._availbletime=[
                min([starttime,self._availbletime[0]]),
                max([endtime,self._availbletime[1]])
            ]
        #get the data
        #get list of columns
        qstr=",".join(["settlementdate",self._marketcolumn])
        #and read data
        if not startblock is None:
            self._print(
                "Rate {}: Getting data for start block {} - {}".format(self.name,startblock[0].isoformat(),startblock[1].isoformat())) 
            data=self._getblock(startblock[0],startblock[1],self._table,qstr,tz)
            #and add to the data frame
            if self._table in self._cache.keys(): #empty things are false
                #adding to existing
                self._cache[self._table]=self._cache[self._table].append(data)
                #remove any dupes
                self._cache[self._table]=self._cache[self._table][~self._cache[self._table].index.duplicated(keep='first')]
                #and fix index
                self._cache[self._table]=self._cache[self._table].sort_index()
            else:
                self._cache[self._table]=data
                self._cache[self._table]=self._cache[self._table].sort_index()
        if not endblock is None:
            self._print(
                "Rate {}: Getting data for end block {} - {}".format(self.name,endblock[0].isoformat(),endblock[1].isoformat())) 
            data=self._getblock(endblock[0],endblock[1],self._table,qstr,tz)
            #and add to the data frame
            if self._table in self._cache.keys():
                #adding to existing
                self._cache[self._table]=self._cache[self._table].append(data)
                #remove any dupes
                self._cache[self._table]=self._cache[self._table][~self._cache[self._table].index.duplicated(keep='first')]
                #and fix index
                self._cache[self._table]=self._cache[self._table].sort_index()
            else:
                self._cache[self._table]=data
                self._cache[self._table]=self._cache[self._table].sort_index()
        #now calculate the cost. We need the power trace, the market price trace (aligned) and the approrpiate capability trace
        total=pd.Series(0.0,index=meas.index)
        #Get the source data
        energy=self._getcolumn(meas,column=column)
        #now we need to add the market data. first as a series
        md=self._cache[self._table][self._marketcolumn]
        #now redindex to get closest price to actual data.            
        md=md.reindex(energy.index,method="nearest")
        #and add to the dataframe
        energy["price"]=md
        #now create a capability column
        cost=energy[column]*energy["price"]/1000 #cos price is per MW, data is per kW
        #and clear cache if we are not caching locally
        if not self._cacheen:
            self._cache={}
        return cost
    
    def generate_enomo_prices(self,times, local_tz: str = 'Australia/Sydney'):
        """
        Generate prices for enomo. What we output depends on tariff type. 
        This is an energy price so we output import/export prices. We export zeroes on the invalid direction. 
        This could be a block tariff, but we just return the signal for low demands (lowest energy rate) 
        
        INPUTS:
        times: the times to generate the price signal for
        
        OUTPUTS:
        import_price, export_price
        """
        #get the price trace
        #first, get the bounds of time we want data for
        starttime=min(times)
        endtime=max(times)
        #and a timezone object
        tz=pytz.timezone(local_tz)
        #now we need to get the blocks of data
        marketdata={}
        #first, see what data we already have. Assume we have all the required columns already
        #which is sensible as these are the same each run
        if self._availbletime is None or not self._cacheen or not self._table in self._cache.keys():
            #grab all data if first time, cache is disabled, or we dont have table data yet
            startblock=[starttime,endtime]
            endblock=None     
            self._availbletime=[starttime,endtime]
        else:
            #we have some data for this table. 
            #there amy be some data missing at the beginning and end
            #data is stored int eh cahce in timestamp corrected form
            #we assume there are no middle blocks missing, which may bite us.
            if self._availbletime[0]>starttime:
                #have start data to get
                startblock=[starttime,min(self._cache[self._table].index)]
            else:
                #is OK
                startblock=None
            if self._availbletime[1]<endtime:
                #have end data to get
                endblock=[max(self._cache[self._table].index),endtime]
            else:
                #is OK
                endblock=None
            #and save
            self._availbletime=[
                min([starttime,self._availbletime[0]]),
                max([endtime,self._availbletime[1]])
            ]
        #get the data
        #get list of columns
        qstr=",".join(["settlementdate",self._marketcolumn])
        #and read data
        if not startblock is None:
            self._print(
                "Rate {}: Getting data for start block {} - {}".format(self.name,startblock[0].isoformat(),startblock[1].isoformat())) 
            data=self._getblock(startblock[0],startblock[1],self._table,qstr,tz)
            #and add to the data frame
            if self._table in self._cache.keys(): #empty things are false
                #adding to existing
                self._cache[self._table]=self._cache[self._table].append(data)
                #remove any dupes
                self._cache[self._table]=self._cache[self._table][~self._cache[self._table].index.duplicated(keep='first')]
                #and fix index
                self._cache[self._table]=self._cache[self._table].sort_index()
            else:
                self._cache[self._table]=data
                self._cache[self._table]=self._cache[self._table].sort_index()
        if not endblock is None:
            self._print(
                "Rate {}: Getting data for end block {} - {}".format(self.name,endblock[0].isoformat(),endblock[1].isoformat())) 
            data=self._getblock(endblock[0],endblock[1],self._table,qstr,tz)
            #and add to the data frame
            if self._table in self._cache.keys():
                #adding to existing
                self._cache[self._table]=self._cache[self._table].append(data)
                #remove any dupes
                self._cache[self._table]=self._cache[self._table][~self._cache[self._table].index.duplicated(keep='first')]
                #and fix index
                self._cache[self._table]=self._cache[self._table].sort_index()
            else:
                self._cache[self._table]=data
                self._cache[self._table]=self._cache[self._table].sort_index()
        #we want to return $/kWh indexed by time
        md=self._cache[self._table][self._marketcolumn]
        #now redindex to get closest price to actual data.            
        md=md.reindex(times,method="nearest")
        #and make data. 
        costs={t:r/1000 for t,r in md.iteritems()}
        if self._direction=="export":
            return {t:0 for t in times},costs
        else:
            return costs,{t:0 for t in times}
        
            
if __name__=="__main__":
    import random
    #fudge some data
    start=datetime.datetime(2020,1,1)
    end=datetime.datetime(2020,2,1)
    timestep=5
    steps=math.ceil((end-start).total_seconds()/(timestep*60))
    times=[start+datetime.timedelta(minutes=timestep*i) for i in range(steps+1)]
    power=[0.5 for t in times]
    maxp=[1 for t in times]
    minp=[0 for t in times]
    df=pd.DataFrame({"power":power,"maxp":maxp,"minp":minp},index=times)
    #and configuration
    connstring=input("Enter database connection string:")
    fcasparams={
        "connectionstring":connstring,
        "name":"Test FCAS rate",
        "region":"NSW1",
        "intervalmultiplier":0.166667,
        "verbose":True,
        "cache":True,
        "convtokwh":True,
        "bands":{
            "R6":{"band":"R6","datacolumn":"power","maxpcolumn":"maxp","minpcolumn":"minp","table":"dispatchis_price"},
            "L6":{"band":"L6","datacolumn":"power","maxpcolumn":"maxp","minpcolumn":"minp","table":"dispatchis_price"}            
        }
    }
    energyparams={
        "connectionstring":connstring,
        "name":"Test energy rate",
        "region":"NSW1",
        "verbose":True,
        "cache":True,
        "marketcolumn":"rrp",
        "column":"power",
        "convtokwh":True
    }
    
    #create the rate
    fcasrate=market_fcas_rate(fcasparams)
    energyrate=market_energy_rate(energyparams)
    #and get the data
    fcasrevenue=fcasrate.generate_price(df)
    energycost=energyrate.generate_price(df)
    print("Sum FCAS revenue new: {}".format(fcasrevenue.sum()))
    print("Sum energy cost: {}".format(energycost.sum()))
    
    #now try extra at end
    start=datetime.datetime(2020,2,1)
    end=datetime.datetime(2020,3,1)
    timestep=5
    steps=math.ceil((end-start).total_seconds()/(timestep*60))
    times=[start+datetime.timedelta(minutes=timestep*i) for i in range(steps+1)]
    power=[random.uniform(-1,1) for t in times]
    maxp=[1 for t in times]
    minp=[-1 for t in times]
    df=pd.DataFrame({"power":power,"maxp":maxp,"minp":minp},index=times)
    #and get the data
    fcasrevenue=fcasrate.generate_price(df)
    energycost=energyrate.generate_price(df)
    print("Sum FCAS revenue extra @ end: {}".format(fcasrevenue.sum()))
    print("Sum energy cost  extra @ end: {}".format(energycost.sum()))
    
    #now try extra at start
    start=datetime.datetime(2019,12,1)
    end=datetime.datetime(2020,2,1)
    timestep=5
    steps=math.ceil((end-start).total_seconds()/(timestep*60))
    times=[start+datetime.timedelta(minutes=timestep*i) for i in range(steps+1)]
    power=[random.uniform(-1,1) for t in times]
    maxp=[1 for t in times]
    minp=[-1 for t in times]
    df=pd.DataFrame({"power":power,"maxp":maxp,"minp":minp},index=times)
    #and get the data
    fcasrevenue=fcasrate.generate_price(df)
    energycost=energyrate.generate_price(df)
    print("Sum FCAS revenue extra @ start: {}".format(fcasrevenue.sum()))
    print("Sum energy cost  extra @ start: {}".format(energycost.sum())) 
    
    #now within
    start=datetime.datetime(2019,12,15)
    end=datetime.datetime(2020,2,15)
    timestep=5
    steps=math.ceil((end-start).total_seconds()/(timestep*60))
    times=[start+datetime.timedelta(minutes=timestep*i) for i in range(steps+1)]
    power=[random.uniform(-1,1) for t in times]
    maxp=[1 for t in times]
    minp=[-1 for t in times]
    df=pd.DataFrame({"power":power,"maxp":maxp,"minp":minp},index=times)
    #and get the data
    #and get the data
    fcasrevenue=fcasrate.generate_price(df)
    energycost=energyrate.generate_price(df)
    print("Sum FCAS revenue middle: {}".format(fcasrevenue.sum()))
    print("Sum energy cost middle: {}".format(energycost.sum())) 
    
    
    
        
        
    