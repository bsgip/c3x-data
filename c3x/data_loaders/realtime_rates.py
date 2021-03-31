"""
Realtime rates. This is a rate that bases its data on a realtime rate stored in a database.
Esentially it takes a timeseries price from a database and applies it. Pretty simple. 
Defines realtime_energy_rate class
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

class realtime_energy_rate(c3x.data_loaders.base_rates.rate):
    """
    Realtime energy rate. This rate is used to apply a realtime ($/kWh) price to a demand trace. 
    """
    def __init__(self,options):
        """
        Initalisation 
        Sets some default vars
        INPUTS:
        options: Options dict
        
        OUTPUTS:
        None
        """
        #first, init super
        super(realtime_energy_rate,self).__init__(options)
        #and type
        self.type="energy"
        #now database. we need one of these
        try:
            db=options["database"]
            self._conn=create_engine("sqlite://{}".format(db), echo=False)
        except Exception as e:
            print("Rate {}: Error connecting to database!".format(self.name))
            raise e
        #now we need to get the other useful stuff. 
        try:
            self._dbcolumn=options["databasecolumn"]
        except Exception as e:
            print("Rate {}: Error getting database column!".format(self.name))
            raise e           
        try:
            self._timecolumn=options["timecolumn"]
        except Exception as e:
            print("Rate {}: Error getting database time column!".format(self.name))
            raise e
        try:
            self._table=options["table"]           
        except Exception as e:
            print("Rate {}: Error getting !".format(self.name))
            raise e
            
        
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
        #get the data
        try:
            column=args['column']
        except:
            column=self._column
        #get data
        energy=self._getcolumn(meas,column=column)
        
        #time bounds
        starttime=min(meas.index)
        endtime=max(meas.index)
        #now get data
        rddata=pd.read_sql("SELECT [{}],[{}] FROM {} WHERE {} BETWEEN '{}' AND '{}'".format(
            self._dbcolumn,self._timecolumn,self._table,self._timecolumn,
            starttime.strftime("%Y-%m-%d %H:%M"),endtime.strftime("%Y-%m-%d %H:%M")
        ),con=self._conn)
        #make the index
        rddata[self._timecolumn]=pd.to_datetime(rddata[self._timecolumn],format='%Y-%m-%d %H:%M:%S')
        rddata=rddata.set_index(self._timecolumn,drop=True)
        #now we need to align the data
        #redindex to get closest price to actual data.            
        rddata=rddata.reindex(energy.index,method="nearest")
        #and add to the dataframe
        energy["price"]=rddata
        #now calculate value
        cost=energy[column]*energy["price"]
        return cost
        
    def generate_enomo_prices(self,times):
        """
        Generate prices for enomo. What we output depends on tariff type. 
        This is an energy price so we output import/export prices. We export zeroes on the invalid direction. 
        This could be a block tariff, but we just return the signal for low demands (lowest energy rate) 
        
        INPUTS:
        times: the times to generate the price signal for
        
        OUTPUTS:
        import_price, export_price
        """
        #time bounds
        starttime=min(times)
        endtime=max(times)
        #now get data
        rddata=pd.read_sql("SELECT [{}],[{}] FROM {} WHERE {} BETWEEN '{}' AND '{}'".format(
            self._dbcolumn,self._timecolumn,self._table,self._timecolumn,
            starttime.strftime("%Y-%m-%d %H:%M"),endtime.strftime("%Y-%m-%d %H:%M")
        ),con=self._conn)
        #make the index
        rddata[self._timecolumn]=pd.to_datetime(rddata[self._timecolumn],format='%Y-%m-%d %H:%M:%S')
        rddata=rddata.set_index(self._timecolumn,drop=True)
        #now we need to align the data
        #redindex to get closest price to actual data.            
        rddata=rddata.reindex(times,method="nearest")
        costs={t:r[self._dbcolumn] for t,r in rddata.iterrows()}
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
    params={
        "name":"Locational Network Price",
        "column":"power",
        "convtokwh":True,
        "database":"/config/tariffs/RT_network.db",
        "databasecolumn":"Belconnen",
        "table":"pricedata",
        "timecolumn":"datetime"
    }
    
    #create the rate
    rate=realtime_energy_rate(params)
    #and get the data
    cost=rate.generate_price(df)
    print("Sum cost: {}".format(cost.sum()))
    