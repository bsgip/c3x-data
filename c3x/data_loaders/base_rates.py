"""
base_tariffs: defines basic tariff rates (what is commonly seen in energy pricing in the NEM)

It defines these pricing mechanics:
 - base_tariff: This is the class other tariffs derive from. Defines convinience functions
   to deal with time of use tariffs and others.
 - energy_tariff: This class is for energy ($/kWh) tariffs. 
 - demand_tariff: This class is for demand ($/kW/kVA) tariffs.
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
from c3x.data_cleaning import unit_conversion


#debug flag. Set to True to print debug messages
debug=True
#and little helper function
def _debugprinter(st):
    if debug:
        print(st)
#groupers
def _gbday(dt):
    #group by day, so just return date
    return dt.date()

def _gbmonth(dt):
    #make first of each month
    date=dt.date()
    return date.replace(day=1)

def _gbquarter(dt):
    #make first of each 3 month period
    date=dt.date()
    #get 3 month period
    month=3*math.floor(date.month/3)
    #and return first of every third month
    return date.replace(day=1,month=month)

def _gbyear(dt):
    #make first of first each year
    date=dt.date()
    return date.replace(day=1,month=1)    

def selfunction(period):
    """
    Return a fucntion to use in groupby that gorups by teh appropriate time period
    
    INPUTS:
    period: string defining period (day, month, quarter, year) 
    
    OUTPUTS:
    a function that groups appropriately
    """
    if period=="day":
        return _gbday
    elif period=="month":
        return _gbmonth
    elif period=="quarter":
        return _gbquarter
    elif period=="year":
        return _gbyear
    else:
        #return a function that simply returns the supplied value
        _debugprinter("Unrecognised group {}".format(period))
        def gbx(dt):
            return dt
        return gbx
    
def periodseconds(period):
    """
    return total number of seconds in a period
    INPUTS:
    period: string defining period (day, month, quarter, year) 
    
    OUTPUTS:
    number of seconds
    """
    if period=="day":
        return (_gbday(datetime.datetime(2020,1,2))-_gbday(datetime.datetime(2020,1,1))).total_seconds()
    elif period=="month":
        return (_gbmonth(datetime.datetime(2020,2,1))-_gbmonth(datetime.datetime(2020,1,1))).total_seconds()
    elif period=="quarter":
        return (_gbquarter(datetime.datetime(2020,4,1))-_gbquarter(datetime.datetime(2020,1,1))).total_seconds()
    elif period=="year":
        return (_gbyear(datetime.datetime(2020,1,1))-_gbyear(datetime.datetime(2021,1,1))).total_seconds()
    else:
        #return one second
        _debugprinter("Unrecognised group {}".format(period))
        return 1
    
        


class rate:
    """
    Rate class is the base class for electricity pricing rates.
    This class defines basic functions and some helpers
    
    USAGE:
    This class is not designed to be used itself, but it does define convinience functions that other instances can use. These
    are for handling common tariff manipulations and making analysis easier.
    
    Time helper:
    Many tariffs implement some form of time of use. Commonly this is time of day, day of week, or month of year. 
    Time is specified in rate["time"] and is designed for seasonal, weeky, and daily TOU rates. 
     - dates:    For tariffs that vary with seasons (e.g. summer, winter, etc). 
                 These are stored in "dates" and are a list of validity periods. Validity periods are a dict with the params:
                  - start: Start date
                  - end: end date
                  - validity: bool (True if valid between these date/times, False if invalid)
                 start and end dates are strings to be used with "datetime.strptime()". The default format is "%d/%m" but can be 
                 overridden with a string in "dateformat" to anything else supported by strptime. Unless year is specified as part 
                 of this string it is ignored. Time is always ignored.
                 
     - days:     A list of days of the week which the tarriff applies. These days are as returned by datetime.isoweekday() and are 
                 in the range [1-7] where 1 is Monday and 7 is Sunday
     
     - times:    For tariffs that applie for part of thed ay (e.g. peak times). 
                 This is formatted similarly to dates and is a list of validity time periods. The default format is "%H:%M" but
                 can be overridden with the "timeformat" parameter.
                 
    In the rate class this is implemented by two helper functions:
     - _initperiods: Initialises periods based on the supplied dict (usually rate["time"])
     - inperiod: returns True if datetime is within the specified period
     - inperioddf: returns a series, where value is True if datetime is within the specified period
     

    
    """
    def __init__(self,options):
        """
        Initialisation
        For the base class we only save some standard parameters, specifically:
         - name: the tariff name
         - columns: column types that the rate needs ('energy', 'power')
        
        INPUTS:
        options: dict of options
        
        OUTPUTS:
        None
        """
        #save name
        self.name=options["name"]
        #and tariff type. This is used ot determine outputs when we only want energy, demand, etc components
        self.type="none"
        #now save the column and column type for the default input column
        try:
            self._column=options["column"]
        except:
            #assume it is "energy"
            self._column="energy"
        #now the column type. Is the convtokwh flag that defines if we need ot convert it to kwh
        try:
            self._convertokwh=options["convtokwh"]
        except:
            #assume we don't need to (in line with default column name) 
            self._convertokwh=False
        #get direction. Can be 'consumption' or 'feedin'. all others are assumed bidirectional
        #these are from 
        try:
            self._direction=options["direction"]
        except:
            #default to consumption
            self._direction="consumption"
        #and a verbose flag
        try:
            self._verbose=options["verbose"]
        except:
            self._verbose=False
            
    def _print(self,string):
        """
        Print a messge dpenedign on debug flag (self._verbose)
        """
        if self._verbose:
            print(string)
    
    def _getcolumn(self,meas,column=None):
        """
        Get the data column and run all neccesary conversions. this includes:
         - get column
         - convert kW to kWh if necesary
         - zero out feedin or consumtion if neccesary 
         
        INPUTS:
        meas: Input dataframe
        column: optional column override
        
        OUTPUTS:
        a dataframe containing just the column and converted as required.
        """
        #first, get the series of the data. do a try/except because we want to give a sensible error
        if column is None:
            column=self._column
        try:
            energy=pd.DataFrame({column:meas[column]})
        except KeyError as e:
            self._print("Error getting data column {} for tariff element {}!".format(column,self.name))
            raise e
        #now convert ot kWh if we want to do that. 
        if self._convertokwh:
            #get timestep. assume it's consistent
            timestep=np.timedelta64(energy.index[1] - energy.index[0])
            #now do the covnersion
            energy=unit_conversion.convert_watt_to_watt_hour(energy, timedelta=timestep)
        #Now zero out the bits we don't want
        if self._direction=="consumption":
            energy[column]=energy[column].where(energy[column]>=0).fillna(value=0.0)
        elif self._direction=="feedin":
            energy[column]=energy[column].where(energy[column]<=0).fillna(value=0.0)
        return energy
   
    def _initperiods(self,periods):
        """
        Initialise periods and store them in self._dateperiods, self._dayperiods, and self._timeperiods
        
        INPUTS:
        periods: Period configuration dict
        
        OUTPUTS:
        None
        """
        #first, do date periods
        try:
            #get list
            dates=periods["dates"]
            #and empty list for periods
            self._dateperiods=[]
        except KeyError:
            #was no dates, so we have a simple function that returns true
            def df(testdate):
                return True
            self._dateperiods=[df]
            dates=[]
        try:
            dateformat=periods["dateformat"]
        except:
            dateformat="%d/%m"
        #work out if date format includes year
        self._datesinclyears="%Y" in dateformat
        #now loop through dates and add to list. This list contains functions which return True
        #if specified datetime is in the range
        #standard datehelper function
        def datehelper(testdatetime,fromdate,todate,validity,overday,changeyear=not self._datesinclyears):
            """
            Date helper. used to assess a supplied date (is it within when we want...?)
            
            INPUTS:
            testdatetime: Date/time to test
            fromdate: start date
            todate: end date
            valididty: True if date is valid within period, False if date is invalid within period
            overyear: True if we go over the end of the year (e.g. summer)
            changeyear: True if we want to ignore year in analysis
            """
            #convert test datetime to a date. 
            testdate=testdatetime.date()
            #set year if thats what we need to do
            if changeyear:
                testdate=testdate.replace(year=1900)
            #now test
            if overday:
                res=testdate>=fromdate or testdate<todate
            else:
                res=testdate>=fromdate and testdate<todate
            if validity:
                return res
            else:
                return not res       
        #now create the date periods
        for date in dates:
            #first, get dates,validity
            fromdate=datetime.datetime.strptime(date["start"],dateformat).date()
            todate=datetime.datetime.strptime(date["end"],dateformat).date()
            valid=date["validity"]
            self._print("For rate {} time period is: {} - {}: {}".format(
                self.name,fromdate.strftime(dateformat),todate.strftime(dateformat),valid))
            #and make function to append
            def df(testdate,fromdate=fromdate,todate=todate,validity=valid,overday=todate<fromdate):
                return datehelper(testdate,fromdate,todate,validity,overday)
            #and append it
            self._dateperiods.append(df)
        
        #now do a weekday function. There is only one of these, we just check against a list
        try:
            weekdays=periods["days"]
        except:
            #all days
            weekdays=[1,2,3,4,5,6,7]
        def weekdaychecker(testdatetime,days=weekdays):
            return testdatetime.isoweekday() in days
        self._dayperiods=weekdaychecker
        
        #Now time of day. This is similar to dates, a list of validity periods
        try:
            #get list of times
            times=periods["times"]
            #and empty list for periods
            self._timeperiods=[]
        except:
            #was no timesso we have a simple function that returns true
            def df(testdate):
                return True
            self._timeperiods=[df]
            times=[]
        try:
            timeformat=periods["timeformat"]
        except:
            timeformat="%H:%M"
        #now loop through dates and add to list. This list contains functions which return True
        #if specified datetime is in the range
        #standard datehelper function
        def timehelper(testdatetime,fromtime,totime,validity,overday):
            #covnert test datetime to a date. the year will be set to 1900 if inclyears is not set
            testtime=testdatetime.time()
            #now test
            if overday:
                res=testtime>=fromtime or testtime<totime
            else:
                res=testtime>=fromtime and testtime<totime
            if validity:
                return res
            else:
                return not res   
  
        #now create the date periods
        for time in times:
            #first, get dates,validity
            fromtime=datetime.datetime.strptime(time["start"],timeformat).time()
            totime=datetime.datetime.strptime(time["end"],timeformat).time()
            valid=time["validity"]
            self._print("For rate {} time period is: {} - {}: {}".format(
                self.name,fromtime.strftime(timeformat),totime.strftime(timeformat),valid))
            #and make function to append
            def df(testdatetime,fromtime=fromtime,totime=totime,validity=valid,overday=totime<fromtime):
                return timehelper(testdatetime,fromtime,totime,validity,overday)
            #and append it
            self._timeperiods.append(df)
    
    def tou_checker(self,dt):
        """
        tou_checker: Returns True if datetime supplied is a valid period for this tariff
        Valid periods are:
         - within any speifified date AND
         - within any specifed day of week AND
         - within any specified time
        
        INPUTS:
        dt: Datetime to check
        
        OUTPUTS:
        True if supplied datetime is valid
        """
        #big line 'o doom. Essentially what it does is check if the conditions in teh docstring are right.
        #max of an array of bools returns logical or
        return max([fcn(dt) for fcn in self._dateperiods]) and self._dayperiods(dt) and max([fcn(dt) for fcn in self._timeperiods])
    
    def tou_checker_dataframe(self,dtseries):
        """
        tou_checker_dataframe: returns a series where the value is True if the datetime is within the period
        
        INPUTS:
        dtseries: Datetime series
        
        OUTPUTS:
        Series
        """
        def af(rw):
            return self.tou_checker(rw.name)
        return dtseries.apply(af,axis=1)
    
    def generate_price_signal(self, meas: pd.DataFrame, local_tz: str = 'Australia/Sydney', args: dict = {}) -> pd.Series:
        """
        Generate timeseries price signal. for the current rate. 
        This generates a series that is the per unit cost for electricity consumed during the
        period of the timestep. We use generate_price and per-unitise it
        
        INPUTS:
        meas (pd.dataframe): Measurement data for which tariff is required (time index required)
        local_tz (str): timezone specified from the pytz list (pytz.all_timezones). (unclear of need)
        args: additional arguments (based in tariff)
        
        returns:
        tariff (pd.Series): A time-indexed series of pricing
        """
        #first, get the total cost 
        totalcost=self.generate_price(meas,local_tz,args)
        #now we need to convert to a unit cost. 
        unitcost=totalcost/meas[self._column]
        #and return
        return unitcost
    
    def generate_price(self, meas: pd.DataFrame, local_tz: str = 'Australia/Sydney', args: dict = {}) -> pd.Series:
        """
        Generate a timeseries price 
        In the base class this simply returns a series of zeroes. 
        
        INPUTS:
        meas (pd.dataframe): Measurement data for which tariff is required (time index required)
        local_tz (str): timezone specified from the pytz list (pytz.all_timezones). (unclear of need)
        args: additional arguments. Unused in this version
        
        returns:
        tariff (pd.FataFrame): A time-indexed series of pricing (for feeding into optimisers) 
        """
        return pd.Series(data=0.0,index=meas.index)
    
    def generate_enomo_prices(self,times):
        """
        Generate prices for enomo. What we output depends on tariff type. Base rate is none, so  that's what we output
        
        INPUTS:
        times: the times to generate the price signal for
        
        OUTPUTS:
        None
        """
        return {t:0 for t in times},{t:0 for t in times}
                   
class energy_rate(rate):
    """
    Energy rate class defines an energy ($/kWh) rate for electricity pricing.
    Each of these rates are for a single price and single time period. A time of use tariff will be built up
    from multiple instances of this class, oen for each price and time band. 
    
    Configuration:
    Energy rate is configured with two key variables: a rate and a valididty period.
    Rates are specified in $/kWh and are stored in rate["rate"]. Rates can be defined two ways:
     - If rate is a number it is simply $/kWh to all consumption
     - Inclining/declning block tariffs are supported by defining rate as a list. This list contains dicts that
       define the rate structure,
    Rate lists consist of dicts with the parameters:
     threshold: Energy threshold below which this rate applies. if not supplied it is infinite
     period: Period which the threshold applies to (can be "day" or "month". TBD: are there any that still do a block tariff 3 monthly?)
             Should all be the same. Only the first one is used. 
     rate: Rate ($/kWh) to apply
    """
    def __init__(self,options):
        """
        Initialisation. Set up tariff and init data structures
        
        INPUTS:
        options: Options dict
        
        OUTPUTS:
        None
        """
        #first, init super
        super(energy_rate,self).__init__(options)
        #and tariff type
        self.type="energy"
        #Now time periods. If these are empty we use an empty dict (which will always return true)
        try:
            super(energy_rate,self)._initperiods(options["periods"])
        except KeyError:
            super(energy_rate,self)._initperiods({})
        #now rates. 
        self._initrates(options["rate"])
        

        
    def _initrates(self,rates):
        """
        Initalise rate function/data structures
        
        INPUTS:
        rate: Rate or list of rates
        
        OUTPUTS:
        None
        """
        #first, what parameters exist depend on if rate is a number or a list
        try:
            self._rate=float(rates)
            self._flatrate=True
            return
        except TypeError:
            #typeerror on float() means that we have a list (hopefully, assume it is anyway)
            self._print("Rate list detected for rate {}".format(self.name))
            self._flatrate=False
        #in runtime we calculate rates using groupby on the rate, so we define the groupby function here
        #assume all periods are the same. If not it will be ...odd
        self._blockgrouper=selfunction(rates[0]["period"])
        #now a list of rate:threshold. 
        self._rates=[]
        #and default to free remaining energy
        self._remaining=0
        for rate in rates:
            #if threshold is missing we put in remaining parameter
            try:
                self._rates.append((rate["threshold"],rate["rate"]))
            except KeyError:
                self._remaining=rate["rate"]
               
    def generate_price(self, meas: pd.DataFrame, local_tz: str = 'Australia/Sydney', args: dict = {}) -> pd.Series:
        """
        Generate timeseries price for the current rate. 
        This generates a series that is the timeseries total cost for electricity consumed during the
        period of the timestep. 
        
        INPUTS:
        meas (pd.dataframe): Measurement data for which tariff is required (time index required)
        local_tz (str): timezone specified from the pytz list (pytz.all_timezones). (unclear of need)
        args: additional args. Unused in this method
        
        returns:
        tariff (pd.Series): A time-indexed series of pricing
        """
        #if we have a column override, use that
        try:
            column=args['column']
        except:
            column=self._column
        #get data
        energy=self._getcolumn(meas,column=column)
        #now we want to calculate the energy. 
        #There are two options: flat rate or block rate.
        #first though get the validity series
        validity=self.tou_checker_dataframe(energy)
        #now munge energy to only include valid periods
        energy[column]=energy[column]*validity
        #now do the calcs
        if self._flatrate:
            #is flat rate, so Uber simple
            return energy[column]*self._rate
        else:
            #is not a flat rate :-(
            #our desired output is timeseries price, so we essentially need to calulate a weighted
            #energy price (total cost/total energy) for each period. 
            #so to calcuate energy cost we need to know energy in the periods of itnerest, as the cost
            #depends on consumption
            #so through the magic of groupby we do that. We stored a function to group by at init.
            groups=energy.groupby(self._blockgrouper)
            #Now loop throught the groups and work out the total energy cost for each one.
            def groupcalculator(group):
                #get total energy
                total_energy=group.sum()
                if total_energy>0:
                    #calc total cost
                    remaining_energy=total_energy
                    total_cost=0
                    for rate in self._rates:
                        #get energy in this block
                        block_energy=min([remaining_energy,rate[0]])
                        #add total cost
                        total_cost=total_cost+block_energy*rate[1]
                        #and subtract this block from total
                        remaining_energy=remaining_energy-block_energy
                    #and calc remaining block cost and add
                    total_cost=total_cost+remaining_energy*self._remaining
                    #now calculate per energy cost
                    unit_cost=total_cost/total_energy
                else:
                    #no energy, no cost
                    unit_cost=0
                #and return
                return group*unit_cost
                
            #now apply and return
            return groups.transform(groupcalculator)[column]
        
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
        #the base (lowest) price
        if self._flatrate:
            rate=self._rate
        else:
            #block rate
            rate=min([r[1] for r in self._rates])
        signal={t:int(self.tou_checker(t))*rate for t in times}
        #and return
        if self._direction=="export":
            return {t:0 for t in times},signal
        else:
            return signal,{t:0 for t in times}
        
        
        
class demand_rate(rate):
    """
    Demand rate class defines a demand ($/kW or kVA) rate for electricity pricing.
    Each of these rates are for a single price and single time period. A time of use tariff will be built up
    from multiple instances of this class, oen for each price and time band. 
    This is based off of energy just like energy tariffs, as demand is ust energy over a period. 
    
    Configuration:
    Demand rate is configured with four key variables: 
     - A rate (how much demand costs)
     - A valididty period (how long each peak applies for e.g. monthly)
     - How long demand is measured over (in seconds)
        - Enter 0 for instantaneous. 
        - If different to sample rate multple timesteps will be averaged. 
          It is possible for a timestep to be in two demand periods, but we ignore this
     - how many peak demand events are averaged to calculate peak
    Rates are specified in $/kW or kVA and are stored in rate["rate"].     
    """
    def __init__(self,options):
        """
        Initialisation. Set up tariff and init data structures
        
        INPUTS:
        options: Options dict
        
        OUTPUTS:
        None
        """
        #first, init super
        super(demand_rate,self).__init__(options)
        #and tariff type. 
        self.type="demand"
        #Now time periods. If these are empty we use an empty dict (which will always return true)
        try:
            super(demand_rate,self)._initperiods(options["periods"])
        except KeyError:
            super(demand_rate,self)._initperiods({})
        #now rates. 
        try:
            self._rate=options["rate"]
        except:
            self._print("Error in demand rate {}: No rate specified. Assuming $0".format(self.name)) 
            self._rate=0
        #now validity period and measurement period
        try:
            self._validityperiod=options["validity"]
        except:
            self._print("Demand rate {} has no specified validity period: Assuming monthly".format(self.name))
            self._validityperiod="monthly"
        try:
            self._measureperiod=options["measureperiod"]
        except:
            self._print("Demand rate {} has no specified measure period: Assuming 5 minutes (300 seconds)".format(self.name))
            self._measureperiod=300
        #now we create a function to groupby for the validity period
        self._grouper=selfunction(self._validityperiod)
        #now number of periods to average
        try:
            self._peakaverages=options["peakaverages"]
        except:
            self._print("Demand rate {} doenst have peak average periods specified: Assuming 1".format(self.name))
            self._peakaverages=1
            
    def generate_price(self, meas: pd.DataFrame, local_tz: str = 'Australia/Sydney', args: dict = {}) -> pd.Series:
        """
        Generate timeseries price for the current rate. 
        This generates a series that is the timeseries total cost for electricity consumed during the
        period of the timestep. 
        Demand tariffs need to be a bit specific as to how to do this. Cost does not apply proportionally to each
        interval as it depends on the demand. If it is a peak period its worth lots, if it isnt it isn't 
        contributing to price. But for a optimiser we also need to know when it's "near" a peak too. So we have
        some options as to how to generate the price signal:
        
        peaks:
        This method all of the cost is split amongst all time intervals which contributed to the peak. It is split 
        by its ratio of the total energy consumed during peak times (e.g. interval/total)
        
        period:
        This method outputs one price each period (depending on settings) which is the total energy cost for the
        relevant period. This is the first interval in the period.
        
        increment:
        This method returns an indication of the impact of increasing (decreasing) the demand. It essentially aims
        to generate soemthing that look as much like an energy tariff as possible. It uses the incremental_energy
        function to generate (see docstring). methodparam args are passed into the function. 
        
        INPUTS:
        meas (pd.dataframe): Measurement data for which tariff is required (time index required)
        local_tz (str): timezone specified from the pytz list (pytz.all_timezones). (unclear of need)
        args: additional arguments
        
        returns:
        tariff (pd.Series): A time-indexed series of pricing
        """
        #get column
        try:
            column=args['column']
        except:
            column=self._column
        #get method from params
        try:
            method=args["method"]
        except:
            method="period"
            
        if method=="period":
            #first, make the interval prices
            intervals=self.generate_interval_prices(meas,False,column=column)
            #return series
            rseries=pd.Series(0.0,index=meas.index)
            #just set the appropriate rows
            rseries.loc[intervals.index]=intervals["cost"]
        elif method=="peaks":
            #set based on the interval's contribution to total demand
            #first, make the interval prices
            intervals=self.generate_interval_prices(meas,True,column=column)
            #return series
            rseries=pd.Series(0.0,index=meas.index)
            for iname,interval in intervals.iterrows():
                #first, sum
                rows=meas.loc[interval["peaks"]]
                sumdemand=rows[column].sum()
                #now assign by ratio
                intprices=interval["cost"]*rows/sumdemand
                #and put in series
                rseries[interval["peaks"]]=intprices[column]
        return rseries
            
       
        
    def generate_interval_prices(self, meas: pd.DataFrame,calcpeaks=False,column=None) -> pd.DataFrame:
        """
        Generate interval prices generates a Series of prices for the supplied data. The series contains
        an entry for each demand period and the total demand cost for that period
        
        INPUTS:
        meas (pd.dataframe): Measurement data for which tariff is required (time index required)
        local_tz (str): timezone specified from the pytz list (pytz.all_timezones). (unclear of need)
        
        returns:
        tariff (pd.Dataframe): A time-indexed series of data including price and break demand
        """
        if column is None:
            column=self._column
        #first, get the series of the data. do a try/except because we want to give a sensible error
        energy=self._getcolumn(meas,column=column)       
        #get timestep. assume it's consistent
        timestep=np.timedelta64(energy.index[1] - energy.index[0])       
        #now do ToU stuff. we zero out all data outside of ToU period
        #get the validity series
        validity=self.tou_checker_dataframe(energy)
        #now munge energy to only include valid periods
        energy[column]=energy[column]*validity
        #now we need to work out the cost for each period. First group
        periods=energy.groupby(self._grouper,axis=0)
        #now create a result Series. Index is the periods
        if calcpeaks:
            prices=pd.DataFrame(0.0,index=periods.groups.keys(),columns=["cost","demand","peaks"])
            #change data types of peaks column
            prices=prices.astype({"peaks":'object'})
        else:
            prices=pd.DataFrame(0.0,index=periods.groups.keys(),columns=["cost","demand"])
        #and work out how many seconds there are in a period. 
        totalseconds=periodseconds(self._validityperiod)
        #now loop through the groups and calculate
        for date,entries in periods:
            #first, calculate demands. To do this we need to sum energy over the periods of interest.
            #this is another groupby. We group by seconds elapsed between the time and the start time of our period
            startdatetime=datetime.datetime.combine(date,datetime.time())
            def demandperiodgrouper(dt,start=startdatetime,step=self._measureperiod):
                #first, get seconds since start
                seconds=(dt-start).total_seconds()
                #now in appropriate steps
                seconds=step*math.floor(seconds/step)
                #now calc datetime we want
                return start+datetime.timedelta(seconds=seconds)
            #now do the groupby and get the sum
            demandgroups=entries.groupby(demandperiodgrouper,axis=0)
            demands=demandgroups.sum()
            #and convert sum energy to power by dividing by number of hours
            demands=demands/(self._measureperiod/(60*60))
            #now get the top N rows and average them
            peakrows=demands.nlargest(self._peakaverages,columns=column)
            breakdemand=peakrows.mean(axis=0)[column]
            #Now calculate the cost. For a demand tariff this is breakdemand*cost.
            #We need to work out how much data we have out of the total period so we can scale
            #so total seconds within period. remember to add a timestep
            currentperiodseconds=(max(entries.index)-min(entries.index)).total_seconds()+timestep.item().total_seconds()
            #and calculate ratio
            ratio=currentperiodseconds/totalseconds
            #and the cost
            cost=breakdemand*ratio*self._rate
            #and we need to give the information needed to work out what periods are within the peaks.
            #this needs to be a list of row ids within the periods that defined the peak
            #surely there is a faster way to do this
            if calcpeaks:
                rowlist=[]
                #we return every row where the demand is the minimum of peakrows or higher
                #we use 99.9% of the peak so that we get the ones that are close but not quite (floating point errors)
                dem=peakrows[column].min()*0.999
                #and filter the dataframe. We 
                data=demands[demands[column]>=dem]
                for row,item in data.iterrows():
                    rowlist=rowlist+list(demandgroups.get_group(row).index)
                prices.at[date,"peaks"]=rowlist
            #save in the right bits of the result dict
            prices.loc[date,"cost"]=cost
            prices.loc[date,"demand"]=breakdemand            
        #return the frame
        return prices
       
    def generate_enomo_prices(self,times):
        """
        Generate prices for enomo. What we output depends on tariff type. 
        This is a demand price so we output validity signal, demand charge, and minimum demand.
        Minimum demand is always 0 because we don't support min demand (like SAPN) at the moment.
        
        INPUTS:
        times: the times to generate the price signal for
        
        OUTPUTS:
        validity signal, demand charge, and minimum demand
        """
        #validity
        signal={t:self.tou_checker(t) for t in times}
        demandcharge={t:int(self.tou_checker(t))*self._rate for t in times}
        #and return
        return signal, demandcharge, 0                  
   

