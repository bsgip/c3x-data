"""
tariff_loaders.py
Loads or manages tariffs for use with energy analysis.
A tariff is simply a means of pricing electricity and this module is designed to be flexible to allow for many
different pricing structures. 

Tariffs are defined through two objects: Tariffs and Rates.

Tariffs
-------
Tariffs define a pricing structure. they consist of multiple rates and are essentially a way of grouping
multiple rates together. 
Tariffs are classes and have hte following signatures:
__init__(name=None,cfgfile=None,rates=None):
    Constructor. Takes in tariff information and stores
    
    INPUTS:
    name: Name of tariff
    cfgfile: path to configuration file or file like object
    tariffs: List of rates 

calculate(data: pd.DataFrame)->pd.Series
    Calculate the cost of energy for the supplied dataframe. 
    Returns a series if costs for eachitnerval. See Rates for how these are reported (particularly demand
    tariffs)

These are for compatability with old code. Functionality may be limited

datetime_tariff_map(datetime: pd.DatetimeIndex, month_day_hour_array)

load_tariff(data_location: str, filename: str, timestamp_index,
                datetime: pd.DatetimeIndex) -> pd.Series
                
link_tariffs(meas: pd.DataFrame, data_location: str, filename: str,
                 local_tz: str = 'Australia/Sydney') -> pd.Series:


Rates
-----
Rates define a single element of a pricing structure. A tariff is made up of one or more of these.
Rates are usually subclasses of the BaseRate class, but anythign with the appropriate methods will work. 
New tariff designs can be created by creating custom subclasses of BaseRate although a few common ones 
are defined here. 
BaseRate class defines the following functions:
__init__(name):
    Constructor. Base rate just takes a name. Subclasses takeother params

calculate(data: pd.DataFrame)->pd.Series
    Calculate the cost per this rate. All added in final result
    
createtimeseries
    
_touhelperinit(...)
    help TOU calculations initialisation (takes rate[time])
    
_touhelper(time)
    returns True if supplied datetime is in the TOU rate period
    
There are several subclasses defined in this module:
flat_rate: Fixed costs
energy_rate: Energy ($/kWh) rates
demand_rate: Demand ($/kW or $/kVA) rates
real_time_energy_rate: Realtime energy ($/kWh) rate with data from an external csv file

Their configuation is described in their docstrings
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

class tariff:
    """
    Tariff class 
    Tariffs are a electricity pricing strucutre. They come in many structures so this is designed to be
    as flexible as possible. 
    The Tariff class does not hold pricing information itself. This is the job of rate objects. Tariffs
    are a collection of rates that define different facets of the particular price structure.
    
    USAGE:
    tbd
    """
    def __init__(self,name=None,cfgfile=None,rates=[]):
        """
        Initialisation - set up tariff object
        
        INPUTS:
        name: Tariff name (used to name columns), optional
        cfgfile: Configuration file. Can be string (file path) or a file-like object
        rates: list of rates (overrides any for configuration file)
        
        OUTPUTS:
        None
        """
        #empty data strucutres
        self._rates={}
        #first thing we do is read the config file. User suppled parameters override file
        self._readfile(cfgfile)
        #now override the cfg file settigns with supplied
        if not name is None:
            self.name=name
        #rates
        for rate in rates:
            self._rates[rate.name]=rate
        
    def _readfile(self,cfgfile):
        """
        read configuration file
        
        INPUTS:
        cfgfile: Configuration file path or file-like object
        
        OUTPUTS:
        None
        """
        #file can either be a path or a file-like object. so we give it a go as a file
        #then try opening it if it fails
        #tariffs are JSON files 
        try:
            data=json.load(cfgfile)
        except AttributeError:
            #not a file. Attempt to open and read it
            _debugprinter("Tariff {} does not appear to be a file-like object. Opening as a file".format(cfgfile))
            with open(cfgfile,'r') as f:
                data=json.load(f)
        #now we need to parse it.
        #the only parameter we care about is name, the rest are rates.
        try:
            self.name=data['name']
        except:
            self.name='UNKNOWN'
        #and short name
        try:
            self.shortname=data["shortname"]
        except KeyError:
            self.shortname=self.name
        #Now there should be a list called 'rates'
        try:
            rates=data['rates']
        except:
            _debugprinter("No rates defined in config file for tariff {}".format(self.name))
            return
        #Go through the rates and add them
        for rate in rates:
            self.add_rate(rate)
            
    def add_rate(self,rate):
        """
        Add a rate to the tariff.
        Rates are defined in a dict, with the specific layout defined by the rate object.
        This method relies on two properties to select the rate to use: module and rate
         - module defines the module to use. 
           If not supplied this module is used (for a standard rate design)
         - rate defines the name of the class to use. Must be a subclass of BaseRate or an
           exception will be raised.
        If neither of these are defined it will assume it is the previous "compatability"
        format, which is EnergyRate (with the compatability flag set to True)
        
        INPUTS:
        rate: the rate to use
        
        OUTPUTS:
        None
        """
        #get the tariff name
        tname=rate["name"]
        #first, get the module and put it in the module object
        try:
            module=importlib.import_module(rate['module'])
        except KeyError:
            #no module, so use ourselves
            _debugprinter("No module defined for rate {} in tariff {}".format(tname,self.name))
            #use default. One day this should be more intelligent
            module=importlib.import_module("c3x.data_loaders.base_rates")
        except:
            #some other error
            print("Error importing requested module: {} for tariff {}, rate {}".format(rate['module'],self.name,tname))
            exctype,excvalue,exctraceback=sys.exc_info()
            traceback.print_tb(exctraceback)
            return
        #now the class. 
        try:
            classname=rate['type']
        except KeyError:
            _debugprinter("No class defined for rate {} in tariff {}. Assuming energy_rate".format(tname,self.name))
            classname='energy_rate'
        #now see if we can find the class in the module
        classes=[c[1] for c in inspect.getmembers(module) if inspect.isclass(c[1]) and c[0]==classname]
        #and use the first one we find, if there are any
        try:
            uclass=classes[0]
        except IndexError:
            #were none found, so write an error and exit
            print("Error! No candidate classes found for rate {} in tariff {}. Requested class: {} ".format(tname,self.name,classname))
            return
        #now we call the rate's constructor
        rateobj=uclass(rate)
        #and put in the rate dict
        self._rates[tname]=rateobj
        
    def tou_checker(self,datetimes: pd.DatetimeIndex):
        """
        Generates a dataframe where for each tariff there is a series that is "True" if it is valid, "False" if not.
        
        INPUTS:
        datetimes: DateTimeIndex of dates
        
        OUTPUTS:
        DataFrame of outputs. columns are tariff names
        """
        #create dataframe
        rdf=pd.DataFrame(False,index=datetimes,columns=['# tariffs']+list(self._rates.keys()))
        #apply
        for ratename in self._rates.keys():
            rate=self._rates[ratename]
            rdf[ratename]=rate.tou_checker_dataframe(rdf)
        #and calculate how many apply
        rdf['# tariffs']=rdf[list(self._rates.keys())].sum(axis=1)
        return rdf
    
    def link_tariffs(meas: pd.DataFrame, data_location: str=None, filename: str=None,
                 local_tz: str = 'Australia/Sydney') -> pd.Series:
        """
        link_tariffs returns a timeseries indication of energy price, matching the provided dataframe. 
        It is up to the rate how prices are presented, as it will depend on the design. This is particularly
        true for a demand tariff where the price is not purely time based. This method sums all the prices from
        all rates. 
        This method assumes all the required data is in the supplied dataframe. Tariffs may require some data to
        work. for example demand tariffs need to know connection point consumption.
        
        INPUTS:
        meas (pd.dataframe): Measurement data for which tariff is required (time index required)
        data_location (str): Location in which the tariff file can be found. Ignored for this call, kept for backward compatability
        filename (str): Specific file name for a tariff. Ignored for this call, kept for backward compatability
        local_tz (str): timezone specified from the pytz list (pytz.all_timezones). (unclear of need)
        
        returns:
        tariff (pd.Series): A time-indexed series of pricing (for feeding into optimisers)
        """
        #We use the pricing_dataframe method ot generate a price dataframe
        df=self.generate_price_signal(meas,local_tz)
        #just return sum
        return df["sum"]
        
    def generate_price_signal(self, meas: pd.DataFrame, local_tz: str = 'Australia/Sydney',args: dict = {}) -> pd.DataFrame:
        """
        Generate a dataframe containing a summed *unit* price and all the individual price components for the current tariff
        This is designed to go into an optimiser (such as enomo) to be optimised. 
        It is up to the rate how prices are presented, as it will depend on the design. This is particularly
        true for a demand tariff where the price is not purely time based. This method sums all the prices from
        all rates. 
        This method assumes all the required data is in the supplied dataframe. Tariffs may require some data to
        work. for example demand tariffs need to know connection point consumption.
        
        INPUTS:
        meas (pd.dataframe): Measurement data for which tariff is required (time index required)
        local_tz (str): timezone specified from the pytz list (pytz.all_timezones). (unclear of need)
        args: Arguments to supply calculation function
        
        returns:
        tariff (pd.FataFrame): A time-indexed series of pricing (for feeding into optimisers)        
        """
        ########################################################timezones
        #create dataframe, same as the columns and a sum
        rdf=pd.DataFrame(0,index=meas.index,columns=['sum']+list(self._rates.keys()))
        #apply
        for ratename in self._rates.keys():
            rate=self._rates[ratename]
            rdf[ratename]=rate.generate_price_signal(meas,local_tz,**args)
        #now sum. 
        rdf['sum']=rdf[list(self._rates.keys())].sum(axis=1)
        #and return
        return rdf
    
    def generate_price(self, meas: pd.DataFrame, local_tz: str = 'Australia/Sydney',args: dict = {}) -> pd.DataFrame:
        """
        Generate a dataframe containing a summed *total* price and all the individual price components for the current tariff
        It is up to the rate how prices are presented, as it will depend on the design. This is particularly
        true for a demand tariff where the price is not purely time based. This method sums all the prices from
        all rates. 
        This method assumes all the required data is in the supplied dataframe. Tariffs may require some data to
        work. for example demand tariffs need to know connection point consumption.
        
        INPUTS:
        meas (pd.dataframe): Measurement data for which tariff is required (time index required)
        local_tz (str): timezone specified from the pytz list (pytz.all_timezones). (unclear of need)
        args: Arguments to supply calculation function
        
        returns:
        tariff (pd.FataFrame): A time-indexed series of pricing     
        """
        ########################################################timezones
        #create dataframe, same as the columns and a sum
        rdf=pd.DataFrame(0,index=meas.index,columns=['sum']+list(self._rates.keys()))
        #apply
        for ratename in self._rates.keys():
            rate=self._rates[ratename]
            rdf[ratename]=rate.generate_price(meas,local_tz,args)
        #now sum. 
        rdf['sum']=rdf[list(self._rates.keys())].sum(axis=1)
        #and return
        return rdf
  
    def generate_optimiser_inputs(self,start,end,timestep):
        """
        Generate tariffs for inputting into the enomo optimiser. 
        This method outputs enomo style tariffs (one for energy, and one for demand). If there are no rates that satisfy
        the constraints (e.g. no energy rates), a tariff with zero cost is returned
        
        INPUTS:
        start: Start datetime
        end: End datetime
        timestep: timestep to use (in minutes)
        
        returns:
        enomo.models.Tariff :Energy rates
        enomo.models.DemandTariff: Demand rates
        """
        #create the time trace
        steps=math.ceil((end-start).total_seconds()/(timestep*60))
        #now fill the data
        times=[start+datetime.timedelta(minutes=timestep*i) for i in range(steps+1)]
        #Energy rate params
        import_tariff={t:0 for t in range(len(times))}
        export_tariff={t:0 for t in range(len(times))}
        #demand rate params
        active_periods={t:0 for t in range(len(times))}
        demand_charge={t:0 for t in range(len(times))}
        min_demand=0
        for ratename,rate in self._rates.items():
            #first, energy
            if rate.type=="energy":
                #energy tariff
                imp,exp=rate.generate_enomo_prices(times)
                import_tariff={t:import_tariff[t]+imp[times[t]] for t in range(len(times))} #enomo expects dict indexed by number...
                export_tariff={t:export_tariff[t]+exp[times[t]] for t in range(len(times))}
            elif rate.type=="demand":
                per,dc,mn=rate.generate_enomo_prices(times)
                active_periods={t:active_periods[t] or per[times[t]] for t in range(len(times))}
                demand_charge={t:demand_charge[t]+dc[times[t]] for t in range(len(times))}
                min_demand=max([min_demand,mn])
        #now we need to covnert demand charge cost into a signle value
        demand_charge=max([d for t,d in demand_charge.items()]) 
        #and return
        return import_tariff, export_tariff, active_periods, demand_charge, min_demand
        
                   
                   
          
            
        
    
if __name__=='__main__':
    #load an old and new style tariff
    newstyle=tariff(cfgfile="new_style_tariff.json")
    #check a datetime series with it
    dtidx=pd.date_range(datetime.date(2020,1,1),datetime.date(2020,1,2),freq="30m")
    dtframe=newstyle.validperiods(dtidx)
    print(dtframe.head(20))
        
        
        
        
      
        
        
