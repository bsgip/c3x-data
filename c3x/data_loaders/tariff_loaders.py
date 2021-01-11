"""tariff_loaders.py can create a rate or load a tariff from json.

A tariff is a set of rates that are applied for different month, days of the week and
hours of the day. If a tariff is loaded an array is generated to hold the tariff information
for a full year. However usually the particular year doesn't matter.

The Tariff loaded can then be mapped to a certain time series (e.G the time index of a
measurement data set)

"""

import pandas as pd
import numpy as np
import json
import sqlalchemy
from c3x.data_cleaning import unit_conversion

DAYS_IN_WEEK = 7
HOURS_IN_DAY = 24
MONTH_IN_YEAR = 12
DAYS_OF_WEEK = range(DAYS_IN_WEEK)
HOURS_OF_DAY = range(HOURS_IN_DAY)
MONTHS_OF_YEAR = range(MONTH_IN_YEAR)


class Rate:
    """ A rate is defined as $ per kw/h. The rate is vaild for a certain time, defined by month,
     weekdays and hour.
    """

    def __init__(self, rate, from_weekday, to_weekday, from_hour, to_hour):
        """defines a rate  for a certain time in which it is vaild. Its assumed that it is
        valid for each month of the year

        Args:
            rate(float): Dollars
            from_weekday(int): start day for validity of rate
            to_weekday(int): end day for validity of rate
            from_hour: start hour for validity of rate
            to_hour: end end for validity of rate

        """
        self.from_weekday = from_weekday
        self.to_weekday = to_weekday
        self.from_hour = from_hour
        self.to_hour = to_hour
        self.rate = rate


def load_nem_prices_from_DB(table_name, database_name, start_time=None, end_time=None, region="NSW1"):
    """
        loads NEM prices for given interval and region from a database

        Note: The values in the database may be arranged in 5 minute intervals, due to the bidding
        processes in the energy market. You may find that you need to adjust your the start_time
        and end_time accordingly (see tests/tariff_database/nemweb_TRADINGPRICES.db)

        Args:
            table_name (str): Name of the nem-table
            database_name (str): Name of the database
            start_time  (datetime.datetime): desired starting time.
            end_time  (datetime.datetime): desired end time.
            region (str) : region for which trading prices are requested
        """

    # query the right amount of data instead of reading the hol think
    statement = "select * from " + table_name + " where REGIONID == " + "'" + region+"'"

    # Open database to read data data in
    database = sqlalchemy.create_engine('sqlite:///' + database_name)
    conn = database.connect()
    trading_price = pd.read_sql_query(statement, conn)
    trading_price = trading_price.set_index('time')
    conn.close()
    database.dispose()

    start_timestamp = start_time.timestamp()
    end_timestamp = end_time.timestamp()

    trading_price = trading_price.truncate(before=start_timestamp, after=end_timestamp)
    trading_price.index = pd.to_datetime(trading_price.index, unit='s')
    trading_price = trading_price.tz_localize('GMT').tz_convert('Australia/Sydney')

    # convert from MWh to kWh
    trading_price = trading_price[['RRP',
                                   'RAISE6SECRRP',
                                   'RAISE60SECRRP',
                                   'RAISE5MINRRP',
                                   'LOWER6SECRRP',
                                   'LOWER60SECRRP',
                                   'LOWER5MINRRP']] / 1000

    trading_price = trading_price.rename(columns={"RRP": "NEM_RRP"})
    # convert to $/kW/5min
    trading_price = trading_price.resample('5min').ffill()  # / 12

    return trading_price[:-1]


def datetime_tariff_map(datetime: pd.DatetimeIndex, month_day_hour_array):
    """
    A tariff is valid during a certain time range. The data to determine validity may be stored as
    month, day and hours. The month_day_hour_array is a collection of those times.


    Args:
        datetime (pd.Datetime.Index): datetime which is of interest
        month_day_hour_array (array): Array specifying rates at month, day and hour for 1 year.

    Returns:
        tariff: Mapped tariff information to specific timeframe

    """
    tariff_array = month_day_hour_array[datetime.month - 1, datetime.weekday, datetime.hour]
    return tariff_array


def load_tariff(data_location: str, filename: str, timestamp_index,
                datetime: pd.DatetimeIndex) -> pd.Series:
    """creates an array that holds information about tariffs for each hour of the year.
        the data is then mapped to the timestamps provided, creating a series of measurement
        timestamps and tariff information

        Args:
            data_location (str): path to data file.
            filename (str): file name of json tariff definition.
            timestamp_index: original index for the dataframe
            datetime (pd.Datetime.Index): datetime index for specified timezone.

        Returns:
            tariff_series (pd.Series): of tariff values
    """

    # creates an array to hold tariff information for each hour, day and moth of a year
    month_day_hour_array = np.zeros([MONTH_IN_YEAR, DAYS_IN_WEEK, HOURS_IN_DAY])

    with open(data_location + filename, 'r') as jf:
        raw_rates = json.load(jf)

        for r in raw_rates["rates"]:
            # create list to find in which time range the tariff is valid
            month_valid = MONTHS_OF_YEAR
            days_valid = range(r["from_weekday"], r["to_weekday"] + 1)
            hours_valid = range(r["from_hour"], r["to_hour"] + 1)

            # a tariff is build for a full year, if no rate can be applied for a specific time
            # the tariff is 0
            for month in MONTHS_OF_YEAR:
                if month in month_valid:
                    for day in DAYS_OF_WEEK:
                        if day in days_valid:
                            for hour in HOURS_OF_DAY:
                                if hour in hours_valid:
                                    month_day_hour_array[month, day, hour] += r["rate"]

    # maps a tariff (which is bound to a timezone) to a measurement index
    tariff_series = pd.Series(name='rate', index=timestamp_index,
                              data=datetime_tariff_map(datetime, month_day_hour_array))

    return tariff_series


def link_tariffs(meas: pd.DataFrame, data_location: str, filename: str,
                 local_tz: str = 'Australia/Sydney') -> pd.Series:
    """Tariffs are linked to a specific sample set of data. The series returned matches
    the timestamps given in the measurement data, the tariff returned match the tariff information
    in a specific timezone.

    Args:
        meas (pd.dataframe): Measurement data for which tariff is required (time index required)
        data_location (str): Location in which the tariff file can be found.
        filename (str): Specific file name for a tariff.
        local_tz (str): timezone specified from the pytz list (pytz.all_timezones).

    returns:
        tariff (pd.Series): A series of tariffs.
    """
    datetime_series = unit_conversion.local_t_from_dataframe_index(meas, local_tz=local_tz)
    tariff_series = load_tariff(data_location, filename, meas.index, datetime_series)

    return tariff_series
