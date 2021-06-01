"""cleaners.py contains functions used to refine the contents of data sets.

All functions work on a pandas dataframe. At times, the order in which functions are called is
important, as some options may cause the insertion of NaNs, which may be undesired for future use.
Processing time may be reduced by reducing the data set to a specific time frame first.
"""

from datetime import datetime, timedelta
from time import mktime
import numpy
import pandas as pd


def duplicates_remove(dataframe: pd.DataFrame,
                      data_replacement: str = 'none',
                      removal_time_frame: str = 'day',
                      fault_placement: str = 'start') -> pd.DataFrame:
    """Removes duplicates for data frame.

    The cleaning method can be specified using the different parameters.

    Data replacement describes how  an error should be handled. None will remove all
    duplicate timestamps without refilling. One of the following data replacements methods
    must be used:
    - first : removes duplicates except for the first occurrence.
    - last : removes duplicates except for the last occurrence.
    - average: not implemented yet
    - max:  not implemented yet
    - remove: Removes the date to users specifications
    - none : no duplicate is kept.

    It may be of  interest to remove more data then the actual faulty data point. A hole day
    (by date) the hole data_set or some hours. One of the following removal_time_frames must
    be chosen:
    - day: 24 h of data will be removed
    - hour: 1h of data will be removed
    - all: all data will be removed

    The time range determine the position of the data point in the middle, at the end or at the
    start of the data. One of the following fault placements are possible:
    - start: fault is places at the beginning of the data that is removed (eg. 1h after the fault is removed)
    - middle: fault is places in the middle of the data that is removed (eg. 30 min before and after the fault is removed)
    - end: fault is places at the end of the data that is removed (eg. 1h before the fault is removed)

    Args:
        dataframe (pd.DataFrame): Dataframe with data to be checked for duplicates
        data_replacement (str, 'none'): Describes the way data shall be removed. Acceptable values
            are first, last, average, max, remove, none.
        removal_time_frame (str: 'day'): Describes the time frame that is removed. Acceptable values
            are day, hour, all.
        fault_placement (str, 'start'): Describes where the error is placed.

    Returns:
        dataframe (pd.DataFrame): Dataframe without duplicates.

    """
    # index.duplicate marks all occurrences as true, but for the indicated on in keep argument
    # to remove all duplicates the resulting array needs to be inverted
    if data_replacement == 'first':
        dataframe = dataframe[~dataframe.index.duplicated(keep='first')]

    elif data_replacement == 'last':
        dataframe = dataframe[~dataframe.index.duplicated(keep='last')]

    elif data_replacement == 'none':
        dataframe = dataframe[~dataframe.index.duplicated(keep=False)]

    elif data_replacement == 'average':
        print("not yet implemented (average)")

    elif data_replacement == 'max':
        print("not yet implemented (max)")

    elif data_replacement == 'remove':
        # here all data point that are duplicates are marked true
        index = dataframe.index[dataframe.duplicated(keep=False)]
        for timestamp in index:
            # slicing returns a dataframe from start to end
            # to remove a slice from a dataframe has to be done inverted.
            # 0 to start index and end index to end of dataframe need to be kept
            if removal_time_frame == 'day':
                index_date_start, index_date_end = find_time_range_method_day(timestamp,
                                                                              fault_placement)

                dataframe = slice_by_index(dataframe,
                                           timestamp_start=index_date_start,
                                           timestamp_end=index_date_end)
            elif removal_time_frame == 'hour':
                index_date_start, index_date_end = find_time_range_method_hour(timestamp,
                                                                               fault_placement)

                dataframe = slice_by_index(dataframe,
                                           timestamp_start=index_date_start,
                                           timestamp_end=index_date_end)
            elif removal_time_frame == 'all':
                dataframe = pd.Dataframe()

    return dataframe


def handle_non_numericals(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Replaces all non numerical values like strings in a dataframe with nan.

    Args:
        dataframe (pd.DataFrame): Dataframe with data to be checked for non numerical values.

    Returns:
        dataframe (pd.DataFrame): Dataframe with non numerical values changed to nans.

    """

    columns = list(dataframe)

    # to numeric can only handle scalars, lists, tuples, 1 - d arrays, or Series
    for column in columns:
        dataframe[column] = pd.to_numeric(dataframe[column], errors='coerce')

    return dataframe


def handle_nans(dataframe: pd.DataFrame, data_replacement: str = 'none',
                removal_time_frame: str = 'day', fault_placement: str = 'start') -> pd.DataFrame:
    """Removes NaNs from a dataframe.

    The cleaning method can be specified using the different parameters.

    Data replacement describes how  an error should be handled. None will remove all
    duplicate timestamps without refilling. One of the following data replacements methods
    must be used:
    - drop: NANs are dropped.
    - zero: Fills all NANs with 0 (value).
    - first: Uses the previous non - NAN occurrence.
    - last: Uses the next non - NAN occurrence.
    - none: Nothing is changed.
    - average: not implemented yet
    - max: not implemented yet
    - remove: Removes the date to users specifications.

    It may be of  interest to remove more data then the actual faulty data point. A hole day
    (by date) the hole data_set or some hours. One of the following removal_time_frames must
    be chosen:
    - day: 24 hours of data will be removed.
    - hour: 1 hour of data will be removed.
    - all: All data will be removed.

    The timerange determine the position of the data point in the middle, at the end or at the
    start of the data. One of the following fault placements are possible:
    - start: Fault is placed at the beginning of the data that is removed (eg. 1 hour after the fault is removed).
    - middle: Fault is placed in the middle of the data that is removed (eg. 30 min before and after the fault is removed).
    - end: Fault is placed at the end of the data that is removed (eg. 1 hour before the fault is removed).

    Args:
        dataframe (pd.DataFrame): Dataframe with data to be fixed.
        data_replacement (str, 'none'): Describes the way data shall be removed.
        removal_time_frame (str, 'day'): Describes the time frame that is removed.
        fault_placement (str, 'start'): Describes where the error is placed.

    Returns:
        dataframe (pd.DataFrame): Dataframe with NaNs removed.

    """

    if data_replacement == 'drop':
        dataframe = dataframe.dropna()

    elif data_replacement == 'zero':
        dataframe = dataframe.fillna(0)

    elif data_replacement == 'first':
        dataframe = dataframe.fillna(method='ffill')

    elif data_replacement == 'last':
        dataframe = dataframe.fillna(method='bfill')

    elif data_replacement == 'average':
        print("not yet implemented (average)")

    elif data_replacement == 'max':
        print("not yet implemented (max)")

    elif data_replacement == 'remove':
        # gets index for wrong signs
        index = dataframe.index[dataframe.isnull().any(axis=1)]

        # iterates through index
        for timestamp in index:
            # converts index to date time
            index_date_start = timestamp
            index_date_end = timestamp
            if removal_time_frame == 'day':
                index_date_start, index_date_end = find_time_range_method_day(timestamp,
                                                                              fault_placement)

                dataframe = slice_by_index(dataframe,
                                           timestamp_start=index_date_start,
                                           timestamp_end=index_date_end)

            elif removal_time_frame == 'hour':
                index_date_start, index_date_end = find_time_range_method_hour(timestamp,
                                                                               fault_placement)

                dataframe = slice_by_index(dataframe,
                                           timestamp_start=index_date_start,
                                           timestamp_end=index_date_end)

            elif removal_time_frame == 'all':
                dataframe = pd.DataFrame()

    return dataframe


def remove_negative_values(dataframe: pd.DataFrame, data_replacement: str = 'none',
                           removal_time_frame: str = 'day', fault_placement: str = 'start',
                           coloumn_index: int = 0) -> pd.DataFrame:
    """Handles the occurrence of negative values in a dataframe, which may be assumed
    to be faulty data points. The cleaning method can be specified using the different
    parameters

    Data replacement describes how  an error should be handled. None will remove all
    duplicate timestamps without refilling. One of the following data replacements methods
    must be used:
    - drop: NANs are dropped.
    - zero: Fills all NANs with 0 (value).
    - nan: Fills negative values with NaN's.
    - none: Nothing is changed.
    - remove: Removes the date to users specifications.

    It may be of  interest to remove more data then the actual faulty data point. A hole day
    (by date) the hole data_set or some hours. One of the following removal_time_frames must
    be chosen:
    - day: 24 hours of data will be removed.
    - hour: 1 hour of data will be removed.
    - all: All data will be removed.

    The time range determine the position of the data point in the middle, at the end or at the
    start of the data. One of the following fault placements are possible:
    - start: Fault is places at the beginning of the data that is removed (eg. 1 hour after the fault is removed)
    - middle: Fault is places in the middle of the data that is removed (eg. 30 minutes before and after the fault is removed)
    - end: Fault is places at the end of the data that is removed (eg. 1 hour before the fault is removed)

    Args:
        dataframe (pd.dataframe): Dataframe with data to be fixed
        data_replacement (str, 'none'): Describes the way data shall be removed. Acceptable values
            are drop, zero, nan, none, remove.
        removal_time_frame (str, 'day'): Describes the time frame that is removed. Acceptable values
            are day, hour, all.
        fault_placement (str, 'start'): Describes where the error is placed. Acceptable values are
            start, middle, end.
        coloumn_index(int): index of column that should be cleaned from negative values

    Returns:
        dataframe (pd.dataframe): Dataframe with cleaned data.

    """

    nu_negative_loads_occurrences = numpy.sum((dataframe.iloc[:, 0] < 0).values.ravel())

    if nu_negative_loads_occurrences > 0:
        if data_replacement == 'zero':
            dataframe[dataframe.iloc[:, coloumn_index] < 0] = 0

        elif data_replacement == 'nan':
            dataframe[dataframe.iloc[:, coloumn_index] < 0] = numpy.nan

        elif data_replacement == 'drop':
            dataframe.drop(dataframe[(dataframe.iloc[:, coloumn_index] < 0)].index, inplace=True)

        elif data_replacement == 'remove':
            # gets index for wrong signs
            index = dataframe[(dataframe.iloc[:, coloumn_index] > 0)].index

            # iterates through index
            for timestamp in index:
                # convertes index to date time
                index_date_start = timestamp
                index_date_end = timestamp

                if removal_time_frame == 'day':
                    index_date_start, index_date_end = find_time_range_method_day(timestamp,
                                                                                  fault_placement)

                    dataframe = slice_by_index(dataframe,
                                               timestamp_start=index_date_start,
                                               timestamp_end=index_date_end)

                elif removal_time_frame == 'hour':
                    index_date_start, index_date_end = find_time_range_method_hour(timestamp,
                                                                                   fault_placement)

                    dataframe = slice_by_index(dataframe,
                                               timestamp_start=index_date_start,
                                               timestamp_end=index_date_end)

                elif removal_time_frame == 'all':
                    dataframe = pd.DataFrame()
    return dataframe


def remove_positive_values(dataframe: pd.DataFrame, data_replacement: str = 'none',
                           removal_time_frame: str = 'day', fault_placement: str = 'start',
                           column_index: int = 0) -> pd.DataFrame:
    """Handles the occurrence of positive values in a dataframe, which may be assumed
    to be faulty data points. The cleaning method can be specified using the different
    parameters.

    Data replacement describes how an error should be handled.
    One of the following data replacements methods must be used:
    - drop: NANs are dropped.
    - zero: Fills all NANs with 0 (value).
    - nan: Fills negative values with NaN's.
    - none: Nothing is changed.
    - remove: Removes the date to users specifications.

    It may be of  interest to remove more data then the actual faulty data point. A hole day
    (by date) the hole data_set or some hours. One of the following removal_time_frames must
    be chosen:
    - day: 24 hour of data will be removed.
    - hour: 1 hour of data will be removed.
    - all: All data will be removed.

    The time range determine the position of the data point in the middle, at the end or at the
    start of the data. One of the following fault placements are possible:
    - start: Fault is places at the beginning of the data that is removed (eg. 1 hour after the fault is removed)
    - middle: Fault is places in the middle of the data that is removed (eg. 30 minutes before and after the fault is removed)
    - end: Fault is places at the end of the data that is removed (eg. 1 hour before the fault is removed)

    Args:
        dataframe (pd.dataframe): Dataframe with data to be fixed.
        data_replacement (str, 'none'): Describes the way data shall be removed. Acceptable values
            are drop, zero, nan, none, remove.
        removal_time_frame (str, 'day'): Describes the time frame that is removed. Acceptable values
            are day, hour, all.
        fault_placement (str, 'start'): Describes where the error is placed. Acceptable values are
            start, middle, end.
        column_index(int): index of column that should be cleaned from positive values

    Returns:
        dataframe (pd.dataframe): Dataframe with cleaned data.

    """
    if data_replacement == 'zero':
        dataframe[dataframe.iloc[:, column_index] > 0] = 0

    elif data_replacement == 'nan':
        dataframe[dataframe.iloc[:, column_index] > 0] = numpy.nan

    elif data_replacement == 'drop':
        dataframe.drop(dataframe[(dataframe.iloc[:, column_index] > 0)].index, inplace=True)

    elif data_replacement == 'remove':
        # gets index for wrong signs
        index = dataframe[(dataframe.iloc[:, column_index] > 0)].index
        # iterates through index
        for timestamp in index:
            # converts index to date time
            index_date_start = timestamp
            index_date_end = timestamp
            if removal_time_frame == 'day':
                index_date_start, index_date_end = find_time_range_method_day(timestamp,
                                                                              fault_placement)

                dataframe = slice_by_index(dataframe,
                                           timestamp_start=index_date_start,
                                           timestamp_end=index_date_end)

            elif removal_time_frame == 'hour':
                index_date_start, index_date_end = find_time_range_method_hour(timestamp,
                                                                               fault_placement)

                dataframe = slice_by_index(dataframe,
                                           timestamp_start=index_date_start,
                                           timestamp_end=index_date_end)
            elif removal_time_frame == 'all':
                dataframe = pd.DataFrame()
    return dataframe


def find_time_range_method_hour(timestamp: int, fault_placement: str = "end") -> tuple:
    """"The method calculates the start and end time for a data removal.

    The time frame considered is here 1h.

    Todo: User should be able to choose amount of hours to be removed

    Note: the start and end time returned is where the time frame starts and ends. If that used to
    removed data with  pd.loc function it needs to be called twice (start of data frame to
    start_time and end_time to end of data frame)

    The timerange determine the position of the data point in the middle, at the end or at the
    start of the data. One of the following fault placements are possible:
    - start: The fault is placed at the beginning of the data that is removed (eg. 1 hour after the fault is removed).
    - middle: The fault is placed in the middle of the data that is removed (eg. 30 minutes before and after the fault is removed).
    - end: The fault is placed at the end of the data that is removed (eg. 1 hour before the fault is removed).

    Args:
        timestamp (int): timestamp in unixtime around which data needs to be removed
        fault_placement (str, 'end'): Describes where the error is placed. Acceptable values are
            start, middle, end.

    Returns:
        start_time (datetime): Timestamp in datetime format ("%Y-%m-%d %H:%M") for the start time of
            data removal.
        end_time (datetime): Timestamp in datetime format ("%Y-%m-%d %H:%M") for the end time of
            data removal.

       """

    date = datetime.fromtimestamp(timestamp)

    if fault_placement == "end":
        index_date_end = date.strftime('%Y-%m-%d %H:%M')
        index_date_start = date - timedelta(hours=1)
        index_date_start = index_date_start.strftime('%Y-%m-%d %H:%M')

    elif fault_placement == "start":
        index_date_start = date.strftime('%Y-%m-%d %H:%M')
        index_date_end = date + timedelta(hours=1)
        index_date_end = index_date_end.strftime('%Y-%m-%d %H:%M')

    elif fault_placement == "middle":
        index_date_start = date - timedelta(hours=1 / 2)
        index_date_start = index_date_start.strftime('%Y-%m-%d %H:%M')
        index_date_end = date + timedelta(hours=1 / 2)
        index_date_end = index_date_end.strftime('%Y-%m-%d %H:%M')

    else:
        index_date_start = date
        index_date_end = date

    index_date_start = datetime.strptime(index_date_start, "%Y-%m-%d %H:%M")
    index_date_end = datetime.strptime(index_date_end, "%Y-%m-%d %H:%M")

    return int(mktime(index_date_start.timetuple())), int(mktime(index_date_end.timetuple()))


def find_time_range_method_day(timestamp: tuple, fault_placement: str = 'start'):
    """"The method calculates the start and end time for a data removal.

    The time frame considered is here 1 day.

    Note: the start and end time returned is where the time frame starts and ends. If that used to
    removed data with  pd.loc function it needs to be called twice (start of data frame to
    start_time and end_time to end of data frame)

    Todo: User should be able to choose amount of days to be removed

    The timerange determine the position of the data point in the middle, at the end or at the
    start of the data. One of the following fault placements are possible:
    - start: The fault is placed at the beginning of the data that is removed (eg. 1 hour after the fault is removed).
    - middle: The fault is placed in the middle of the data that is removed (eg. 30 minutes before and after the fault is removed).
    - end: The fault is placed at the end of the data that is removed (eg. 1 hour before the fault is removed).

    Args:
        timestamp (int): Timestamp in unix time around which data needs to be removed.
        fault_placement (str, 'start'): Describes where the error is placed.

    Returns:
        start_time (datetime): Timestamp in datetime format ("%Y-%m-%d %H:%M") for the start time of
            data removal.
        end_time (datetime): Timestamp in datetime format ("%Y-%m-%d %H:%M") for the end time of
            data removal.

    """

    date = datetime.fromtimestamp(timestamp)

    if fault_placement == "start":
        # removes 24 before the timestamp
        index_date_start = date.strftime('%Y-%m-%d %H:%M')
        index_date_end = date + timedelta(days=1)
        index_date_end = index_date_end.strftime('%Y-%m-%d %H:%M')

    elif fault_placement == "end":
        # removes 24 before the timestamp
        index_date_end = date.strftime('%Y-%m-%d %H:%M')
        index_date_start = date - timedelta(days=1)
        index_date_start = index_date_start.strftime('%Y-%m-%d %H:%M')

    elif fault_placement == "middle":
        # removes 12h  before/after the timestamp
        index_date_start = date - timedelta(hours=12)
        index_date_start = index_date_start.strftime('%Y-%m-%d %H:%M')
        index_date_end = date + timedelta(hours=12)
        index_date_end = index_date_end.strftime('%Y-%m-%d %H:%M')

    else:
        index_date_start = date
        index_date_end = date

    index_date_start = datetime.strptime(index_date_start, "%Y-%m-%d %H:%M")
    index_date_end = datetime.strptime(index_date_end, "%Y-%m-%d %H:%M")

    return int(mktime(index_date_start.timetuple())), int(mktime(index_date_end.timetuple()))


def find_time_range_method_calendarday(timestamp: tuple):
    """"The method calculates the start and end time for a data removal.

    The time frame considered is here 1 day.

    Note: the start and end time returned is where the time frame starts and ends. If that used to
    removed data with  pd.loc function it needs to be called twice (start of data frame to
    start_time and end_time to end of data frame)

    Todo: User should be able to choose amount of days to be removed

    The timerange determine the position of the data point in the middle, at the end or at the
    start of the data. One of the following fault placements are possible:
    - start: The fault is placed at the beginning of the data that is removed (eg. 1 hour after the fault is removed).
    - middle: The fault is placed in the middle of the data that is removed (eg. 30 minutes before and after the fault is removed).
    - end: The fault is placed at the end of the data that is removed (eg. 1 hour before the fault is removed).

    Args:
        timestamp (int): Timestamp in unix time around which data needs to be removed.

    Returns:
        start_time (datetime): Timestamp in datetime format ("%Y-%m-%d %H:%M") for the start time of
            data removal.
        end_time (datetime): Timestamp in datetime format ("%Y-%m-%d %H:%M") for the end time of
            data removal.

    """

    date = datetime.fromtimestamp(timestamp)

    index_date_start = date.strftime('%Y-%m-%d')
    index_date_end = date + timedelta(days=1)
    index_date_end = index_date_end.strftime('%Y-%m-%d %H:%M')

    index_date_start = datetime.strptime(index_date_start, "%Y-%m-%d %H:%M")
    index_date_end = datetime.strptime(index_date_end, "%Y-%m-%d %H:%M")

    return int(mktime(index_date_start.timetuple())), int(mktime(index_date_end.timetuple()))


def slice_by_index(dataframe: pd.DataFrame, timestamp_start: int = None,
                   timestamp_end: int = None) -> pd.DataFrame:
    """cuts out the data in between the timestamps given and returns the data to both sides of the
    time range given. If one start is not provided, it is assumed to be the start of the data frame.
    If end is not provided its assumed to be the end of the data frame

    Args:
        dataframe (pd.DataFrame): Data frame to be sliced
        timestamp_start (int): index of first data point (inclusive, unix timestamp) .
        timestamp_end (int): index of last data point (inclusive, unix time stamp)

    Returns:
        dataframe (pd.DataFrame): sliced pd DataFrame.

    """
    if timestamp_start is None:
        timestamp_start = dataframe.first_valid_index()

    if timestamp_end is None:
        timestamp_end = dataframe.last_valid_index()

    dataframe = dataframe[(dataframe.index < timestamp_start) | (dataframe.index > timestamp_end)]

    return dataframe


def time_filter_data(dataframe: pd.DataFrame, timestamp_start: int = None,
                     timestamp_end: int = None) -> pd.DataFrame:
    """reduce a dataframe based on the provided times start and end timestamp. It is assumed that
    the provided time stamp are not necessarily in the data, an approximation is used to slice as
    accurately as possible. If start is not provided, it is assumed to be the
    start of the data frame. If end is not provided its assumed to be the end of the data frame.

    Note: the index will be sorted in order to enable slicing

    Args:
        dataframe (pd.DataFrame): Data frame to be sliced
        timestamp_start (int): index of first data point (inclusive, unix timestamp) .
        timestamp_end (int): index of last data point (inclusive, unix time stamp)

    Returns:
        dataframe (pd.DataFrame): sliced pd DataFrame.

    """

    dataframe = dataframe.sort_index()
    if timestamp_start is None:
        print("start index was not provided")
        timestamp_start = dataframe.first_valid_index()

    if timestamp_end is None:
        print("end index is not provided")
        timestamp_end = dataframe.last_valid_index()

    reduced_dataframe = dataframe[(dataframe.index > timestamp_start) & (dataframe.index < timestamp_end)]

    return reduced_dataframe


def resample(dataframe: pd.DataFrame, resampling_step: int = None, resampling_unit: str = 'min',
             resampling_strategy_upsampling: str = 'first') -> pd.DataFrame:
    """Resample data to desired spacing. 
    
    If the resolution is finer values averaged (mean). If the resolutions is coarser the first value
    can be chosen (ffill) or the next value (last/bbill) can be used.
    In special circumstance it my be useful to fill the missing data with NaN(nan)

    The following describes the parameters in more detail:
    - resampling_step: This is the desired time step of final dataframe.
    - resampling_unit: The unit of desired time step. Possible units are:
    - h 	hour 	+/- 1.0e15 years 	[1.0e15 BC, 1.0e15 AD]
    - m 	minute 	+/- 1.7e13 years 	[1.7e13 BC, 1.7e13 AD]
    - s 	second 	+/- 2.9e12 years 	[ 2.9e9 BC, 2.9e9 AD]

    One of the following upsampling strategies are possible
    - first: The value before the newly inserted value is chosen (ffill).
    - last: The next value after the newly inserted value is chosen.

    Args:
        dataframe (pd.DataFrame): The dataframe to be resampled.
        resampling_step (int, 8): This is the desired time step of final dataframe.
        resampling_unit (str, 'm'): unit of desired time step
        resampling_strategy_upsampling (str, 'first', nan): Define how the upsampling is conducted.

    Returns:
        dataframe_tmp (pd.DataFrame): The resampled dataframe.

    """

    if resampling_step is not None and len(dataframe) > 1:
        delta_time_tmp = pd.to_timedelta(resampling_step, resampling_unit)

        # force index and delta time to have the same unit
        delta_time_tmp = pd.to_timedelta(delta_time_tmp, 's')

        if dataframe.index[0].dtype in ['timedelta64[s]', 'timedelta64[m]', 'timedelta64[h]']:
            delta_time_tmp_raw = pd.to_timedelta((dataframe.index[1] - dataframe.index[0]), 's')
        else:
            delta_time_tmp_raw = pd.to_timedelta(int(dataframe.index[1] - dataframe.index[0]), 's')

        if delta_time_tmp == delta_time_tmp_raw:
            print("Raw data sample rate is at desired rate", delta_time_tmp_raw)
            return dataframe

        # Temporarily make datetime the index to allow for resampling
        dataframe['ts'] = pd.to_datetime(dataframe.index, unit='s')
        dataframe['original index'] = dataframe.index
        dataframe = dataframe.set_index('ts')

        # Raw data has finer resolution than requested - down sample by averaging
        if delta_time_tmp_raw < delta_time_tmp:
            dataframe_tmp = dataframe.resample(delta_time_tmp).mean()

        # Raw data has coarser resolution than requested - up sample by infilling
        elif delta_time_tmp_raw > delta_time_tmp:
            if resampling_strategy_upsampling == 'first':
                dataframe_tmp = dataframe.resample(delta_time_tmp).ffill()

            elif resampling_strategy_upsampling == 'last':
                dataframe_tmp = dataframe.resample(delta_time_tmp).bfill()

            elif resampling_strategy_upsampling == 'nan':
                dataframe_tmp = dataframe.resample(delta_time_tmp).asfreq()

            dataframe_tmp['original index'] = dataframe_tmp.index.astype(numpy.int64) // 10 ** 9

        dataframe_tmp.reset_index(drop=True, inplace=True)
        dataframe_tmp['ts'] = dataframe_tmp['original index']
        dataframe_tmp = dataframe_tmp.set_index('ts')
        dataframe_tmp = dataframe_tmp.drop('original index', axis='columns')
    else:
        print("Unable to resample (step is invalid or to little data)")
        dataframe_tmp = dataframe

    return dataframe_tmp


def data_refill(dataframe: pd.DataFrame, days: int = 7, attempts: int = 7, threshold: int = 5,
                forward_fill: bool = True, backward_fill: bool = True):
    """ refills data with future data or data from the past.

    Since the data may be influenced by weather or season. It may be useful to adjust
    the days to jump and the attempts used. If jumping for a week for 7 attempts to find
    a suitable block my change the season in which the data was recorded. Attempt * days
    gives a reasonable estimate on how far away a sample can be from the original block

    Note: in the data frame provide missing data needs to replaced by NaN.
    Functions for cleaning used prior to this function should be set up accordingly

    Note: this function assumes a full index (use force_full_index beforehand)

    Args:
        dataframe(dataframe): data frame containing NaN values
        days(int): Number of days that should be jumped for each try
        attempts(int): number of tries to find a matching dataset
        threshold(int): number of samples that are considered to be a block
        forward_fill(bool): use data from the future
        backward_fill(bool): use data from the past

    Returns:
        dataframe: a refilled dataframe that may still contain NaN due to impossible refills.
    """

    # check which rows contain NaNs
    if dataframe.isnull().any().any():
        print("NaN's detected proceed with data refill")
        check_for_nan = dataframe.isnull().any(axis=1)

        # find all indexes for nans and numbers
        blocks = (check_for_nan != check_for_nan.shift()).cumsum()
        # Filter to create a series with (index_start, index_end, count) for NaN blocks
        indices = blocks.groupby(blocks).apply(lambda x: (x.index[0],
                                                          x.index[-1],
                                                          (x.index[-1] - x.index[0])
                                                          if check_for_nan[x.index[0]]
                                                          else numpy.NaN))
        # drop all nan blocks
        indices = indices.dropna()
        indices = pd.DataFrame(indices.tolist(),
                               columns=['out-1', 'out-2', 'out-3'],
                               index=indices.index)

        # remove Rows that don't match the threshold
        # timestep between samples in unit of timestamps
        delta_time_tmp = dataframe.index[0] - dataframe.index[1]

        # threshold describes the number of samples that make a block of missing data
        # (needs to consider the spacing between data)
        threshold_sec = threshold * delta_time_tmp
        indices = indices[indices['out-3'] >= threshold_sec]

        # list with indices that need replacement
        indices_list = indices.values.tolist()
        print("found ", len(indices_list), "blocks of NaN's")

        # find suitable replacement data
        for block in indices_list:
            benchmark = block[2] / delta_time_tmp
            attempt = 0

            # check for future data to refill
            if forward_fill:
                for iter in range(1, attempts):
                    timediff = pd.Timedelta(days, unit='D')
                    start = int(mktime((datetime.fromtimestamp(block[0]) + (timediff*iter)).timetuple()))
                    end = int(mktime((datetime.fromtimestamp(block[1]) + (timediff*iter)).timetuple()))
                    replacement_block = time_filter_data(dataframe, start, end)
                    if not replacement_block.empty:
                        num_of_nan = replacement_block.isnull().sum().sum()
                        # check if they are better suited (less NaN)
                        if benchmark > num_of_nan:
                            benchmark = num_of_nan
                            attempt = iter
            if backward_fill:
                for iter in range(1, attempts):
                    timediff = pd.Timedelta(days, unit='D')
                    start = int(mktime((datetime.fromtimestamp(block[0]) - (timediff * iter)).timetuple()))
                    end = int(mktime((datetime.fromtimestamp(block[1]) - (timediff * iter)).timetuple()))

                    replacement_block = time_filter_data(dataframe, start, end)
                    if not replacement_block.empty:
                        num_of_nan = replacement_block.isnull().sum().sum()
                        # check if they are better suited (less NaN)
                        if benchmark > num_of_nan:
                            benchmark = num_of_nan
                            attempt = iter

            # actual replacement of data
            if attempt == 0:
                print("fail to find a better suited data block for: ", block[0], block[1])
            else:
                print("better block was found in attempt: ", attempt)
                # replace chunk with the best suited
                start = block[0] + (timediff*attempt)
                end = block[1] + (timediff*attempt)
                replacement_block = time_filter_data(dataframe, start, end)
                dataframe.replace({block[0]: block[1]}, replacement_block)

    return dataframe


def force_full_index(dataframe: pd.DataFrame, resampling_step: int = None,
                     resampling_unit: str = "min", timestamp_start: int = None,
                     timestamp_end: int = None) -> pd.DataFrame:
    """ forces a full index. Missing index will be replaced by Nan.

        Note: resampling should be done before to benefit from sampling strategies.

        Args:
            dataframe(dataframe): data frame containing NaN values
            resampling_step (int, 8): This is the desired time step of final dataframe.
            resampling_unit (str, 'M'): unit of desired time step
            timestamp_start (string, none): index at which the dataframe starts
            timestamp_end (string, none): index at which the dataframe ends
        Returns
            dataframe(pandas.Dataframe): dataframe with full index
    """

    if timestamp_start is None:
        print("start index was not provided")
        timestamp_start = dataframe.first_valid_index()

    if timestamp_end is None:
        print("end index is not provided")
        timestamp_end = dataframe.last_valid_index()

    freq = str(resampling_step) + resampling_unit

    new_index = pd.date_range(start=timestamp_start, end=timestamp_end, freq=freq)
    new_index = new_index.astype(numpy.int64) // 10 ** 9
    delta_time_tmp = dataframe.reindex(index=new_index, fill_value=numpy.nan)

    return delta_time_tmp
