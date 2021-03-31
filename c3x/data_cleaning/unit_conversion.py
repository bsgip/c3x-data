"""
module for unit conversion
"""

import numpy
import pandas


def convert_watt_hour_to_watt(dataframe, timedelta=None):
    """
        Converts data Frame data from energy in (k)Wh to power in (k)W.
        Note: Assumes that the data frame is indexed with timestamp!

        Args:
            dataframe  (pandas.dataframe): DataFrame to be converted.
            ts  (timedelta): Time interval of original data.

        Returns:
            dataframe  (pandas.dataframe): DataFrame with data in (k)W.
    """

    if timedelta is None:
        if isinstance(dataframe.index[0], numpy.int64):
            tstep = int(dataframe.index[1] - dataframe.index[0])
            timedelta = numpy.timedelta64(tstep, 's')
        else:
            timedelta = numpy.timedelta64(dataframe.index[1] - dataframe.index[0])

    watthour_to_watt = numpy.timedelta64(1, 'h') / timedelta

    if isinstance(dataframe, pandas.core.series.Series):
        dataframe = dataframe.apply(lambda x: x * watthour_to_watt)
    else:
        dataframe = dataframe * watthour_to_watt

    return dataframe


def convert_watt_hour_to_kwatt(dataframe, timedelta=None):
    """
        Convert dataframe data from energy in Wh to power in kW.
        Note: Assumes that dataframe is indexed with timestamp!

        Args:
            dataframe  (pandas.dataframe): DataFrame to be converted.
            timedelta  (timedelta): Time interval of original data.

        Returns:
            dataframe  (pandas.dataframe): DataFrame with data in kW.
    """

    if timedelta is None:
        if isinstance(dataframe.index[0], numpy.int64):
            tstep = int(dataframe.index[1] - dataframe.index[0])
            timedelta = numpy.timedelta64(tstep, 's')
        else:
            timedelta = numpy.timedelta64(dataframe.index[1] - dataframe.index[0])

    watthour_to_kwatt = (numpy.timedelta64(1, 'h') / timedelta)/1000

    if isinstance(dataframe, pandas.core.series.Series):
        dataframe = dataframe.apply(lambda x: x*watthour_to_kwatt)
    else:
        dataframe = dataframe*watthour_to_kwatt

    return dataframe


def convert_watt_to_watt_hour(dataframe, timedelta=None):
    """
        Convert dataframe data from power in (k)W to energy in (k)Wh.
        Note: Assumes that dataframe is indexed with timestamp!

        Args:
            dataframe  (pandas.timedelta): DataFrame to be converted.
            timedelta  (timedelta): Time interval of original data.

        Returns:
            dataframe  (pandas.dataframe): DataFrame with data in (k)Wh.
    """

    if timedelta is None:
        if isinstance(dataframe.index[0], numpy.int64):
            tstep = int(dataframe.index[1] - dataframe.index[0])
            timedelta = numpy.timedelta64(tstep, 's')
        else:
            timedelta = numpy.timedelta64(dataframe.index[1] - dataframe.index[0])

    watt_to_watthour = timedelta / numpy.timedelta64(1, 'h')

    if isinstance(dataframe, pandas.core.series.Series):
        dataframe = dataframe.apply(lambda x: x*watt_to_watthour)
    else:
        dataframe = dataframe*watt_to_watthour

    return dataframe


def convert_watt_to_kwatt_hour(dataframe, timedelta=None):
    """
    Convert dataframe data from power in W to energy in kWh.
    Note: Assumes that dataframe is indexed with timestamp!

    Args:
        dataframe  (pandas.dataframe): DataFrame to be converted.
        timedelta  (timedelta): Time interval of original data.

    Returns:
        dataframe  (pandas.dataframe): DataFrame with data in kWh.
    """

    if timedelta is None:
        if isinstance(dataframe.index[0], numpy.int64):
            tstep = int(dataframe.index[1] - dataframe.index[0])
            timedelta = numpy.timedelta64(tstep, 's')
        else:
            timedelta = numpy.timedelta64(dataframe.index[1] - dataframe.index[0])

    w_to_kwh = (timedelta / numpy.timedelta64(1, 'h'))/1000
    if isinstance(dataframe, pandas.core.series.Series):
        dataframe = dataframe.apply(lambda x: x*w_to_kwh)
    else:
        dataframe = dataframe*w_to_kwh

    return dataframe


def local_t_from_dataframe_index(dataframe, local_tz=None):
    """
        Convert unix timestamps found in dataframe index to local datetimes.

        Args:
            local_tz  (timezone key): timezone you want it converted to.
    """

    return pandas.to_datetime(dataframe.index, unit='s').tz_localize('GMT').tz_convert(local_tz)
