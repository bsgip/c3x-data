import numpy
import pandas
from c3x.data_cleaning import cleaners


def test_nans_dropped():
    """
    Test that the all negative values are removed from a dataframe.
    """

    # generate test data
    dataframe = pandas.DataFrame(numpy.random.randint(-100, 100, size=(100, 6)), columns=list('ABCDEF'))
    dataframe['A'][5] = numpy.nan

    # run function on dataframe
    result_dataframe = cleaners.handle_nans(dataframe, data_replacement="drop", removal_time_frame="day", fault_placement="calendar")
    assert dataframe.size != result_dataframe.size, "nan have not been dropped"


def test_nans_replaced_last():
    """
    Test that the all negative values are replaced in a dataframe.
    """

    # generate test data
    dataframe = pandas.DataFrame(numpy.random.randint(-100, 100, size=(100, 6)), columns=list('ABCDEF'))
    dataframe['A'][5] = numpy.nan

    # run function on dataframe
    result_dataframe = cleaners.handle_nans(dataframe, data_replacement="last", removal_time_frame="day",
                                            fault_placement="calendar")

    assert dataframe.size == result_dataframe.size, "nan have been dropped"
    assert dataframe['A'][6] == result_dataframe['A'][5], "nan has not be replaced with last"


def test_nans_replaced_first():
    """
    Test that the all negative values are replaced in a dataframe.
    """

    #generate test data
    dataframe = pandas.DataFrame(numpy.random.randint(-100, 100, size=(100, 6)), columns=list('ABCDEF'))
    dataframe['A'][5] = numpy.nan

    #run function on dataframe
    result_dataframe = cleaners.handle_nans(dataframe, data_replacement="first", removal_time_frame="day",
                                            fault_placement="calendar")

    assert dataframe.size == result_dataframe.size, "nan have been dropped"
    assert dataframe['A'][4] == result_dataframe['A'][5], "nan has not be replaced with last"

def test_nans_replaced_zero():
    """
    Test that the all negative values are replaced in a dataframe.
    """

    # generate test data
    dataframe = pandas.DataFrame(numpy.random.randint(-100, 100, size=(100, 6)), columns=list('ABCDEF'))
    dataframe['A'][5] = numpy.nan

    # run function on dataframe
    result_dataframe = cleaners.handle_nans(dataframe, data_replacement="zero", removal_time_frame="day",
                                            fault_placement="calendar")
    assert dataframe.size == result_dataframe.size, "nan have been dropped"
    assert result_dataframe['A'][5] == 0, "nan has not be replaced with last"


def test_nans_replaced_none():
    """
    Test that the all negative values are replaced in a dataframe.
    """

    #generate test data
    dataframe = pandas.DataFrame(numpy.random.randint(-100, 100, size=(100, 6)), columns=list('ABCDEF'))
    dataframe['A'][5] = numpy.nan

    #run function on dataframe
    result_dataframe = cleaners.handle_nans(dataframe, data_replacement="none", removal_time_frame="day",
                                            fault_placement="calendar")

    assert dataframe.size == result_dataframe.size, "nan has not been dropped"
    assert result_dataframe['A'][5] != numpy.nan, "nan was removed but shouldn't have"


# function is not yet implemented
def test_nans_replaced_average():
    """
    Test that the all negative values are replaced in a dataframe.
    """
    # generate test data
    dataframe = pandas.DataFrame(numpy.random.randint(-100, 100, size=(100, 6)), columns=list('ABCDEF'))
    dataframe['A'][5] = numpy.nan

    # run function on dataframe
    result_dataframe = cleaners.handle_nans(dataframe, data_replacement="average", removal_time_frame="day",
                                            fault_placement="calendar")

    assert dataframe.size == result_dataframe.size, "nan has not been dropped"
    assert result_dataframe['A'][5] != numpy.nan, "nan was removed but shouldn't have"


# function is not yet implemented
def test_nans_replaced_max():
    """
    Test that the all negative values are replaced in a dataframe.
    """
    # generate test data
    dataframe = pandas.DataFrame(numpy.random.randint(-100, 100, size=(100, 6)), columns=list('ABCDEF'))
    dataframe['A'][5] = numpy.nan

    # run function on dataframe
    result_dataframe = cleaners.handle_nans(dataframe, data_replacement="max", removal_time_frame="day",
                                            fault_placement="calendar")

    assert dataframe.size == result_dataframe.size, "nan has not been dropped"
    assert result_dataframe['A'][5] != numpy.nan, "nan was removed but shouldn't have"