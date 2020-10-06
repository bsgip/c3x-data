import pandas
import numpy
from c3e_data_preparation.preparation import cleaners

def test_positive_dropped():
    """
    Test that the all positive values are removed from a dataframe.
    """

    #generate test data
    dataframe = pandas.DataFrame(numpy.random.randint(-100, -1, size=(100, 4)), columns=list('ABCD'))
    dataframe['A'][5] = 66

    #run function on dataframe
    result_dataframe = cleaners.remove_positive_values(dataframe, data_replacement='drop', removal_time_frame='day', fault_placement='calendar')

    assert numpy.sum((result_dataframe > 0).values.ravel()) is not 0, "positive values have not been handeld"
    assert len(result_dataframe) != len(dataframe)-1, "positive have not been dropped"

def test_positive_nan():
    """
    Test that the all positive values are replaced in a dataframe.
    """

    #generate test data
    dataframe = pandas.DataFrame(numpy.random.randint(-100, -1, size=(100, 4)), columns=list('ABCD'))
    dataframe['A'][5] = 66

    #run function on dataframe
    result_dataframe = cleaners.remove_positive_values(dataframe, data_replacement='nan', removal_time_frame='day', fault_placement='calendar')

    assert numpy.sum((result_dataframe > 0).values.ravel()) is not 0, "positive values have not been handeld"
    assert len(result_dataframe) == len(dataframe), "positive have been dropped"
    assert dataframe.isnull().values.any() == 1, "positive have not been replaced with Nan"

def test_positive_zeros():
    """
    Test that the all positive values are replaced in a dataframe.
    """

    #generate test data
    dataframe = pandas.DataFrame(numpy.random.randint(-100, -1, size=(100, 4)), columns=list('ABCD'))
    dataframe['A'][5] = 66

    #run function on dataframe
    result_dataframe = cleaners.remove_positive_values(dataframe, data_replacement='zero', removal_time_frame='day', fault_placement='calendar')

    assert numpy.sum((result_dataframe < 0).values.ravel()) is not 0, "neg values have not been handeld"
    assert len(result_dataframe) == len(dataframe), "positives have been dropped"
    assert dataframe.isnull().values.any() == 0, "positives have been replaced with a number"
    assert result_dataframe['A'][5] == 0, "positive values has not be replaced with 0"

def test_positive_none():
    """
    Test that the all positive values are replaced in a dataframe.
    """

    #generate test data
    dataframe = pandas.DataFrame(numpy.random.randint(-100, 100, size=(100, 6)), columns=list('ABCDEF'))
    dataframe['A'][5] = 66

    #run function on dataframe
    result_dataframe = cleaners.remove_positive_values(dataframe, data_replacement='none', removal_time_frame='day', fault_placement='calendar')

    assert numpy.sum((result_dataframe > 0).values.ravel()) is not 0, "Nothing Changed (expected)"
    assert len(result_dataframe) == len(dataframe), "negatives have been dropped (by accident)"
    assert result_dataframe['A'][5] == 66, "Positive values has be replaced (by accident)"

def test_positive_remove():
    """
    Test that the all positive values are replaced in a dataframe.
    """

    #generate test data
    dataframe = pandas.DataFrame(numpy.random.randint(-100, 100, size=(100, 6)), columns=list('ABCDEF'))
    dataframe['A'][5] = 66

    #run function on dataframe
    result_dataframe = cleaners.remove_positive_values(dataframe, data_replacement='remove', removal_time_frame='all', fault_placement='calendar')

    assert len(result_dataframe) is 0, "Dataframe is now empty"