import pandas
import numpy
from c3e_data_preparation.preparation import cleaners


def test_handle_non_numerical_values():
    """
    Test that the all positive values are removed from a dataframe.
    """

    #generate test data
    dataframe = pandas.DataFrame(numpy.random.randint(-100, 100 , size=(100, 4)), columns=list('ABCD'))
    dataframe['A'][5] = 'ACD'

    #run function on dataframe
    result_dataframe = cleaners.handle_non_numericals(dataframe)

    assert len(result_dataframe) == len(dataframe), "non numerical value was dropped"
    assert dataframe['A'][5] != result_dataframe['A'][5], "non numerical values has no been changed"
