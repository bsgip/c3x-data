from c3x.data_cleaning import cleaners
import pandas


def test_duplicates():

    data = pandas.DataFrame({
        'timestamp': [1548154800, 1548155000, 1548155200, 1548155200, 1548155600, 1548155800, 1548156000],
        'value': [32, 32, 45, 70, 90, 100, 110]
    })
    data = data.set_index('timestamp')

    result_data = cleaners.duplicates_remove(data, data_replacement='none', removal_time_frame='day', fault_placement='calendar')

    assert data.size != result_data.size, "duplicates have not been dropped"
    assert result_data.index.nunique() == result_data.size, "duplicates are remained in dataframe"
