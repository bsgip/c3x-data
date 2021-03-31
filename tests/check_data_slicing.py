from c3e_data_preparation.preparation import cleaners
import pandas

def test_slicing_start_to_end():
    timestamp_start = 1548155000
    timestamp_end = 1548155800

    data = pandas.DataFrame({'timestamp': [1548154800, 1548155000, 1548155200, 1548155400, 1548155600, 1548155800, 1548156000],
            'Value': [32, 32, 45, 70, 90, 100, 110]})
    data = data.set_index('timestamp')

    cleaned = cleaners.slice_by_index(data, timestamp_start, timestamp_end)

    assert len(cleaned) != len(data), "data was not removed"
    assert len(cleaned) == 5, "not enough data removed"

def test_slicing_start():
    timestamp_start = 1548155000
    timestamp_end = 1548156200

    data = pandas.DataFrame({'timestamp': [1548154800, 1548155000, 1548155200, 1548155400, 1548155600, 1548155800, 1548156000],
            'Value': [32, 32, 45, 70, 90, 100, 110]})
    data = data.set_index('timestamp')

    cleaned = cleaners.slice_by_index(data, timestamp_start, timestamp_end)

    assert len(cleaned) != len(data), "data was not removed"
    assert len(cleaned) == 6, "not engough data removed"

def test_slicing_end():
    timestamp_start = 154814000
    timestamp_end = 1548155800

    data = pandas.DataFrame({'timestamp': [1548154800, 1548155000, 1548155200, 1548155400, 1548155600, 1548155800, 1548156000],
            'Value': [32, 32, 45, 70, 90, 100, 110]})
    data = data.set_index('timestamp')

    cleaned = cleaners.slice_by_index(data, timestamp_start, timestamp_end)

    assert len(cleaned) != len(data), "data was not removed"
    assert len(cleaned) == 6, "not engough data removed"


def test_slicing_both():
    timestamp_start = 154814000
    timestamp_end = 1548156200

    data = pandas.DataFrame({'timestamp': [1548154800, 1548155000, 1548155200, 1548155400, 1548155600, 1548155800, 1548156000],
            'Value': [32, 32, 45, 70, 90, 100, 110]})
    data = data.set_index('timestamp')

    cleaned = cleaners.slice_by_index(data, timestamp_start, timestamp_end)

    assert len(cleaned) == len(data), "data was not removed"