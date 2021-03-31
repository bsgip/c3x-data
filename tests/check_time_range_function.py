from c3e_data_preparation.preparation import cleaners

def test_timestamp_end_h():
    timestamp = 1548154800

    start, end = cleaners.find_time_range_method_hour(timestamp=timestamp, fault_placement="end")

    assert end == timestamp, "timerange is wrong"
    assert start != timestamp, "timerange is wrong"

def test_timestamp_start_h():
    timestamp = 1548154800

    start, end = cleaners.find_time_range_method_hour(timestamp=timestamp, fault_placement="start")

    assert start == timestamp, "timerange is wrong"
    assert end != timestamp, "timerange is wrong"

def test_timestamp_middle_h():
    timestamp = 1548154800

    start, end = cleaners.find_time_range_method_hour(timestamp=timestamp, fault_placement="middle")

    assert start != timestamp, "timerange is wrong"
    assert start != timestamp, "timerange is wrong"

def test_timestamp_end_d():
    timestamp = 1548154800

    start, end = cleaners.find_time_range_method_day(timestamp=timestamp, fault_placement="end")

    assert end == timestamp, "timerange is wrong"
    assert start != timestamp, "timerange is wrong"

def test_timestamp_start_d():
    timestamp = 1548154800

    start, end = cleaners.find_time_range_method_day(timestamp=timestamp, fault_placement="start")

    assert start == timestamp, "timerange is wrong"
    assert end != timestamp, "timerange is wrong"

def test_timestamp_middle_d():
    timestamp = 1548154800

    start, end = cleaners.find_time_range_method_day(timestamp=timestamp, fault_placement="middle")

    assert start != timestamp, "timerange is wrong"
    assert start != timestamp, "timerange is wrong"

