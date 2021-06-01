"""
    Extracts each connections points requested from the original set of data.
    the data from those connections points is saved as evolve core network objects in pickeld numpy files

    Each node is  saved separate in a files named node_$ID.npy
"""
import os
import pandas
import pickle
from time import mktime

# BSGIP specific tools
from c3x.data_loaders import configfileparser, nextgen_loaders
from c3x.data_cleaning import cleaners


##################### Load and check data #####################

config = configfileparser.ConfigFileParser("config/example_for_cleaning.ini")

data_paths = config.read_data_path()
batch_info = config.read_batches()
data_usage = config.read_data_usage()

# Create a nextGen data object that has working paths and can be sliced using batches
next_gen = nextgen_loaders.NextGenData(data_name='NextGen',
                                       source =data_paths["source"],
                                       batteries=data_paths["batteries"],
                                       solar=data_paths["solar"],
                                       node=data_paths["node"],
                                       loads=data_paths["loads"],
                                       results=data_paths["results"],
                                       number_of_batches=batch_info["number_of_batches"],
                                       files_per_batch=batch_info["files_per_batch"],
                                       concat_batches_start=batch_info["concat_batches_start"],
                                       concat_batches_end=batch_info["concat_batches_end"])

# concatenates batches of one data type to one file per node (data frames and separate network)
for data_type in data_usage:
    next_gen.concat_data(meas_type=data_type,
                         concat_batches_start=batch_info["concat_batches_start"],
                         concat_batches_end=batch_info["concat_batches_end"])

##################### Clean measurement data #####################

# read cleaning information from config file
time = config.read_time_filters()
signs = config.read_sign_correction()
duplicates = config.read_duplicate_removal()
nan_handling = config.read_nan_handeling()
resampling = config.read_resampling()
measurement_types = config.read_measurement_types()

# generate a file list that needs cleaning (only node data is considered e.G. concatenated data)
data_path_list = []
data_files = []

for data_type in data_usage:
    path = data_paths[data_type]
    for file in os.listdir(data_paths[data_type]):
        data_files.append(os.path.join(path, file))

# hands data frame from files to cleaners, only node data is relevant here
for file in data_files:
    if "node.npy" in file:
        print("Cleaning File: ", file)
        node_data = pandas.read_pickle(file)
        if time["time_filter_use"]:
            print("slice dataframe to user specified time range")
            node_data = cleaners.time_filter_data(node_data, int(mktime(time["start_time"].timetuple())), int(mktime(time["end_time"].timetuple())))

        if duplicates["duplicate_removal"] and not node_data.empty:
            print("remove duplicates from time frame")
            node_data = cleaners.duplicates_remove(node_data,
                                                   duplicates["data_replacement"],
                                                   duplicates['removal_time_frame'],
                                                   duplicates["fault_placement"])

        if "solar" in file and signs["wrong_sign_removal"] and not node_data.empty:
            print("remove wrong signs from solar data")
            node_data = cleaners.remove_positive_values(node_data,
                                                        signs["data_replacement"],
                                                        signs["removal_time_frame"],
                                                        signs["fault_placement"],
                                                        column_index=0)

        if "load" in file and signs["wrong_sign_removal"] and not node_data.empty:
            print("remove wrong signs from load data")

            node_data = cleaners.remove_negative_values(node_data,
                                                        signs["data_replacement"],
                                                        signs["removal_time_frame"],
                                                        signs["fault_placement"],
                                                        coloumn_index=0)


        if nan_handling["nan_removal"] and not node_data.empty:
            print("remove nans from time frame")
            node_data = cleaners.handle_nans(node_data,
                                             nan_handling["data_replacement"],
                                             nan_handling["removal_time_frame"],
                                             nan_handling["fault_placement"])

        if resampling["resampling"] and not node_data.empty:
            print("resampling node data")
            node_data = cleaners.resample(node_data,
                                          resampling_step=resampling["resampling_step"],
                                          resampling_unit=resampling["resampling_unit"],
                                          resampling_strategy_upsampling=resampling["resampling_strategy_upsampling"])

        print("forcing full index")
        node_data = cleaners.force_full_index(node_data,
                                            resampling_step=resampling["resampling_step"],
                                            resampling_unit=resampling["resampling_unit"],
                                            timestamp_start=time["start_time"],
                                            timestamp_end=time["end_time"])

        if not node_data.empty:
            path = file.split("/")
            filename = path[len(path)-1].split(".")[0]
            result_location = data_paths["results"] + "/" + filename
            with open(result_location + '.npy', 'wb') as handle:
                pickle.dump(node_data, handle, protocol=pickle.HIGHEST_PROTOCOL)
            node_data.to_csv(path_or_buf=(result_location + ".csv"), index=True)
        else:
            print("dataframe is now empty, file removed ", file)
            os.remove(file)

cleaned_data = next_gen.read_clean_data(measurement_types["loads"], measurement_types["solar"], measurement_types["batteries"])

print("number of properties with data between ", time["start_time"], "and", time["end_time"], "is",
      len(cleaned_data.keys()))

print(cleaned_data)



