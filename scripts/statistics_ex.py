import os
import pandas


from c3x.data_loaders import configfileparser, nextgen_loaders
from c3x.data_statistics import statistics as stats

# Reads a config file to produce a dictionary that can be handed over to functions
config = configfileparser.ConfigFileParser("config/config_nextGen_stats.ini")
data_paths = config.read_data_path()
batch_info = config.read_batches()
measurement_types = config.read_data_usage()

# Create a nextGen data object that has working paths and can be sliced using batches
# it might be appropriate for the example to make the batches smaller, however that may
# increase computing time,
# the next row can be commented if data was processed prior to running this script

nextgen = nextgen_loaders.NextGenData(data_name='NextGen',
                                       source=data_paths["source"],
                                       batteries=data_paths["batteries"],
                                       solar=data_paths["solar"],
                                       node=data_paths["node"],
                                       loads=data_paths["loads"],
                                       results=data_paths["results"],
                                       stats = data_paths["stats"],
                                       number_of_batches=batch_info["number_of_batches"],
                                       files_per_batch=batch_info["files_per_batch"],
                                       concat_batches_start=batch_info["concat_batches_start"],
                                       concat_batches_end=batch_info["concat_batches_end"])
# now we have a folder structure with lots of files with batch numbers
print("ALL BATCHES ANALYSIS")
node_count, batch_list = stats.batch_with_highest_node_count(data_dir=data_paths,
                                                             batch_info=batch_info,
                                                             measurement_types=measurement_types)
print("max number of nodes: ", node_count)
print("batches with max node count: ", batch_list)
print("number of batches with that node count: ", len(batch_list))

data_path_list = []
data_files = []
sorted_dict = {}
node_list = []

# here a dictionary is generate that holds a list of nodes per batch  (batch:[node_ids])
for batch in range(batch_info["number_of_batches"]):
    node_list = stats.nodes_per_batch(data_paths, batch, measurement_types)
    sorted_dict[batch] = node_list

# a list of all files is created
for data_type in measurement_types:
    path = data_paths[data_type]
    for file in os.listdir(data_paths[data_type]):
        data_files.append(os.path.join(path, file))

# some Data Frames and Labels for saving results nicely
result_data_frame = pandas.DataFrame()
batch_data_results = pandas.DataFrame()
index = ['Battery - PLG',
         'Battery - QLG',
         'Battery - RC',
         'Solar - PLG',
         'Load - PLG',
         'Load - QLG']

columns = pandas.MultiIndex.from_product([['Samples', 'Duplicates'], index],
                                         names=['Type', 'Measurement'])

# iterate through batches
for batch in range(batch_info["number_of_batches"]):
    batch_data = pandas.DataFrame()

    # iterate through nodes
    result_data_frame = pandas.DataFrame()
    for node in sorted_dict[batch]:
        node_data = pandas.DataFrame()

        search = str(node) + "_" + str(batch) + ".npy"
        batch_node_subset = [val for i, val in enumerate(data_files) if val.endswith(search)]

        # build a data frame with all measurement data
        first_run = True
        for path in batch_node_subset:
            tmp_data_frame = pandas.read_pickle(path)
            if first_run is True:
                node_data = pandas.DataFrame(tmp_data_frame)
                first_run = False
            else:
                node_data = pandas.concat([node_data, tmp_data_frame], axis=1)

        # get the node ID
        node_df = pandas.DataFrame(pandas.Series(node))
        node_df.columns = ["node"]

        # count samples and convert to data frame
        samples = pandas.Series(stats.count_samples(node_data))
        samples = pandas.DataFrame(samples).transpose()

        # count duplicates anc convert to data frame
        duplicates = pandas.Series(stats.count_duplictaes(node_data))
        duplicates = pandas.DataFrame(duplicates).transpose()

        # concat and rename nicely
        samples_dupli = pandas.concat([samples, duplicates], axis=1)
        samples_dupli.columns = columns

        # count nans
        nans = pandas.DataFrame(pandas.Series(stats.count_nan(node_data)))
        nans.columns = ["total missing samples"]

        # get timings from data frame
        time_data_frame = stats.get_time_range(node_data)
        time_data_frame.columns = ["start time", "end time"]

        # read solar/load data separately and analyse for sign mismatch
        solar_sign = pandas.DataFrame()
        load_sign = pandas.DataFrame()
        for path in batch_node_subset:
            if "solar" in path:
                tmp_data_frame = pandas.read_pickle(path)
                solar_sign = pandas.DataFrame(pandas.Series(
                    stats.count_wrong_signs(tmp_data_frame, measurement_type='solar')))
                solar_sign.columns = ["invalid sign - solar"]

            if "load" in path:
                tmp_data_frame = pandas.read_pickle(path)
                load_sign = pandas.DataFrame(pandas.Series(
                    stats.count_wrong_signs(tmp_data_frame, measurement_type='load')))
                load_sign.columns = ["invalid sign - load"]

        # combine data to row
        row = pandas.concat([node_df, samples_dupli, nans, solar_sign, load_sign, time_data_frame], axis=1)

        # add row to results
        result_data_frame = result_data_frame.append(row, ignore_index=True)

    # reindex and save
    result_data_frame = result_data_frame.set_index('node')
    print(result_data_frame)

    # results are saved with batch numbers so that the selection
    # for data processing can be linked back
    result_data_frame.to_csv(data_paths["stats"] + "/stats_" + str(batch) + '.csv')
