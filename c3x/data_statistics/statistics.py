"""
statistic.py contains functions used to analyse the contents of data sets. All functions work on a
pandas dataframe.
"""

from datetime import datetime
import os
import numpy
import pandas


# TODO: need an option to read a network


def nodes_per_batch(data_dir, batch_number, measurement_types):
    """
        Extracts the nodes in a batch per measurement type.
        The files analysed must be named with a batch number at the end.

        Args:
            data_dir(dict): dictionary of folder:path pairs that are supposed to be searched
            batch_number(int): Batch of interest
            measurement_types(list): measurement types of interest

        Returns:
            node_list(list): contains node_ids found in a batch
    """
    node_list = []

    for measurement_type in measurement_types:
        data_files = os.listdir(data_dir[measurement_type])
        data_files = sorted([f for f in data_files if f.endswith('_' + str(batch_number) + '.npy')])

        for file in data_files:
            if "node" not in file:
                node_id = file.split('_')
                node_id = node_id[2]
                node_list.append(node_id) if node_id not in node_list else node_list

    return node_list


def batch_with_highest_node_count(data_dir, batch_info, measurement_types):
    """
        Extracts the nodes in a batch per measurement type.
        The files analysed must be named with a batch number at the end.

        Args:
            data_dir(dict): List of folders that are supposed to be searched
            batch_info (int): dict with batch information
            measurement_types(dict): measurement types of interest

        Returns:
            tuple (int, list): the node count is returned and a list of batches with that node count
    """
    node_count = 0
    result_batches = []

    batches = numpy.arange(batch_info["number_of_batches"])
    for batch in batches:
        node_list = []
        for measurement_type in measurement_types:
            data_files = os.listdir(data_dir[measurement_type])
            data_files = sorted([f for f in data_files if f.endswith('_' + str(batch) + '.npy')])
            for file in data_files:
                node_id = file.split('_')
                node_id = node_id[2]
                node_list.append(node_id) if node_id not in node_list else node_list

        if node_count <= len(node_list):
            if node_count == len(node_list):
                result_batches.append(batch)
            else:
                result_batches = [batch]
            node_count = len(node_list)

    return node_count, result_batches


def count_samples(measurement_df):
    """
        Counts the number of samples in a given data frame.

        Args:
            measurement_df(Data Frame): Contains data to be checked

        Returns:
            Dataframe: Number of data samples per column
    """
    return measurement_df.count()


def count_duplictaes(measurement_df):
    """
        Counts the number duplicated samples in a data frame.

        Args:
            measurement_df(Data Frame): Contains data to be checked

        Returns:
            Dataframe: Number of data samples per column
    """
    return measurement_df.count() - measurement_df.nunique()


def count_nan(measurement_df):
    """
        Counts the number of NotANumbers in a data frame

        Args:
            measurement_df(Data Frame): Data frame to be checked

        Returns:
            Dataframe: Number of data samples per column
    """
    return measurement_df.isnull().sum().sum()


def count_wrong_signs(measurement_df, measurement_type='load'):
    """
        Some data in the solar or loads can be wrong, which is usually indicated by
        a wrong signage. Loads must be positive and solar must be negative

        Args:
            measurement_df (Data frame): Data frame to be checked
            measurement_type: measurement type e.G. loads, solar
        Returns:
            int: with the number of samples with wrong signage
    """
    if measurement_type == "load":
        negative_values = measurement_df.lt(0).sum().sum()
        return int(negative_values)
    if measurement_type == "solar":
        positive_values = measurement_df.gt(0).sum()
        return int(positive_values)
    return 0


def get_time_range(measurement_df):
    """
        Each dataframe is indexed by a time stemp. This functions reads the first and the last
        index and returns them in a human readable manner as as data frame

        Args:
            measurement_df (DataFrame): Data to be analysed
        Returns:
            Data frame: pandas DataFrame with col: start time, end time
    """

    first = measurement_df.last_valid_index()

    if first is None:
        first = pandas.Series("None")
    else:
        first = pandas.Series(datetime.utcfromtimestamp(first))

    first.columns = ["start time"]

    last = measurement_df.first_valid_index()
    if last is None:
        last = pandas.Series("None")
    else:
        last = pandas.Series(datetime.utcfromtimestamp(last))
    last.columns = ["end time"]

    return pandas.concat([first, last], axis=1)
