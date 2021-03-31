"""
 This examples shows how cleaned data can be read for further use
"""

import os
import pandas

# BSGIP specific tools
from c3x.data_loaders import configfileparser

config = configfileparser.ConfigFileParser("config/example_for_cleaning.ini")

measurement_types = config.read_data_usage()
data_files = []

for data_type in measurement_types:
    path = "/home/meike/repos/c3x-data/tests/cleaned_data/"
    for file in os.listdir(path):
        data_files.append(os.path.join(path, file))

# cleaned npy files
for file in data_files:
    # Note: there is a file with additional node information
    if "node.npy" in file:
        print("reading npy-file: ", file)
        node_data = pandas.read_pickle(file)

# cleaned csv files
for file in data_files:
    # Note: there is a file with additional node information
    if "node.csv" in file:
        print("reading csv-file: ", file)
        node_data = pandas.read_csv(file)
