"""
    Extracts each block of a config files
"""

from c3x.data_loaders import configfileparser

# Reads a config file to produce a dictionary that can be handed over to functions
config = configfileparser.ConfigFileParser("config/example_for_cleaning.ini")

data_paths = config.read_data_path()
batch_info = config.read_batches()
time = config.read_time_filters()
signs = config.read_sign_correction()
duplicates = config.read_duplicate_removal()
data_usage = config.read_data_usage()
nan_handling = config.read_nan_handeling()
resampling = config.read_resampling()
refill = config.read_refill()
