"""
    Extracts each block of a config files
"""

from c3x.data_loaders import configfileparser

# Reads a config file to produce a dictionary that can be handed over to functions
config = configfileparser.ConfigFileParser("config/example_full_config.ini")

data_paths = config.read_data_path()
batch_info = config.read_batches()
time = config.read_time_filters()
signs = config.read_sign_correction()
duplicates = config.read_duplicate_removal()
data_usage = config.read_data_usage()
nan_handling = config.read_nan_handeling()
resampling = config.read_resampling()
refill = config.read_refill()
optimiser_objectives_set = config.read_optimiser_objective_set()
optimiser_objectives = config.read_optimiser_objectives()
inverter = config.read_inverter()
energy_storage = config.read_energy_storage()
energy_system = config.read_energy_system()
tariff_factors = config.read_tariff_factors()
scenario = config.read_scenario_info()
