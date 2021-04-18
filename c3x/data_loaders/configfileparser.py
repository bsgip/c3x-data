""" Config File Parser Module helps to parse a config file and convert read sections to dictionaries of Keys:Values """

import configparser
import datetime


class ConfigFileParser:
    """The module reads a config file. Values to be read are group in sections Batches, Data Path,
    Data Usage, Time Filter, Sign Correction, Duplicate Removal, Nan handling and Resample and so on.
    Each of these section can be read separately and the used as parameters for various functions

    """

    def __init__(self, ini_path: str):
        """Creates a Data object for a config file so that configurations for that file can be used
        easily.

        Args:
            ini_path (str): string to the config file.
        """

        self.ini_path = ini_path
        self.ini_path = ini_path
        self.config = configparser.ConfigParser()
        self.config.read(self.ini_path)

    def read_batches(self) -> dict:
        """Reads the batch section from config file and converts it to a dictionary that is
        typecasted.

        Possible values for this section are (key/value):
            batch_size = 30
            concat_batches_start = 1
            concat_batches_end = 30

        Returns:
            batches_dict (dict): Dictionary with configuration values for file batching
        """

        batches_dict = dict(self.config.items('Batches')) if "Batches" in self.config else {}
        if batches_dict:
            batches_dict["number_of_batches"] = self.config["Batches"].getint("number_of_batches")
            batches_dict["concat_batches_start"] = \
                self.config["Batches"].getint("concat_batches_start")
            batches_dict["concat_batches_end"] = self.config["Batches"].getint("concat_batches_end")
            batches_dict["files_per_batch"] = self.config["Batches"].getint("files_per_batch")
        print("batches dict: ", batches_dict)
        return batches_dict

    def read_data_path(self) -> dict:
        """Reads the "Data Path" section from config file and converts it to a dictionary.

        Possible values for this section are (key:value)
            source:/some/path/to/folder
            batteries:/some/path/to/folder
            solar:/some/path/to/folder
            node:/some/path/to/folder
            loads:/some/path/to/folder
            tariffs: /some/path/to/folder

        Returns:
            data_path_dict (dict): Dictionary with configuration values for file batching

        """
        data_path_dict = dict(self.config.items('Data Path')) if "Data Path" in self.config else {}
        print("datapath: ", data_path_dict)
        return data_path_dict

    def read_time_filters(self) -> dict:
        """Reads the Time Filter section from config file and converts it to a dictionary that is
        typecasted. Timestamps are unix times.

        Variables read into the dictionary (key:value)
            time_filter_use: True/False
            start_time: 1/1/2018 00:00:00
            end_time: 31/12/2018 23:59:59

        Returns:
            time_filter_dict (dict): dictionary with configuration values for time filtering
        """

        time_filter_dict = dict(self.config.items("Time Filter")) \
            if "Time Filter" in self.config else {}

        if time_filter_dict:
            time_filter_dict["time_filter_use"] = \
                self.config["Time Filter"].getboolean("time_filter_use", fallback=False)

            if time_filter_dict["time_filter_use"]:
                time_filter_dict["start_time"] = datetime.datetime.strptime(
                    time_filter_dict["start_time"], "%Y-%m-%d %H:%M")
                time_filter_dict["end_time"] = datetime.datetime.strptime(
                    time_filter_dict["end_time"], "%Y-%m-%d %H:%M")

        print("time filter: ", time_filter_dict)
        return time_filter_dict

    def read_sign_correction(self) -> dict:
        """Reads the Sign Correction section from config file and converts it to a dictionary
        that is typecasted.

        Variables read into the dictionary (key:value):
        - wrong_sign_removal: True/False
        - data_replacement: drop/nan/zero/day/hour/all.
        - removal_time_frame: day/hour/all
        - fault_placement:start/middle/end/calendar

        Returns:
            sign_correction_dict (dict): dictionary with configuration values for sign correction
        """

        sign_correction_dict = dict(self.config.items("Sign Correction")) \
            if "Sign Correction" in self.config else {}
        if sign_correction_dict:
            sign_correction_dict["wrong_sign_removal"] = \
                self.config["Sign Correction"].getboolean("wrong_sign_removal", fallback=False)

        print("sign correction: ", sign_correction_dict)
        return sign_correction_dict

    def read_duplicate_removal(self) -> dict:
        """Reads the duplicate Removal section from config file and converts it to a dictionary that
        is type casted.

        Variables read into the dictionary:
        - duplicate_removal:True/False
        - data_replacement:first/last/none/average/max/remove
        - removal_time_frame:day/hour/all
        - fault_placement:start/middle/end/calendar

        Returns:
            duplicate_removal_dict (dict): dictionary with configuration values for
            duplicate removal
        """
        duplicate_removal_dict = dict(self.config.items("Duplicate Removal")) \
            if "Duplicate Removal" in self.config else {}

        if duplicate_removal_dict:
            duplicate_removal_dict["duplicate_removal"] = \
                self.config["Duplicate Removal"].getboolean("duplicate_removal", fallback=False)

        print("Duplicate Removal: ", duplicate_removal_dict)
        return duplicate_removal_dict

    def read_nan_handeling(self) -> dict:
        """Reads the nan handling section from config file and converts it to a dictionary that is
        typecasted.

        Variables read into the dictionary:
        - nan_removal:True/False
        - data_replacement:drop/zero/first/last/none/average/max/remove
        - removal_time_frame:day/hour/all
        - fault_placement:start/middle/end/calendar

        Returns:
            nan_handling_dict (dict): dictionary with configuration values for nan handling
        """
        nan_handling_dict = dict(self.config.items("Nan handling")) \
            if "Nan handling" in self.config else {}

        if nan_handling_dict:
            nan_handling_dict["nan_removal"] = \
                self.config["Nan handling"].getboolean("nan_removal", fallback=False)

        print("Nan handling: ", nan_handling_dict)
        return nan_handling_dict

    def read_data_usage(self) -> list:
        """Reads the Data Usage section from config file and converts it to a list of measurement
        types that should be used.

        Values read contain:
        - batteries = True/False
        - solar = True/False
        - node = True/False
        - loads = True/False

        Returns:
            measurement_types_list (list): a list with measurement types to be used
        """
        measurement_types_list = []
        measurement_type_dict = dict(self.config.items("Data Usage")) \
            if "Data Usage" in self.config else {}

        if measurement_type_dict:
            if self.config["Data Usage"].getboolean("batteries", fallback=False):
                measurement_types_list.append("batteries")
            if self.config["Data Usage"].getboolean("solar", fallback=False):
                measurement_types_list.append("solar")
            if self.config["Data Usage"].getboolean("loads", fallback=False):
                measurement_types_list.append("loads")
            if self.config["Data Usage"].getboolean("node", fallback=False):
                measurement_types_list.append("node")

        print("Measurement Type list : ", measurement_types_list)
        return measurement_types_list

    def read_measurement_types(self) -> list:
        """Reads the Data Usage section from config file and converts it to a dictionary of measurement
        types that should be used.

        Values read contain:
        - batteries = True/False
        - solar = True/False
        - node = True/False
        - loads = True/False

        Returns:
            measurement_types_dict (dict): a list with measurement types to be used
        """
        measurement_type_dict = dict(self.config.items("Data Usage")) \
            if "Data Usage" in self.config else {}

        if measurement_type_dict:
            measurement_type_dict["batteries"] = self.config["Data Usage"].getboolean("batteries", fallback=False)
            measurement_type_dict["solar"] = self.config["Data Usage"].getboolean("solar", fallback=False)
            measurement_type_dict["loads"] = self.config["Data Usage"].getboolean("loads", fallback=False)

        print("Measurement Type list : ", measurement_type_dict)
        return measurement_type_dict

    def read_resampling(self) -> dict:
        """Reads the resample section from config file and converts it to a dictionary that is type
        casted.

        The user my choose to resample the index. If so he can choose a time step to which the data
        is resampled.The time step can have a unit possible units are :
        - h 	hour 	+/- 1.0e15 years 	[1.0e15 BC, 1.0e15 AD]
        - m 	minute 	+/- 1.7e13 years 	[1.7e13 BC, 1.7e13 AD]
        - s 	second 	+/- 2.9e12 years 	[ 2.9e9 BC, 2.9e9 AD]

        the user may choose how he would like to handle fills in upsampled data

        Variables read into the dictionary:
        - resampeling = True
        - resampeling_step = 8
        - resampeling_unit = m
        - resampeling_strategie_upsampeling = first/last
        """
        resampling_dict = dict(self.config.items("Resample")) if "Resample" in self.config else {}

        if resampling_dict:
            resampling_dict["resampling"] = \
                self.config["Resample"].getboolean("resampling", fallback=False)

            resampling_dict["resampling_step"] = \
                self.config["Resample"].getint("resampling_step")

        print("resampling : ", type(resampling_dict), resampling_dict)
        return resampling_dict


    def read_refill(self) -> dict:
        """Reads the refill section from config file and converts it to a dictionary that is
        typecasted.

        Variables read into the dictionary:
        - data_refill(bool) :True/False
        - days (int) : 7
        - attempts: 7
        - threshold: 5
        - forward_fill = True
        - backward_fill = True

        Returns:
            refill_dict (dict): dictionary with configuration values for refilling data
        """
        refill_dict = dict(self.config.items("Refill")) \
            if "Refill" in self.config else {}

        if refill_dict:
            refill_dict["data_refill"] = self.config["Refill"].getboolean("data_refill",
                                                                          fallback=False)
            refill_dict["forward_fill"] = self.config["Refill"].getboolean("forward_fill",
                                                                           fallback=False)
            refill_dict["backward_fill"] = self.config["Refill"].getboolean("backward_fill",
                                                                            fallback=False)
            refill_dict["days"] = self.config["Refill"].getint("days")
            refill_dict["attempts"] = self.config["Refill"].getint("attempts")
            refill_dict["threshold"] = self.config["Refill"].getint("threshold")

        print("Refill: ", refill_dict)
        return refill_dict

    def read_optimiser_objective_set(self) -> str:
        """ Reads the optimiser Set from config file.

            Returns:
                String with name of the optimiser set to be used
        """
        optimiser_set_dict = dict(self.config.items("Optimiser Objective Set"))\
            if "Optimiser Objective Set" in self.config else {}

        optimiser_set_name = optimiser_set_dict["optimiserobjectiveset"]

        print("optimiser set: ", optimiser_set_name)
        return optimiser_set_name

    def read_optimiser_objectives(self) -> list:
        """ Reads the optimiser objectives from config file.

            Returns:
                List with name of the objectives choosen to be used
        """
        optimiser_objectives_dict = dict(self.config.items("Optimiser Objectives"))\
            if "Optimiser Objectives" in self.config else {}

        if optimiser_objectives_dict:
            optimiser_objectives_dict["ConnectionPointCost"] = self.config["Optimiser Objectives"].getboolean("ConnectionPointCost", fallback=False)
            optimiser_objectives_dict["ConnectionPointEnergy"] = self.config["Optimiser Objectives"].getboolean("ConnectionPointEnergy", fallback=False)
            optimiser_objectives_dict["ThroughputCost"] = self.config["Optimiser Objectives"].getboolean("ThroughputCost", fallback=False)
            optimiser_objectives_dict["Throughput"] = self.config["Optimiser Objectives"].getboolean("Throughput", fallback=False)
            optimiser_objectives_dict["GreedyGenerationCharging"] = self.config["Optimiser Objectives"].getboolean("GreedyGenerationCharging", fallback=False)
            optimiser_objectives_dict["GreedyDemandDischarging"] = self.config["Optimiser Objectives"].getboolean("GreedyDemandDischarging", fallback=False)
            optimiser_objectives_dict["EqualStorageActions"] = self.config["Optimiser Objectives"].getboolean("EqualStorageActions", fallback=False)
            optimiser_objectives_dict["ConnectionPointPeakPower"] = self.config["Optimiser Objectives"].getboolean("ConnectionPointPeakPower", fallback=False)
            optimiser_objectives_dict["ConnectionPointQuantisedPeak"] = self.config["Optimiser Objectives"].getboolean("ConnectionPointQuantisedPeak", fallback=False)
            optimiser_objectives_dict["PiecewiseLinear"] = self.config["Optimiser Objectives"].getboolean("PiecewiseLinear", fallback=False)
            optimiser_objectives_dict["LocalModelsCost"] = self.config["Optimiser Objectives"].getboolean("LocalModelsCost", fallback=False)
            optimiser_objectives_dict["LocalGridMinimiser"] = self.config["Optimiser Objectives"].getboolean("LocalGridMinimiser", fallback=False)
            optimiser_objectives_dict["LocalThirdParty"] = self.config["Optimiser Objectives"].getboolean("LocalThirdParty", fallback=False)
            optimiser_objectives_dict["LocalGridPeakPower"] = self.config["Optimiser Objectives"].getboolean("LocalGridPeakPower", fallback=False)

        print("Objectives: ", optimiser_objectives_dict)
        return optimiser_objectives_dict

    def read_inverter(self) -> dict:
        """ Reads the inverters from config file.

            Returns:
                dict with inverter settings
        """

        inverter_dict = dict(self.config.items("Inverter")) \
            if "Inverters" in self.config else {}

        inverter_dict["charging_power_limit"] = self.config["Inverter"].getfloat("charging_power_limit")
        inverter_dict["discharging_power_limit"] = self.config["Inverter"].getfloat("discharging_power_limit")
        inverter_dict["charging_efficiency"] = self.config["Inverter"].getfloat("charging_efficiency")
        inverter_dict["discharging_efficiency"] = self.config["Inverter"].getfloat("discharging_efficiency")
        inverter_dict["charging_reactive_power_limit"] = self.config["Inverter"].getfloat("charging_reactive_power_limit")
        inverter_dict["discharging_reactive_power_limit"] = self.config["Inverter"].getfloat("discharging_reactive_power_limit")
        inverter_dict["reactive_charging_efficiency"] = self.config["Inverter"].getfloat("reactive_charging_efficiency")
        inverter_dict["reactive_discharging_efficiency"] = self.config["Inverter"].getfloat("reactive_discharging_efficiency")

        print("Inerter: ", inverter_dict)
        return inverter_dict

    def read_energy_storage(self) -> dict:
        """ Reads the energy storage from config file.

            Returns:
                dict with energy storage settings
        """

        energy_storage_dict = dict(self.config.items("EnergyStorage")) \
            if "EnergyStorage" in self.config else {}

        # energy_storage_dict["node_id"] = self.config["EnergyStorage"].getint("node_id")
        energy_storage_dict["max_capacity"] = self.config["EnergyStorage"].getfloat("max_capacity")
        energy_storage_dict["depth_of_discharge_limit"] = self.config["EnergyStorage"].getfloat("depth_of_discharge_limit")
        energy_storage_dict["charging_power_limit"] = self.config["EnergyStorage"].getfloat("charging_power_limit")
        energy_storage_dict["discharging_power_limit"] = self.config["EnergyStorage"].getfloat("discharging_power_limit")
        energy_storage_dict["charging_efficiency"] = self.config["EnergyStorage"].getfloat("charging_efficiency")
        energy_storage_dict["discharging_efficiency"] = self.config["EnergyStorage"].getfloat("discharging_efficiency")
        energy_storage_dict["throughput_cost"] = self.config["EnergyStorage"].getfloat("throughput_cost")
        energy_storage_dict["initial_state_of_charge"] = self.config["EnergyStorage"].getfloat("initial_state_of_charge")

        print("energy storage: ", energy_storage_dict)
        return energy_storage_dict

    def read_energy_system(self) -> dict:
        """ Reads the energy system from config file.
        
            Returns:
                dict with energy system compoments
        """

        energy_system_dict = dict(self.config.items("EnergySystem")) \
            if "EnergySystem" in self.config else {}

        energy_system_dict["energy_storage"] = self.config["EnergySystem"].getboolean("energy_storage", fallback=False)
        energy_system_dict["inverter"] = self.config["EnergySystem"].getboolean("inverter", fallback=False)
        energy_system_dict["generation"] = self.config["EnergySystem"].getboolean("generation", fallback=False)
        energy_system_dict["is_hybrid"] = self.config["EnergySystem"].getboolean("is_hybrid", fallback=False)

        print("energy system: ", energy_system_dict)
        return energy_system_dict

    def read_tariff_factors(self) -> dict:
        """ Reads the tariff factors from config file.

            Returns:
                dict with tariff factors
        """

        tariff_Factors_dict = dict(self.config.items("TariffFactors")) \
            if "TariffFactors" in self.config else {}

        tariff_Factors_dict["lt_import_factor"] = self.config["TariffFactors"].getfloat("lt_import_factor")
        tariff_Factors_dict["lt_export_factor"] = self.config["TariffFactors"].getfloat("lt_export_factor")
        tariff_Factors_dict["rt_export_factor"] = self.config["TariffFactors"].getfloat("rt_export_factor")
        tariff_Factors_dict["rt_import_factor"] = self.config["TariffFactors"].getfloat("rt_import_factor")
        tariff_Factors_dict["subscription_fee"] = self.config["TariffFactors"].getfloat("subscription_fee")

        print("tariff factors: ", tariff_Factors_dict)
        return tariff_Factors_dict

    def read_scenario_info(self) ->dict:
        scenario_dict = dict(self.config.items("Scenario")) \
            if "Scenario" in self.config else {}

        print("Scenario Information: ", scenario_dict)
        return scenario_dict
