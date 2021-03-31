""" Module to parse a config file and convert read sections to dictionaries of Keys:Values """

import configparser
import datetime


class ConfigFileParser:
    """The module reads a config file. Values to be read are group in sections Batches, Data Path,
    Data Usage, Time Filter, Sign Correction, Duplicate Removal, Nan handling and Resample. Each of
    these section can be read separately and the used as parameters for various functions

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
                    time_filter_dict["start_time"], "%Y-%m-%d %H:%M").strftime("%s")
                time_filter_dict["end_time"] = datetime.datetime.strptime(
                    time_filter_dict["end_time"], "%Y-%m-%d %H:%M").strftime("%s")

        print("time filter: ", time_filter_dict)
        return time_filter_dict

    def read_sign_correction(self) -> dict:
        """Reads the Sign Correction section from config file and converts it to a dictionary
        that is typecasted.

        Variables read into the dictionary (key:value):
            wrong_sign_removal: True/False
            data_replacement: drop/nan/zero/day/hour/all.
            removal_time_frame: day/hour/all
            fault_placement:start/middle/end/calendar

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
            duplicate_removal:True/False
            data_replacement:first/last/none/average/max/remove
            removal_time_frame:day/hour/all
            fault_placement:start/middle/end/calendar

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
            nan_removal:True/False
            data_replacement:drop/zero/first/last/none/average/max/remove
            removal_time_frame:day/hour/all
            fault_placement:start/middle/end/calendar

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
            batteries = True/False
            solar = True/False
            node = True/False
            loads = True/False

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

    def read_resampling(self) -> dict:
        """Reads the resample section from config file and converts it to a dictionary that is type
        casted.

        The user my choose to resample the index. If so he can choose a time step to which the data
        is resampled.The time step can have a unit possible units are :
            h 	hour 	+/- 1.0e15 years 	[1.0e15 BC, 1.0e15 AD]
            m 	minute 	+/- 1.7e13 years 	[1.7e13 BC, 1.7e13 AD]
            s 	second 	+/- 2.9e12 years 	[ 2.9e9 BC, 2.9e9 AD]

        the user may choose how he would like to handle fills in upsampled data

        Variables read into the dictionary:
            resampeling = True
            resampeling_step = 8
            resampeling_unit = m
            resampeling_strategie_upsampeling = first/last
        """
        resampling_dict = dict(self.config.items("Resample")) if "Resample" in self.config else {}

        if resampling_dict:
            resampling_dict["resampling"] = \
                self.config["Resample"].getboolean("resample", fallback=False)

            resampling_dict["resampling_step"] = \
                self.config["Resample"].getint("resampling_step")

        print("resampling : ", type(resampling_dict), resampling_dict)
        return resampling_dict


    def read_refill(self) -> dict:
        """Reads the nan handling section from config file and converts it to a dictionary that is
        typecasted.

        Variables read into the dictionary:
            nan_removal:True/False
            data_replacement:drop/zero/first/last/none/average/max/remove
            removal_time_frame:day/hour/all
            fault_placement:start/middle/end/calendar

        Returns:
            nan_handling_dict (dict): dictionary with configuration values for nan handling
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
