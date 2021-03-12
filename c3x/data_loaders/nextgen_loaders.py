"""Next Gen Loaders contains a class of functions that loads a next gen data set, returning the data in a
consistent, well structured format.

Data is read as it is and is not cleaned in anyway in this class (if cleaning is required see
preparation.cleaners).

"""

import os
import pandas
import numpy


class NextGenData:
    """This classes is responsible for loading and managing next gen data sets provided by Reposit
    The data provided can be read and converted measurement data. Data is
    read and stored as an intermediate step in a folder "Data", each measurement type has its own
    sub folder (loads, batteries, node, solar) and is aggregated per node. To handle huge amounts of
    data, it is read in batches. Each file is labeled with the type, node id and batch number
    (measurement_type_nodeID_batchnumber). Batches can be concatenated and stored per node. The data is
    stored as dataframes
    """

    def __init__(self, data_name: str, source: str, batteries: str = None, solar: str = None,
                 node: str = None, loads: str = None, results: str = None, stats: str = None,
                 number_of_batches: int = 30, files_per_batch: int = 30,
                 concat_batches_start: int = 0, concat_batches_end: int = 1):
        """Creates a Data object for measurement data.

        It sets up the folder structure for later use. Paths can be adjusted to user needs or used
        with defaults. If any of the destination folders are not provided a default will be create
        at ./data/FOLDERNAME

        Batches can be used to separate data into smaller units. The user can choose which batches
        they wish to concatenate after cleaning later on.

        Args:
            data_name (str): Human readable short hand name of data set.
            source (str): Path to data source (if not provided FileNotFoundError.
            batteries (str, None): Path to where batteries data is stored (destination).
            solar (str, None): Path to where solar data is stored (destination).
            node (str, None): Path to where node data is stored (destination).
            loads (str, None): Path to where loads data is stored (destination).
            results (str, None): Path to where concatenated and cleaned data is stored
                (destination).
            stats (str, None): Path to where statitsics are stored
                (destination).
            number_of_batches (int, 30): Integer with the number of batches that should be used.
            files_per_batch (int, 30): The number of files in each batch.
            concat_batches_start (int, 0): Integer that determines with which batch concatenation
                starts.
            concat_batches_end (int, 1): Integer that determines with which batch concatenation
                ends.

        """
        self.batch_info = {}
        self.data_dir = {}

        if source is None or not os.path.isdir(source):
            raise FileNotFoundError

        if batteries is None:
            batteries = "data/batteries"
        if solar is None:
            solar = "data/solar"
        if loads is None:
            loads = "data/loads"
        if node is None:
            node = "data/node"
        if results is None:
            results = "data/result"
        if stats is None:
            stats = "data/stats"

        self.data_dir["source"] = source
        self.data_dir["batteries"] = batteries
        self.data_dir["solar"] = solar
        self.data_dir["loads"] = loads
        self.data_dir["node"] = node
        self.data_dir["results"] = results
        self.data_dir["stats"] = stats

        for key in self.data_dir.keys():
            if key != "source":
                try:
                    os.makedirs(self.data_dir[key])
                except OSError:
                    print("Creation of the directories failed (folders already there?)", self.data_dir[key])

        self.data_name = data_name

        self.data_files = os.listdir(self.data_dir["source"])
        self.data_files = sorted([f for f in self.data_files if f.endswith('.csv')])

        if len(self.data_files) < files_per_batch:
            print("Warning: not enough files for batching, set to appropriate values")
            files_per_batch = 1
            concat_batches_start = 0
            concat_batches_end = 1

        self.batch_info["number_of_batches"] = number_of_batches
        self.batch_info["concat_batches_start"] = concat_batches_start
        self.batch_info["concat_batches_end"] = concat_batches_end
        self.batch_info["files_per_batch"] = files_per_batch

        self.node_measurement_dict = {}

        if data_name == 'NextGen':
            batches = numpy.arange(self.batch_info["number_of_batches"])
            for batch in batches:
                self.load_nextgen_processing(batch)

    def load_nextgen_processing(self, batch: int = 0):
        """Loads NextGen data provided by Reposit.

        The function can only read CSV-Files in a specific format. Dataframes are created and stored
        to disk, sorted via node_id. All measurements are saved as separate data files per batch so
        that they can be processed and filtered later on according to user requirements.

        All Data is stored to a "data" folder which needs to contain sub folders (batteries, solar,
        node and loads) for each measurement type. Each file is identified by a batch number.

        Note: timestamps refer to the start of measurement period in UTC.
        Note: Data is generally in kW, kVA etc. See Readme.md for details.

        Args:
            batch (integer, 0): Batchnumber that processing is working on

        """
        batch_start = batch * self.batch_info["files_per_batch"]
        batch_end = (batch + 1) * self.batch_info["files_per_batch"]
        data_files = self.data_files[batch_start:batch_end]

        raw_df = pandas.concat([pandas.read_csv(os.path.join(self.data_dir["source"], f))
                                for f in data_files])

        raw_df = raw_df.rename(columns={'major': 'utc'})
        raw_df = raw_df.set_index('utc')

        # Note the time intervals of the raw data, even though it's 1.
        info_df = pandas.read_json(os.path.join(self.data_dir["source"], "deployment_info.json"))
        info_df = info_df.set_index('id')
        info_df.to_pickle(self.data_dir["results"] + "/node_info.npy")

        node_names = sorted(raw_df.identifier.unique())

        for id_name in node_names:
            node_df = raw_df.loc[raw_df['identifier'] == id_name]

            # create measurement data for loads
            power = pandas.DataFrame(-node_df['solarPower']
                                     - node_df['batteryPower']
                                     + node_df['meterPower'])
            reactive_power = pandas.DataFrame(node_df['meterReactivePower']
                                              - node_df['batteryReactivePower'])

            loads_df = pandas.DataFrame(pandas.concat([power, reactive_power], axis=1))
            loads_df.set_index(node_df.index)

            # create measurements for solar
            solar_df = pandas.DataFrame(node_df['solarPower'])
            solar_df.set_index(node_df.index)

            # create a measurements for battery
            batt_p_q_c = pandas.concat([node_df['batteryPower'],
                                        node_df['batteryReactivePower'],
                                        node_df['remainingCharge'].apply(lambda x: x / 1000)],
                                       axis=1)

            batt_p_q_c.set_index(node_df.index)

            v_f = node_df[['meterVoltage', 'meterFrequency']]

            # save data to different folder so it can be read seperatly if needed
            batt_p_q_c.to_pickle(self.data_dir["batteries"] + "/measurement_batteries_"
                                 + str(id_name) + "_" + str(batch) + '.npy')
            solar_df.to_pickle(self.data_dir["solar"] + "/measurement_solar_"
                               + str(id_name) + "_" + str(batch) + '.npy')
            loads_df.to_pickle(self.data_dir["loads"] + "/measurement_loads_"
                               + str(id_name) + "_" + str(batch) + '.npy')
            v_f.to_pickle(self.data_dir["node"] + "/measurement_node_"
                          + str(id_name) + "_" + str(batch) + '.npy')

    def create_node_list(self, meas_type="batteries") -> list:
        """ Creates a list of node_ids for a given type of measurement.

        Args:
            meas_type (str, "batteries"): The measurement type. Acceptable values are batteries,
                solar, loads, node.

        Returns
            node_ids (list): list of IDs (empty if no nodes are found).

        """
        # Location and file names of incoming data set.
        nodes = os.listdir(self.data_dir[meas_type])
        nodes = sorted([f for f in nodes if f.endswith('.npy')])

        node_ids = []

        for node in nodes:
            parts = node.split('_')
            if len(parts)>3:
                node_id = node.split('_')[2]

                if node_id not in node_ids:
                    node_ids.append(node_id)

        return node_ids

    def concat_data(self, meas_type: str = "batteries", concat_batches_start: int = 0,
                    concat_batches_end: int = 1):
        """Concatenates batches to one dataset per node.

        A start and an end for the concatenation can be choosen by using the batch_info at init.
        Concatenated data is saved to a file per node and labeled with the node id.

        Args:
            meas_type (str, "batteries"): The type of measurement to be concatenated. Acceptable
                values are batteries, solar, loads, node.
            concat_batches_start (int, 0): The batch number to start the concat at.
            concat_batches_end (int, 1): The batch number to end the concat at.

        """
        batches = numpy.arange(concat_batches_start, concat_batches_end)

        if meas_type == "loads":
            path = self.data_dir["loads"] + "/measurement_loads_"
        elif meas_type == "batteries":
            path = self.data_dir["batteries"] + "/measurement_batteries_"
        elif meas_type == "solar":
            path = self.data_dir["solar"] + "/measurement_solar_"
        elif meas_type == "node":
            path = self.data_dir["node"] + "/measurement_node_"

        node_ids = self.create_node_list(meas_type)

        for node in node_ids:
            first_run = True
            measurement_df = pandas.DataFrame()
            for batch in batches:
                try:
                    print("working on batch: ", batch, "node Id: ",
                          node, "path: ", path + str(node) + '_' + str(batch) + '.npy')
                    measurement_df_tmp = pandas.read_pickle(path
                                                            + str(node) + '_'
                                                            + str(batch) + '.npy')

                    os.remove(path + str(node) + '_' + str(batch) + '.npy')

                    if first_run is True:
                        measurement_df = pandas.DataFrame(measurement_df_tmp)
                    else:
                        measurement_df = pandas.concat([measurement_df, measurement_df_tmp], axis=0)
                    first_run = False
                except FileNotFoundError:
                    print("FILE NOT FOUND. Data may have been empty. Move on to next file")

            if not measurement_df.empty:
                measurement_df.to_pickle(path + str(node) + "_node" + '.npy')
            else:
                print("measurement data for node ", node, "is empty, no file create")

    def to_measurement_data(self):
        """Collects data to build a measuring dictionary.

        The data is saved to a node file with one measurement per node.

        Returns:
            node_network_dict: A network without any topology but with measurements.

        """
        node_ids = self.create_node_list(meas_type="batteries")
        node_ids_solar = self.create_node_list(meas_type="solar")
        node_ids_loads = self.create_node_list(meas_type="loads")

        node_ids.extend(x for x in node_ids_solar if x not in node_ids)
        node_ids.extend(x for x in node_ids_loads if x not in node_ids)

        full_dict = {}

        for node_id in node_ids:
            meas_dict = {}

            try:
                measurement_df_loads_temp = pandas.read_pickle(self.data_dir["loads"]
                                                               + "/measurement_loads_"
                                                               + str(node_id) + "_node" + '.npy')
                measurement_df_loads_temp.columns = ["PLG", "QLG"]

                measurement_df_solar_temp = pandas.read_pickle(self.data_dir["solar"]
                                                               + "/measurement_solar_"
                                                               + str(node_id) + "_node" + '.npy')
                measurement_df_solar_temp.columns = ["PLG"]

                measurement_df_batteries_temp = pandas.read_pickle(self.data_dir["batteries"]
                                                                   + "/measurement_batteries_"
                                                                   + str(node_id) + "_node"
                                                                   + '.npy')
                measurement_df_batteries_temp.columns = ["PLG", "QLG", "RC"]

                meas_dict = {"loads_" + node_id: measurement_df_batteries_temp,
                             "solar_" + node_id: measurement_df_solar_temp,
                             "battery_" + node_id: measurement_df_loads_temp}

                numpy.save(self.data_dir["results"] + "/node_" + str(node_id),
                           meas_dict, allow_pickle=True)

                full_dict[node_id] = meas_dict
            except FileNotFoundError:
                print("node ", node_id, " has insufficient data .. No data added to dictionary")

        return full_dict
