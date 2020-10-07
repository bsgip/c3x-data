"""Financial calculations for energy.

    This module contains financial calculations based on solar power and batteries
    in a given network. The networks used are defined as network objects (see evolve parsers).

    Todo:
        * Add inverters: Inverters are not considered at the moment
        * Improve Nan Handeling
"""

import numpy
import pandas as pd
from c3x.data_cleaning import unit_conversion


def meter_power(meas_dict: dict, meter: int, axis: int = 0) -> pd.Series:
    """
    calculates the power for a meter of individual measurement points
    by summing load, solar and battery power

    Args:
       meas_dict (dict): dict with measurement for one or multiple nodes.
       meter(int): Id for a meter
       axis (int): how data is concatenated for results

    return:
        meter_p (pd.Series): combined power (solar, battery, load)
    """
    meter_p = pd.DataFrame()

    if meas_dict[meter]:
        meter_p = pd.DataFrame()
        for meas in meas_dict[meter]:
            if 'loads' in meas:
                meter_p = pd.concat([meter_p, meas_dict[meter][meas]["PLG"]], axis=axis)
            elif 'solar' in meas:
                meter_p = pd.concat([meter_p, meas_dict[meter][meas]["PLG"]], axis=axis)
            elif 'batteries' in meas:
                meter_p = pd.concat([meter_p, meas_dict[meter][meas]["PLG"]], axis=axis)
    meter_p = meter_p.sum(axis=1)
    return meter_p


def financial(meter_p: pd.Series, import_tariff: pd.Series, export_tariff: pd.Series) -> pd.Series:
    """
        Evaluate the financial outcome for a customer.
        A conversion from kW to kWh is handled internally

        Note: assumes constant step size in timestamps (use forth index beforehand)

        Args:
            meter_p (pd.Series ): Power of a node
            import_tariff (pd.Series): Expects this to be in $/kWh.
            export_tariff (pd.Series): Expects this to be in $/kWh.
        Returns:
            cost (pd.Series): cost per measurement point, using import and export tariffs
    """

    # Note: need to ensure meter data is converted to kWh
    timestep = numpy.timedelta64(meter_p.index[1] - meter_p.index[0])
    meter = unit_conversion.convert_watt_to_watt_hour(meter_p, timedelta=timestep)

    import_power_cost = numpy.sum(meter >= 0)
    export_power_revenue = numpy.sum(meter < 0)

    cost = import_power_cost * import_tariff + export_power_revenue*export_tariff

    return cost


def customer_financial(meas_dict: dict, node_keys: list = None, tariff: dict = None) -> dict:
    """
        Evaluate the financial outcome for a selected customer or for all customers.

        Note: not currently setup to handle missing data (eg NANs)

        #TODO: consider inverters and how to avoid double counting with solar, batteries

        Args:
            meas_dict (dict): dict with measurement for one or multiple nodes.
            node_keys (list): list nodes for which financials are calculated.
            tariff (dict): nodes tariff data. Expects this to be in $/kWh.

        Returns:
            results_dict: cost per node and the average cost over all nodes
    """

    results_dict = {}
    average = []

    nodes = node_keys if node_keys else meas_dict.keys()

    for key in nodes:
        if type(key) == int:
            key = key.tostr()
        if meas_dict[key]:
            if key in tariff:
                meter_p = meter_power(meas_dict, key, axis=0)
                meter_p_cost = financial(meter_p,
                                         tariff[key]['import_tariff'],
                                         tariff[key]['export_tariff'])
                results_dict[key] = meter_p_cost

    initiate = 0
    for node in results_dict.values():
        average = node if initiate == 0 else average.append(node)
        initiate = 1

    average = numpy.nanmean(average)
    results_dict["average"] = average

    return results_dict


def customer_cost_financial(tariff: dict, energy_grid_load: pd.Series, energy_solar_grid: pd.Series,
                            energy_battery_load: pd.Series, energy_solar_battery: pd.Series,
                            energy_solar_load: pd.Series) -> pd.Series:
    """
        evaluates the customers cost

        Args:
            tariff: specifies tariffs to be applied to aggregation of customers.
            energy_grid_load: specifies the energy flow between grid and load
            energy_solar_grid: specifies the energy flow between solar and gird
            energy_battery_load: specifies the energy flow between battery and load
            energy_solar_battery: specifies the energy flow between solar and battery
            energy_solar_load: specifies the energy flow between solar and load

        Returns:
            customer_cost (pd.Series):
    """
    customer_cost = financial(energy_grid_load, tariff['re_import_tariff'], 0)
    customer_cost += financial(energy_grid_load, tariff['rt_import_tariff'], 0)
    customer_cost += financial(energy_battery_load, tariff['le_import_tariff'], 0)
    customer_cost += financial(energy_battery_load, tariff['lt_import_tariff'], 0)
    customer_cost -= financial(energy_solar_grid, tariff['re_export_tariff'], 0)
    customer_cost += financial(energy_solar_grid, tariff['rt_export_tariff'], 0)
    customer_cost -= financial(energy_solar_battery, tariff['le_export_tariff'], 0)
    customer_cost += financial(energy_solar_battery, tariff['lt_export_tariff'], 0)
    customer_cost -= financial(energy_solar_battery, tariff['le_export_tariff'], 0)
    customer_cost += financial(energy_solar_load, tariff['lt_import_tariff'], 0)
    customer_cost += financial(energy_solar_load, tariff['lt_export_tariff'], 0)

    return customer_cost


def battery_cost_financial(tariff: dict, energy_grid_battery: pd.Series,
                           energy_battery_grid: pd.Series, energy_battery_load: pd.Series,
                           energy_solar_battery: pd.Series) -> pd.Series:
    """
       evaluates the battery cost

       Args:
            tariff (dict): specifies tariffs to be applied to aggregation of customers.
            energy_grid_battery (pd.Series): specifies the energy flow between grid and battery
            energy_battery_grid (pd.Series): specifies the energy flow between battery and gird
            energy_battery_load (pd.Series): specifies the energy flow between battery and load
            energy_solar_battery (pd.Series): specifies the energy flow between solar and battery

        Returns:
            battery_cost (pd.Series):
    """
    battery_cost = financial(energy_solar_battery, tariff['le_import_tariff'], 0)
    battery_cost += financial(energy_solar_battery, tariff['lt_import_tariff'], 0)
    battery_cost -= financial(energy_battery_load, tariff['le_export_tariff'], 0)
    battery_cost += financial(energy_battery_load, tariff['lt_export_tariff'], 0)
    battery_cost += financial(energy_grid_battery, tariff['re_import_tariff'], 0)
    battery_cost += financial(energy_grid_battery, tariff['rt_import_tariff'], 0)
    battery_cost -= financial(energy_battery_grid, tariff['re_export_tariff'], 0)
    battery_cost += financial(energy_battery_grid, tariff['rt_export_tariff'], 0)

    return battery_cost


def network_cost_financial(tariff: dict, energy_grid_load: pd.Series,
                           energy_grid_battery: pd.Series, energy_battery_grid: pd.Series,
                           energy_battery_load: pd.Series, energy_solar_battery: pd.Series,
                           energy_solar_load: pd.Series) -> pd.Series:
    """
        evaluates the network cost

        Args:
            tariff (dict): specifies tariffs to be applied to aggregation of customers.
            energy_grid_load (pd.Series): specifies the energy flow between grid and load
            energy_grid_battery (pd.Series): specifies the energy flow between grid and battery
            energy_battery_grid (pd.Series): specifies the energy flow between battery and grid
            energy_battery_load (pd.Series): specifies the energy flow between battery and solar
            energy_solar_battery (pd.Series) : specifies the energy flow between solar and battery
            energy_solar_load (pd.Series): specifies the energy flow between solar and load

        Returns:
            network_cost(pd.Series)
    """

    network_cost = -financial(energy_grid_load, tariff['rt_import_tariff'], 0)
    network_cost -= financial(energy_battery_load, tariff['lt_import_tariff'], 0)
    network_cost -= financial(energy_battery_load, tariff['lt_export_tariff'], 0)
    network_cost -= financial(energy_solar_battery, tariff['lt_import_tariff'], 0)
    network_cost -= financial(energy_solar_battery, tariff['lt_export_tariff'], 0)
    network_cost -= financial(energy_grid_battery, tariff['rt_import_tariff'], 0)
    network_cost -= financial(energy_battery_grid, tariff['rt_export_tariff'], 0)
    network_cost -= financial(energy_solar_load, tariff['lt_import_tariff'], 0)
    network_cost -= financial(energy_solar_load, tariff['lt_export_tariff'], 0)

    return network_cost


def lem_financial(customer_tariffs, energy_grid_load, energy_grid_battery, energy_solar_grid,
                  energy_battery_grid, energy_battery_load, energy_solar_battery,
                  energy_solar_load, battery_tariffs=None):
    """
        evaluate the cost for the local energy model

        Args:
            customer_tariffs: specifies tariffs to be applied to aggregation of customers.
            energy_grid_load (pd.series): specifies the energy flow between grid and load
            energy_grid_battery: specifies the energy flow between grid and battery
            energy_solar_grid: specifies the energy flow between solar and grid
            energy_battery_grid: specifies the energy flow between battery and grid
            energy_battery_load: specifies the energy flow between battery and solar
            energy_solar_battery: specifies the energy flow between solar and battery
            energy_solar_load: specifies the energy flow between solar and load
            battery_tariffs: specifies tariffs to be applied to aggregation of battery.
                            (if none given customer_tariffs ware used)

        Returns:
            customer_cost, battery_cost, network_cost
    """

    customer_cost = customer_cost_financial(customer_tariffs, energy_grid_load, energy_solar_grid,
                                            energy_battery_load, energy_solar_battery,
                                            energy_solar_load)

    bt_choice = battery_tariffs if battery_tariffs else customer_tariffs

    battery_cost = battery_cost_financial(bt_choice, energy_grid_battery, energy_battery_grid,
                                          energy_battery_load, energy_solar_battery)

    network_cost = network_cost_financial(customer_tariffs, energy_grid_load, energy_grid_battery,
                                          energy_battery_grid, energy_battery_load,
                                          energy_solar_battery, energy_solar_load)

    return customer_cost, battery_cost, network_cost


def peak_powers(meas_dict: dict, node_keys: list = None) -> dict:
    """
        Calculate the peak power flows into and out of the network.
        #TODO: consider selecting peak powers per phase
        #TODO: consider inverters and how to avoid double counting with solar, batteries

        Args:
            meas_dict (dict): dict with measurement for one or multiple nodes.
            node_keys (list): list of Node.names in Network.nodes.

        Returns:
            results_dict (dict): dictionary of peak power into and out of network in kW,
                          and in kW/connection point.
    """

    nodes = node_keys if node_keys else meas_dict.keys()
    sum_meter_power = pd.DataFrame([])

    for key in nodes:
        if type(key) == int:
            key = key.tostr()
        if meas_dict[key]:
            meter_p = meter_power(meas_dict, key, axis=1)
            if sum_meter_power.empty:
                sum_meter_power = meter_p.copy()
            else:
                sum_meter_power = pd.concat([sum_meter_power, meter_p], axis=1, sort=True)
    sum_power = sum_meter_power.sum(axis=1)
    aver_power = numpy.nanmean(sum_meter_power, axis=1)

    return {"peak_power_import": numpy.max(sum_power),
            "peak_power_export": numpy.min(sum_power),
            "peak_power_import_av": numpy.max(aver_power),
            "peak_power_export_av": numpy.min(aver_power),
            "peak_power_import_index": sum_power.idxmax(),
            "peak_power_export_index": sum_power.idxmax()}


def self_sufficiency(load_p: pd.DataFrame, solar_p: pd.DataFrame, battery_p: pd.DataFrame):
    """
        Self-sufficiency = 1 - imports / consumption

        Note: the function expects a full index

        #TODO: consider inverters and how to avoid double counting with solar, batteries

        Args:
            load_p (pd.dataframe): measurement data for load of a s single node.
            solar_p (pd.dataframe): measurement data for solar of a s single node.
            battery_p(pd.dataframe): measurement data for battery of a s single node.

        Returns:
            results_dict: self_consumption_solar, self_consumption_batteries
    """
    self_sufficiency_solar = numpy.nan
    self_sufficiency_battery = numpy.nan

    if not load_p.empty:
        net_load_solar = pd.concat((load_p, solar_p), axis=1).sum(axis=1)
        net_load_solar_battery = pd.concat((load_p, solar_p, battery_p), axis=1).sum(axis=1)

        #create an array that contains which entries are import and which are export
        mask_import_solar = (net_load_solar >= 0)
        mask_import_solar_battery = (net_load_solar_battery >= 0)

        net_import_solar = net_load_solar * mask_import_solar
        net_import_solar_battery = net_load_solar_battery * mask_import_solar_battery

        sum_load = numpy.nansum(load_p)
        sum_solar = numpy.nansum(solar_p)

        if sum_solar < 0:
            self_sufficiency_solar = 1 - (numpy.nansum(net_import_solar) / sum_load)
            self_sufficiency_battery = 1 - (numpy.nansum(net_import_solar_battery) / sum_load)
    else:
        print("Warning: not enough data to calculate")

    return {"self_sufficiency_solar": self_sufficiency_solar,
            "self_sufficiency_batteries": self_sufficiency_battery}


def self_consumption(load_p: pd.DataFrame, solar_p: pd.DataFrame, battery_p: pd.DataFrame) -> dict:
    """
        Self-consumption = 1 - exports / generation

        Note: the function expects a full index

        #TODO: consider inverters and how to avoid double counting with solar, batteries

        Args:
            load_p (pd.dataframe): measurement data for load of a s single node.
            solar_p (pd.dataframe): measurement data for solar of a s single node.
            battery_p(pd.dataframe): measurement data for battery of a s single node.

        Retruns:
            results_dict: self_consumption_solar, self_consumption_batteries
    """

    net_load_solar = pd.concat((load_p, solar_p), axis=1).sum(axis=1)
    net_load_solar_battery = pd.concat((load_p, solar_p, battery_p), axis=1).sum(axis=1)

    # create an array that contains which entries are import and which are export
    mask_export_solar = (net_load_solar < 0)
    mask_export_solar_battery = (net_load_solar_battery < 0)

    net_export_solar = net_load_solar * mask_export_solar
    net_import_solar_battery = net_load_solar_battery * mask_export_solar_battery

    sum_solar = numpy.nansum(solar_p)

    self_consumption_solar = numpy.nan
    self_consumption_battery = numpy.nan

    if sum_solar < 0:
        self_consumption_solar = 1 - (numpy.nansum(net_export_solar) / sum_solar)
        self_consumption_battery = 1 - (numpy.nansum(net_import_solar_battery) / sum_solar)

    return {"self_consumption_solar": self_consumption_solar,
            "self_consumption_batteries": self_consumption_battery}


def self_sufficiency_self_consumption_average(self_consumption_self_sufficiency_dict: dict) -> dict:
    """
        calculates the average for self sufficiency and consumption over a given measurement.

        #TODO: consider inverters and how to avoid double counting with solar, batteries

        Args:
            self_consumption_self_sufficiency_dict: The dictionary has a node Id as Key and
                                                    4 values per node

        Returns:
            results_dict: dictionary with averages for the given network
    """

    self_sufficiency_solar = []
    self_sufficiency_batteries = []
    self_consumption_solar = []
    self_consumption_batteries = []

    for node in self_consumption_self_sufficiency_dict.values():
        self_sufficiency_solar.append(node["self_sufficiency_solar"])
        self_sufficiency_batteries.append(node["self_sufficiency_batteries"])
        self_consumption_solar.append(node["self_consumption_solar"])
        self_consumption_batteries.append(node["self_consumption_batteries"])

    av_self_sufficiency_solar = numpy.nanmean(self_sufficiency_solar)
    av_self_sufficiency_batteries = numpy.nanmean(self_sufficiency_batteries)
    av_self_consumption_solar = numpy.nanmean(self_consumption_solar)
    av_self_consumption_batteries = numpy.nanmean(self_consumption_batteries)

    return {"av_self_sufficiency_solar": av_self_sufficiency_solar,
            "av_self_sufficiency_batteries": av_self_sufficiency_batteries,
            "av_self_consumption_solar": av_self_consumption_solar,
            "av_self_consumption_batteries": av_self_consumption_batteries}


def self_sufficiency_self_consumption(meas_dict: dict, node_keys: list = None) -> dict:
    """
        Self-sufficiency = 1 - imports / consumption
        Self-consumption = 1 - exports / generation
        And average over those

        #TODO: consider inverters and how to avoid double counting with solar, batteries

        Args:
            meas_dict (dict): dict with measurement for one or multiple nodes.
            node_keys  (list): list of Node.names in Network.nodes.

        Returns:
            results_dict: self_sufficiency_solar, self_sufficiency_batteries,
                            self_consumption_solar, self_consumption_batteries
    """

    results_dict = {}

    nodes = node_keys if node_keys else meas_dict.keys()

    for key in nodes:
        if type(key) == int:
            key = key.tostr()
        if meas_dict[key]:
            load_p = pd.DataFrame([])
            solar_p = pd.DataFrame([])
            battery_p = pd.DataFrame([])
            for meas in meas_dict[key]:
                data_df = meas_dict[key][meas]
                if not data_df.empty:
                    if 'loads' in meas:
                        load_p = pd.concat([load_p, meas_dict[key][meas]["PLG"]])
                    elif 'solar' in meas:
                        solar_p = pd.concat([solar_p, meas_dict[key][meas]["PLG"]])
                    elif 'batteries' in meas:
                        battery_p = pd.concat([battery_p, meas_dict[key][meas]["PLG"]])

            self_sufficiency_dict = self_sufficiency(load_p, solar_p, battery_p)
            self_consumption_dict = self_consumption(load_p, solar_p, battery_p)

            results_dict[key] = self_sufficiency_dict.copy()
            results_dict[key].update(self_consumption_dict)

    averages_dict = self_sufficiency_self_consumption_average(results_dict)
    results_dict.update(averages_dict)

    return results_dict


def network_net_power(meas_dict: dict, node_keys: list = None) -> dict:
    """
    Calculate the net power (kW) of the network on the point of common coupling
    (ignoring network structure and losses etc).
    Import and Export are the net_load with all values set to zero, which are not matching.

    Note: net_load is calculated by using load, solar and batterie values for each node at each
    time. If you load already has solar factored into it, then you should not pass the solar data
    on as a separate column in your measurement dict

    #TODO: consider inverters and how to avoid double counting with solar, batteries

    Args:
        meas_dict (dict): dict with measurement for one or multiple nodes.
        node_keys  (): list of Node.names in Network.nodes.

    Returns:
         dictionary of net_load, net_import, net_export
    """

    nodes = node_keys if node_keys else meas_dict.keys()

    for key in nodes:
        if type(key) == int:
            key = key.tostr()
        if meas_dict[key]:
            load_p = pd.DataFrame([])
            solar_p = pd.DataFrame([])
            battery_p = pd.DataFrame([])
            for meas in meas_dict[key]:
                if 'loads' in meas:
                    load_p = pd.concat([load_p, meas_dict[key][meas]["PLG"]])
                elif 'solar' in meas:
                    solar_p = pd.concat([solar_p, meas_dict[key][meas]["PLG"]])
                elif 'batteries' in meas:
                    battery_p = pd.concat([battery_p, meas_dict[key][meas]["PLG"]])

    net_load = pd.DataFrame([])
    net_load = pd.concat((net_load, load_p, solar_p, battery_p), axis=1).sum(axis=1)

    #create an array that contains which entries are import and which are export
    mask_import = (net_load >= 0)
    mask_export = (net_load < 0)

    net_import = numpy.copy(net_load) * mask_import
    net_export = numpy.copy(net_load) * mask_export

    return {'net_load': net_load, 'net_import': net_import, 'net_export': net_export}


def solar_kwh_per_kw(meas_dict: dict, node_info: pd.DataFrame, node_keys: list = None) -> dict:
    """
    Calculates the amount of solar energy generated per kW of rated solar capacity for all
    given meters

    Args:
        meas_dict (dict): dict with measurement for one or multiple nodes.
        node_info (pd.DataFrame) : Data frame with additional information on each node
        node_keys (list): list of Node ID's in the network.

    Returns:
        results_dict(dict): rated capacity for all given meters
    """
    results_dict = {}
    hours_in_day = 24
    initiate = 0
    nu_nonzero_properties = 0

    nodes = node_keys if node_keys else meas_dict.keys()

    for key in nodes:
        if type(key) == int:
            key = key.tostr()
        if meas_dict[key]:
            solar_power = pd.DataFrame([])
            solar_capacity = 0

            for meas in meas_dict[key]:
                if 'solar' in meas:
                    solar_power = pd.concat([solar_power, meas_dict[key][meas]["PLG"]])

                    #calculates the overall solar power capacity
                    node_data = node_info[node_info.index == int(key)]
                    solar_capacity += node_data["system_max_p"].get(int(key))

            # summing up all the individual solar powers
            sum_power = solar_power.sum(axis=1)

            if solar_capacity != 0:
                sum_power /= solar_capacity
                mega_df = sum_power if initiate == 0 else mega_df.append(sum_power)
                initiate = 1
                results_dict[key] = -numpy.nanmean(sum_power)*hours_in_day
                nu_nonzero_properties += 1

    kwh_per_kw_true_average = numpy.nanmean(mega_df)*hours_in_day
    results_dict["average"] = -kwh_per_kw_true_average
    return results_dict
