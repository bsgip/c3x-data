"""
 This examples shows how cleaned data can be read for further use
"""


import numpy
import matplotlib.pyplot as plt
import seaborn as sns


# BSGIP specific tools
from c3x.data_loaders import configfileparser, nextgen_loaders, tariff_loaders
from c3x.data_statistics import figure_of_merit
from c3x.enomo.models import EnergyStorage, EnergySystem, Demand, Generation, LocalTariff
from c3x.enomo.energy_optimiser import OptimiserObjectiveSet, LocalEnergyOptimiser

# set up seaborn the way you like
sns.set_style({'axes.linewidth': 1, 'axes.edgecolor': 'black', 'xtick.direction': \
    'out', 'xtick.major.size': 4.0, 'ytick.direction': 'out', 'ytick.major.size': 4.0, \
               'axes.facecolor': 'white', 'grid.color': '.8', 'grid.linestyle': u'-', 'grid.linewidth': 0.5})

config = configfileparser.ConfigFileParser("config/example_for_FoMs.ini")

measurement_types = config.read_data_usage()
data_paths = config.read_data_path()
data_files = []

# Create a nextGen data object that has working paths and can be sliced using batches
next_gen = nextgen_loaders.NextGenData('FoM', source=data_paths['source'],
                                       batteries=data_paths["batteries"],
                                       solar=data_paths["solar"],
                                       loads=data_paths["loads"],
                                       node=data_paths["node"])

cleaned_data = next_gen.to_measurement_data()

# Tariffs are in $ / kwh
data_location = '../tests/tariff_database/'
local_tz = 'Australia/Sydney'

tariff_dict = {}
for node in cleaned_data:
    load = cleaned_data[node]["loads_" + str(node)]
    tariff_info = {}
    tariff_info['import_tariff'] = tariff_loaders.link_tariffs(load, data_location, 'test_tou_tariff.json')
    tariff_info['export_tariff'] = tariff_loaders.link_tariffs(load, data_location, 'test_feed_in_tariff.json')
    tariff_info['le_export_tariff'] = tariff_loaders.link_tariffs(load, data_location, 'test_le_export_tariff.json')
    tariff_info['le_import_tariff'] = tariff_loaders.link_tariffs(load, data_location, 'test_le_import_tariff.json')
    tariff_info['lt_export_tariff'] = tariff_loaders.link_tariffs(load, data_location, 'test_lt_export_tariff.json')
    tariff_info['lt_import_tariff'] = tariff_loaders.link_tariffs(load, data_location, 'test_lt_import_tariff.json')
    tariff_info['re_export_tariff'] = tariff_loaders.link_tariffs(load, data_location, 'test_re_export_tariff.json')
    tariff_info['re_import_tariff'] = tariff_loaders.link_tariffs(load, data_location, 'test_re_import_tariff.json')
    tariff_info['rt_export_tariff'] = tariff_loaders.link_tariffs(load, data_location, 'test_rt_export_tariff.json')
    tariff_info['rt_import_tariff'] = tariff_loaders.link_tariffs(load, data_location, 'test_rt_import_tariff.json')
    tariff_dict[node] = tariff_info

customer = next(iter(cleaned_data))
data_customer = cleaned_data[customer]
c_load = data_customer["loads_" + customer]
solar = data_customer["solar_" + customer]
net_load = c_load["PLG"] + solar["PLG"]

colors = sns.color_palette()
hrs = numpy.arange(0, len(c_load["PLG"])) / 4
fig = plt.figure(figsize=(14, 4))
ax1 = fig.add_subplot(1, 1, 1)
l1, = ax1.plot(hrs, 4 * c_load["PLG"], color=colors[0])
l2, = ax1.plot(hrs, 4 * solar, color=colors[1])
l3, = ax1.plot(hrs, 4 * net_load, color=colors[2])
ax1.set_xlabel('hour'), ax1.set_ylabel('kW')
ax1.legend([l1, l2, l3], ['Load', 'PV', 'Connection Point'], ncol=2)
ax1.set_xlim([0, len(c_load["PLG"]) / 4])
fig.tight_layout()

fig.show()

# ENOMO optimisation battery
battery = EnergyStorage(max_capacity=15.0,
                        depth_of_discharge_limit=0,
                        charging_power_limit=5.0,
                        discharging_power_limit=-5.0,
                        charging_efficiency=1,
                        discharging_efficiency=1,
                        throughput_cost=0.018)

net_load = figure_of_merit.network_net_power(cleaned_data, node_keys=[customer])
import_net_load = net_load["net_import"]
export_net_load = net_load["net_export"]

# add the energy system e.G. Battery
# demand will reject negative values
energy_system = EnergySystem()
energy_system.add_energy_storage(battery)

# add a demand for the system e.G. imported energy
load = Demand()
load.add_demand_profile(import_net_load)

# add a generation for the system e.G. exported energy
# generation will reject positive values
pv = Generation()
pv.add_generation_profile(export_net_load)

# add local tariff data for cost
local_tariff = LocalTariff()
local_tariff.add_local_energy_tariff_profile_export(dict(enumerate(tariff_info['le_export_tariff'])))
local_tariff.add_local_energy_tariff_profile_import(dict(enumerate(tariff_info['le_import_tariff'])))
local_tariff.add_local_transport_tariff_profile_export(dict(enumerate(tariff_info['lt_export_tariff'])))
local_tariff.add_local_transport_tariff_profile_import(dict(enumerate(tariff_info['lt_import_tariff'])))
local_tariff.add_remote_energy_tariff_profile_export(dict(enumerate(tariff_info['re_export_tariff'])))
local_tariff.add_remote_energy_tariff_profile_import(dict(enumerate(tariff_info['re_import_tariff'])))
local_tariff.add_remote_transport_tariff_profile_export(dict(enumerate(tariff_info['rt_export_tariff'])))
local_tariff.add_remote_transport_tariff_profile_import(dict(enumerate(tariff_info['rt_import_tariff'])))

# add the demand and generation profiles to the energy system
energy_system.add_demand(load)
energy_system.add_generation(pv)
energy_system.add_local_tariff(local_tariff)

# Invoke the optimiser and optimise
local_energy_models = True
optimiser = LocalEnergyOptimiser(15, 324, energy_system, OptimiserObjectiveSet.LocalModelsThirdParty + OptimiserObjectiveSet.LocalPeakOptimisation)

############################ Analyse the Optimisation ########################################
storage_energy_delta = optimiser.values('storage_charge_grid') +\
                       optimiser.values('storage_charge_generation') +\
                       optimiser.values('storage_discharge_demand') +\
                       optimiser.values('storage_discharge_grid')

colors = sns.color_palette()
hrs = numpy.arange(0, len(c_load["PLG"])) / 4
fig = plt.figure(figsize=(14, 7))
ax1 = fig.add_subplot(2, 1, 1)
l1, = ax1.plot(hrs, 4 * c_load["PLG"], color=colors[0])
l2, = ax1.plot(hrs, 4 * solar, color=colors[1])
l4, = ax1.plot(hrs, 4 * storage_energy_delta, color=colors[3])
ax1.set_xlabel('hour'), ax1.set_ylabel('kW')
ax1.legend([l1, l2, l4], ['Load', 'PV', 'Storage'], ncol=3)
ax1.set_xlim([0, len(c_load["PLG"]) / 4])
ax3 = fig.add_subplot(2, 1, 2)
l1, = ax3.plot(hrs, storage_energy_delta * 4, color=colors[5])
l2, = ax3.plot(hrs, optimiser.values('storage_state_of_charge'), color=colors[4])
ax3.set_xlabel('hour'), ax3.set_ylabel('action')
ax3.legend([l1, l2], ['battery action (kW)', 'SOC (kWh)'], ncol=2)
ax3.set_xlim([0, len(c_load["PLG"]) / 4])
fig.tight_layout()
plt.show()

net_grid_flow = 4 * optimiser.values('storage_charge_grid') + 4 * optimiser.values('storage_discharge_grid') + 4 * optimiser.values('local_net_import') + 4 * optimiser.values('local_net_export')

fig = plt.figure(figsize=(14, 7))
ax11 = fig.add_subplot(2, 1, 1)
l1, = ax11.plot(hrs, 4 * net_load["net_load"], color=colors[0])
l2, = ax11.plot(hrs, 4 * optimiser.values('storage_charge_grid'), color=colors[1])
l3, = ax11.plot(hrs, 4 * optimiser.values('storage_charge_generation'), color=colors[2])
l4, = ax11.plot(hrs, 4 * optimiser.values('storage_discharge_demand'), color=colors[3])
l5, = ax11.plot(hrs, 4 * optimiser.values('storage_discharge_grid'), color=colors[4])
l6, = ax11.plot(hrs, 4 * optimiser.values('local_net_import'), color=colors[5])
l7, = ax11.plot(hrs, 4 * optimiser.values('local_net_export'), color=colors[6])
l8, = ax11.plot(hrs, 4 * optimiser.values('local_demand_transfer'), color=colors[8])
ax11.set_xlabel('hour'), ax1.set_ylabel('kW')
ax11.legend([l1, l2, l3, l4, l5, l6, l7, l8], ['Net', 'storage_charge_grid', 'storage_charge_generation', 'storage_discharge_load', 'storage_discharge_grid', 'Net Customer Import', 'Net Customer Export', 'Local Transfer'], ncol=3)
ax11.set_xlim([0, len(c_load["PLG"]) / 4])
ax33 = fig.add_subplot(2, 1, 2)
l33, = ax33.plot(hrs, net_grid_flow, color=colors[0])
ax33.set_xlabel('hour'), ax3.set_ylabel('kW')
ax33.legend([l33], ['Net Grid Flows'], ncol=2)
ax33.set_xlim([0, len(c_load["PLG"]) / 4])
plt.show()
