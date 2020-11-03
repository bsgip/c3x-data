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

customer = next(iter(cleaned_data))

# calculate the self sufficiency and consumption over all nodes in this data set
self_suf_con = figure_of_merit.self_sufficiency_self_consumption(cleaned_data, node_keys=[customer])

print("property ID: ", customer)
sub_dict = self_suf_con[customer]
print('self_sufficiency_solar', numpy.round(100*sub_dict["self_sufficiency_solar"], decimals=2), '%')
print('self_sufficiency_batteries', numpy.round(100*sub_dict["self_sufficiency_batteries"], decimals=2), '%')
print('self_consumption_solar', numpy.round(100*sub_dict["self_consumption_solar"], decimals=2), '%')
print('self_consumption_batteries', numpy.round(100*sub_dict["self_consumption_batteries"], decimals=2), '%')

data_customer = cleaned_data[customer]
c_load = data_customer["loads_" + customer]
solar = data_customer["solar_" + customer]
battery = data_customer["battery_" + customer]
net_load = c_load["PLG"] + battery["PLG"] + solar["PLG"]

colors = sns.color_palette()
hrs = numpy.arange(0, len(c_load["PLG"])) / 4
fig = plt.figure(figsize=(14, 4))
ax1 = fig.add_subplot(1, 1, 1)
l1, = ax1.plot(hrs, 4 * c_load["PLG"], color=colors[0])
l2, = ax1.plot(hrs, 4 * solar, color=colors[1])
l3, = ax1.plot(hrs, 4 * battery["PLG"], color=colors[2])
l4, = ax1.plot(hrs, 4 * net_load, color=colors[3])
ax1.set_xlabel('hour'), ax1.set_ylabel('kW')
ax1.legend([l1, l2, l3, l4], ['Load', 'PV', 'Battery', "net"], ncol=2)
ax1.set_xlim([0, len(c_load["PLG"]) / 4])
fig.tight_layout()


plt.show()


