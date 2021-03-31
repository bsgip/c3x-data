"""
 This examples shows how cleaned data can be read for further use
"""

import os
import pandas
import numpy
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import datetime

# BSGIP specific tools
from c3x.data_loaders import configfileparser, nextgen_loaders, tariff_loaders
from c3x.data_cleaning import cleaners
from c3x.data_statistics import figure_of_merit


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

##################### Figures of Merit #####################
property_id = next(iter(cleaned_data))
print("One Day of data exported for Property: ", property_id)

phase = cleaned_data[property_id]

load = phase["loads_"+str(property_id)]
solar = phase["solar_" + str(property_id)]
battery = phase["battery_" + str(property_id)]

load_reduced = cleaners.time_filter_data(load, "2019-01-23 00:05","2019-01-23 13:00")
solar_reduced = cleaners.time_filter_data(solar, "2019-01-23 00:05","2019-01-23 13:00")
battery_reduced = cleaners.time_filter_data(battery, "2019-01-23 00:05","2019-01-23 13:00")

plot_time = pandas.to_datetime(load_reduced.index, unit='s').tz_localize('GMT')
plt.xticks(rotation=45, fontsize=4)

colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k', 'w']

plt.plot(plot_time, load_reduced.iloc[:, 0], label='Demand', color=colors[0])
plt.plot(plot_time, solar_reduced.iloc[:, 0], label='Solar', color=colors[4], linestyle=':')
plt.plot(plot_time, battery_reduced.iloc[:, 0], label='Battery', color=colors[2], linestyle='--')

plt.legend(numpoints=1)
plt.xlabel('Hour of day - h')

plt.grid()
plt.savefig('One_day.pdf')

# calculate the self sufficiency and consumption over all nodes in this data set
self_suf_con = figure_of_merit.self_sufficiency_self_consumption(cleaned_data)

for key in self_suf_con.keys():
    if "av" not in key:
        print("property ID: ", key)
        sub_dict = self_suf_con[key]
        print('self_sufficiency_solar', numpy.round(100*sub_dict["self_sufficiency_solar"], decimals=2), '%')
        print('self_sufficiency_batteries', numpy.round(100*sub_dict["self_sufficiency_batteries"], decimals=2), '%')
        print('self_consumption_solar', numpy.round(100*sub_dict["self_consumption_solar"], decimals=2), '%')
        print('self_consumption_batteries', numpy.round(100*sub_dict["self_consumption_batteries"], decimals=2), '%')

print("Average: ", )
print('av_self_sufficiency_solar', numpy.round(100*self_suf_con["av_self_sufficiency_solar"], decimals=2), '%')
print('av_self_sufficiency_batteries', numpy.round(100*self_suf_con["av_self_sufficiency_batteries"], decimals=2), '%')
print('av_self_consumption_solar', numpy.round(100*self_suf_con["av_self_consumption_solar"], decimals=2), '%')
print('av_self_consumption_batteries', numpy.round(100*self_suf_con["av_self_consumption_batteries"], decimals=2), '%')

node_info = pandas.read_pickle(data_paths["node"] + "/node_info.npy")
solar_result = figure_of_merit.solar_kwh_per_kw(cleaned_data, node_info)

print("Average solar performance ", numpy.round(solar_result["average"], decimals=2), 'kWh/kW')

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

FoM_financial_raw = figure_of_merit.customer_financial(cleaned_data, node_keys=None, tariff=tariff_dict)

print('------ Measured data ------')
print("Average cost per connection point $", numpy.round(FoM_financial_raw["average"], decimals=2))
print("Note this cost doesn't account for missing data")

FoM_peak_powers = figure_of_merit.peak_powers(cleaned_data)  # , node_keys=['node_1'])
print('Peak power imported into network', numpy.round(FoM_peak_powers["peak_power_import"], 4), 'kW')
print('Peak power exported from network', numpy.round(FoM_peak_powers["peak_power_export"], 4), 'kW')
