c3x data
--------------------

Tools for analysing typical data relevant to investigations of Battery, Storage and Grid Integration.

Structure
--------------------

This package (currently) contains two modules:
1. loaders, whose functions read in data sets and structure them into data structure.
2. statistics, whose functions read data sets and analyse the quality of data
3. cleaners, whose functions read data sets and clean it according user configurations 
(e.G configuration file)

Conventions
--------------------

Incoming data sets have a variety of sign and unit conventions, the statistics can be used to 
analyse the data. Cleaners can be used to remove weaknesses in the data according to the users 
configuration. For later analysis the data **should always be cleaned and units adjusted**.

Each object may have any of the following qualities with the given units. 
The sign convention is that **power flows into loads are positive and generation is negative**. 

|Metric        |Variable|Unit |Notes                                                                                 |
|--------------|---------|:---:|:-------------------------------------------------------------------------------------|
|power         |p       |kW   |Grid import(export), battery charging(discharging) is +ve(-ve), solar generate is -ve |
|reactivePower |q       |kVA  |Grid import(export), battery charging(discharging) is +ve(-ve), solar generate is -ve |
|frequency     |f       |Hz   |                                                                                      |
|voltage       |v       |V    |                                                                                      |
|charge        |c       |kWh  |Always positive                                                                       |


Data Structure 
----------------

This Data Loader is responsible for loading and managing next gens data sets provided by Reposit
The data provided can be read and converted into measurement data. Data is read and stored as 
an intermediate step in a folder "Data", each measurement type has its own sub folder 
(loads, batteries, node, solar) and is aggregated per node. To handle huge amounts of
data, it is read in batches (see scripts/config for example). 
Each file is labeled with the type, node id and batch number (measurement_type_nodeID_batchnumber). 
Batches can be concatenated and stored per node.

Tariff loader
--------------

The tariff loader loads a tariff into a dataframe structure. A tariff structure is build for each 
hour of the year. If there is insufficient data to fill in missing data points the will be set to
0. The tariff can be mapped to timestamps matching the measurement data.

Cleaning
---------

A data set can be cleaned from faulty data (see scripts/config for example), which includes, 
duplicates, NaN (via refill with other data or "simple") and removing unwanted time stamps.
The refill function can be used tp replace chunks of missing data. It looks for a better suited 
data block based on users specification (see scripts/config for example)

   




