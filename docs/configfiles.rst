==========================
Using c3x-data configfiles
==========================

Cleaning process
----------------

c3x data implements a set of function useful for cleaning measurement data in the context of solar and battery systems. The following will describe a process to clean up raw Nextgen data for different use cases. The cleaning processes used can be specified in a config file (examples can be found here).
The cleaning process is using two different stages to make it efficient in terms of memory usage and time consumption.
1. reading and sorting data into batches
2. Filtering data
3. resampling, forcing full index and refilling

Reading and sorting data
------------------------

Batches
^^^^^^^

Batches can be use to reduce the raw data to a more interesting subset, without specifying a particular time range. A statistical analysis can be conducted before this step to find a suitable batch with data. e.g batch with fewest amount of duplicates, missing data or with the most nodes.
Using batches also helps to prevent out of memory exceptions, as a batch may only contain a certain amount of files at a time. In case of the Nextgen data we are dealing with roughly 26000 raw files totalling to 9GB of data

Here are a few examples for batching:

.. code-block:: ini

    # read all available data and create measurement files using all data
    [Batches]
    number_of_batches = 26
    files_per_batch = 1000
    concat_batches_start = 0
    concat_batches_end = 25

    # read all available data and create measurement but use only a subset for a nodes measurement file
    [Batches]
    number_of_batches = 26
    files_per_batch = 1000
    concat_batches_start = 10
    concat_batches_end = 15

    # reading all available data but on a per file bases (here 1h of data at a time) -> good for statistics
    [Batches]
    number_of_batches = 26364
    files_per_batch = 1
    concat_batches_start = 0
    concat_batches_end = 26364

Data Path
^^^^^^^^^

This section defines where data is going to be stored for intermediate steps of cleaning (e.g batches, measurement subsets) and which data is supposed to be read (source). These paths have to be adjusted to the specific computer that runs the cleaning process. It is possible to add new paths by creating a key in the same way, as the others. A dictionary is created that has the variable name (e.G. results) as key and the path as value.

Note: node data is not available for the time being:

.. code-block:: ini

    [Data Path]
    source = /home/arena/raw_next_gen_data/
    batteries = /home/arena/ARENA-Clean/batteries
    solar = /home/arena/ARENA-Clean/solar
    node = /home/arena/ARENA-Clean/node
    loads = /home/arena/ARENA-Clean/loads
    results = /home/arena/ARENA-Clean/results

Data usage
^^^^^^^^^^

In many cases only particular data is needed. this section allows for filtering in regards to the data needed for later use.

.. code-block:: ini

    # only use solar and load data, battery is not required
    [Data Usage]
    batterieThis can bes = False
    solar = True
    node = False
    loads = True

Filtering
---------

Filtering describes a process of removing certain data from the data set. It is in the interest of the user to remove big chunks of unwanted data early in the process as it might increase speed (e.g. use time filter before other cleaning steps). However if there is an attempt to refill data its best to work on the full data set, so that more suitable data can be found.

Time Filter
^^^^^^^^^^^

The timefilter can be use to reduce the dataset per node to a specific time frame. Only the data in between the start and end will be kept. Reducing the amount of data drastically. The date used needs to match the times given in the measurement data and has to be provide in a format of YYYY-M-d hh:mm

.. code-block:: ini

    [Time Filter]
    time_filter_use = True
    start_time = 2018-1-1 00:00
    end_time = 2018-1-31 23:55

Sign Correction, Duplicate removal and NaN handling
---------------------------------------------------

Faulty data in solar and load often have wrong sign as an indication of fault. Sometimes duplicated entries can be found or NaN's for missing data. the removal of these is handled consistently using the same set of configurations.
It is important to understand, that the goal for removing those values may differ and the timing of when the removal is done can impact the result of the cleaned data.

Settings
^^^^^^^^

Data replacement:  describes how an error should be handled. One of the following data replacements methods must be used:

1. first: Uses the first (duplicate) or the value before fault that is not NaN
2. last : Uses the last (duplicate) or the value after fault that is not NaN
3. drop (only for NaN): faults are dropped (removed from data, this can lead to inconsitent timestamps)
4. zero (only for NaN): replaces fault with 0
5. nan (not for NaN) : replaces faults with Nan
6. none: Nothing is changed. In case of dublicates none are kept
7. remove: Removes the date to users specifications.

It may be of interest to remove more data then the actual faulty data point. A hole day (e.G 24h not calendar date) the hole data_set or only some hours. One of the following removal_time_frames must be chosen:

1. day: 24 hour of data will be removed.
2. hour: 1 hour of data will be removed.
3. all: All data will be removed.

The time range determine the position of the data point in the middle, at the end or at the start of the data. One of the following fault placements are possible:

start: Fault is places at the beginning of the data that is removed (eg. 1 hour after the fault is removed)
middle: Fault is places in the middle of the data that is removed (eg. 30 minutes before and after the fault is removed)
end: Fault is places at the end of the data that is removed (eg. 1 hour before the fault is removed)

.. code-block:: ini

    # removes a full day of date before the fault
    [Duplicate Removal]
    duplicate_removal = True
    data_replacement = remove
    removal_time_frame = day
    fault_placement = end

    # drops individual NaN values (may cause missing timestamps)
    [Nan handling]
    nan_removal = True
    data_replacement = drop

    # these are ignored if data_replacement is not "remove"
    removal_time_frame = day
    fault_placement = end

    # removes 1h of data with the fault at the end
    [Sign Correction]
    wrong_sign_removal = True
    data_replacement = remove
    removal_time_frame = hour
    fault_placement = end

Resampling, forcing full index and refilling
--------------------------------------------

Resampling and forcing a full index and refilling may cause NaN's in the data. It is best to use the handle NaN at the very end of a script to make sure all NaN's are removed in a consistent way.

resampling
^^^^^^^^^^

The data can be reduced or extended to match a certain frequency. The Next Gen data has a sampling rate of 5 min (data point every 5 minutes). However it can be useful to change the frequency,

.. code-block:: ini

    #resamples the data to 5 min steps using the first data point to refill the gab
    [Resample]
    resampling = True
    resampling_step = 5
    resampling_unit = min
    resampling_strategy_upsampling = first

forcing full index
^^^^^^^^^^^^^^^^^^

This step uses information provided in "resampling" (for the right timestep) and "time filter" to create a dataframe with the appropriate indexes for the optimisation. This should be used before refilling data. it ensures that all measurement data have the same rate and length

refill
^^^^^^

Refilling data is not always possible. However the function makes an attempt to do so by finding a Block of NaN in a certain time surrounding the missing block that has less NaN's then the original data. The function can be used multiple times, to ensure a good result. However the replacement data may then consist of data from multiple other sections instead of one. be careful about seasons!

1. days: specify the amount of days that shall be moved forward for each attempt
2. attempts: number of times a jump is made
3. threshold: MINIMUM number of NaN's that form a block to be replaced.


example: A block of 5 NaNs was found at 8:00 am till 8:30 of the 5th of January. If days is defined as 7 and attempts as 2 a block for replacement is considers from the 13th of January and the 20th of January, both for the time from 8:00 till 8:30. The block used for replacement out of ALL POSSIBLE OPTIONS is the one with the smallest amount of NaNs.

.. code-block:: ini

    # finds blocks of a minimum of 5 NaNs and attempts to replace it with data from 7 days and 14 days into the future
    [Refill]
    data_refill = True
    days = 7
    attempts = 2
    threshold = 5
    forward_fill = True
    backward_fill = False
