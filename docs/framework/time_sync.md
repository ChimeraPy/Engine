# Time Synchronization

For ChimeraPy to load data from multiple sources and align them 
temporally, we require to create a simple data structure: a global 
timetrack. This structure is constructed by ChimeraPy during runtime 
through an efficient use of Pandas ``DataFrame``. Note: ChimeraPy uses
``pandas.Timedelta`` to track time and entries in the data streams, 
this is done to avoid ambiguity with time unit (seconds, milliseconds, 
etc.).

## Data Structure (Global Timetrack)

The ``Collector`` performs the operation of constructing the global
timetrack by *collecting* all the data streams, extracting their
individual timelines, and sorting them by time. The global timetrack 
does **not** include the data itself from the all the data streams, but 
instead a counter referring to the index of the data steram at each timestamp.
Below is an example global timetrack:

| time                   | ds_index | group | ds_type      |
|------------------------|----------|-------|--------------|
| 0 days 00:00:00        | 0        | root  | test_tabular |
| 0 days 00:00:00        | 0        | root  | test_video   |
| 0 days 00:00:00:0.0333 | 1        | root  | test_video   |

To improve the efficiency when loading data, batch loading of data from
a *start* and *end* time window is performed. 

## Time Windows

Once the global timetrack is constructed, it is partitioned into time 
windows. 

![Global Timetrack & Time Windows](../_static/global_timetrack.drawio.png)

The data is then loaded by following this batch scheme and it is 
propagated through the pipeline in the same structure.

