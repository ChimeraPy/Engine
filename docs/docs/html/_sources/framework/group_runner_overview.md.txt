# Group Runner Overview

In constrast to the ``SingleRunner``, the ``GroupRunner`` is focuses in
use cases where there are multiple objects/persons of interest, with
their own sensors and data streams. The ``GroupRunner`` provides the
capability of running multiple individual pipelines and then a single 
group pipeline. This can be seen in the illustration below.

![GroupRunner Overview](../_static/group_runner_overview.drawio.png)

Note: All the boxes in different colors are their own process which are
connected by data and messasing queues to the ``GroupRunner`` (the main
process that spawns the ``Loader`` and the ``Logger``).

## Loader

In the ``GroupRunner`` use case, the ``Loader`` and ``Collector`` have
the more challenging task of loading, syncing, and organizing data from
multiple data streams for multiple objects/persons of interest. A 
2 level dictionary is used to organize the time window data from each
data stream for each object/person of interest. Here is an example 
dictionary:

```python
data_samples = {
    'root': {
        'group_camera': pd.DataFrame({}),
        'group_sensor': pd.DataFrame({}),
    },
    'P01': { # participant #01
        'egocentric_video': pd.DataFrame({}),
        'text': pd.DataFrame({})
    },
    'P02': { # participant #02
        'egocentric_video': pd.DataFrame({}),
        'text': pd.DataFrame({})
}

```

The data loaded by the ``Loader`` is then placed in the loading queue, 
waiting for the ``GroupRunner`` to later forward propagate through the
pipeline.

## Individual Pipeline + Group Pipeline

Similar to ``SingleRunner`` use case, the data is forward propagated 
through the individual pipelines. In constrast, the output of the 
individual pipelines are then passed as input for the group pipeline 
to allow analysis between objects/persons of interest (like collaboration).

### Processes

The user-defined processes are modular compartments that store the logic
intended to perform an operation on the data streams. NOTE: currently 
processes are run purely serially, there is current development to 
parallelize these process to improve time effiency.

## Logging

Logging data in the ``GroupRunner`` use case is more complicated. The
logged data (as defined in the pipelines) is saved and organized by
separating the each pipeline's data into individual folders. The group
pipeline's logged data is saved in the root directory of the logging
folder.

