# Single Runner Overview

As a refresher, a ``SingleRunner`` is intended to be used when collecting
and analyzing data from a single object/person of interest. Given that,
let's look into how the ``SingleRunner`` class operates. Below is an
illustration in the logic and data flow in the system.

![SingleRunner Overview](../_static/single_runner_overview.drawio.png)

Note: All the boxes in different colors are their own process which are
connected by data and messasing queues to the ``SingleRunner`` (the main
process that spawns the ``Loader`` and the ``Logger``).

## Loader

The ``Loader`` and its attribute ``Collector`` load and sync the data
that will be processed further down the process. In the ``SingleRunner``
case, all the data streams pertain to a single pipeline and are 
therefore stored together in a dictionary. Here is an example dictionary:

```python
data_samples = {
    'video': pd.DataFrame({}),
    'gaze': pd.DataFrame({}),
    'text': pd.DataFrame({})
}
```

The data loaded by the ``Loader`` is then placed in the loading queue, 
waiting for the ``SingleRunner`` to later forward propagate through the
pipeline.

## Individual Pipeline

The generated dictionary by the ``Loader`` is then forward propagated 
through the individual pipeline (that was passed to the ``SingleRunner`` 
during its construction). The pipeline is defined by the user to handle
their own special use case. Through the use of multiple modularized 
``Process``, the desired output of the pipeline can be achieved.

### Processes

The user-defined processes are modular compartments that store the logic
intended to perform an operation on the data streams. NOTE: currently 
processes are run purely serially, there is current development to 
parallelize these process to improve time effiency.

## Logging

The ``Logger``, in the ``SingleRunner``, just logs all data (requested
by the ``Pipeline``) to a single directory for the ``SingleRunner``.
