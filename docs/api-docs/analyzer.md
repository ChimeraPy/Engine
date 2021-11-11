<!-- markdownlint-disable -->

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/analyzer.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `analyzer`
Module focused on the ``Analyzer`` implementation. 

Contains the following classes:  ``Analyzer`` 



---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/analyzer.py#L26"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `Analyzer`
Multimodal Data Processing and Analysis Coordinator.  

The Analyzer is the coordinator between the other modules. First, the analyzer determines the flow of data from the data streams stored in the collector and the given processes. Once the analyzer constructs the data flow graph, the pipelines for the input  data stream are determined and stored. 

Then, when feeding new data samples, the analyzer send them to the right pipeline and its processes. The original data sample and its subsequent generated data sample in the pipeline are stored in the Session as a means to keep track of the latest sample. 



**Attributes:**
 
 - <b>`collector`</b> (pymddt.Collector):  The collector used to match the  timetracks of each individual data stream. 


 - <b>`processes`</b> (Sequence[pymddt.Process]):  A list of processes to be executed depending on their inputs and triggers. 


 - <b>`session`</b> (pymddt.Session):  The session that stores all of the latest data samples from original and generated data streams. 


 - <b>`data_flow_graph`</b> (nx.DiGraph):  The data flow constructed from the data streams and the processes. 


 - <b>`pipeline_lookup`</b> (Dict):  A dictionary that stores the pipelines for each type of input data stream. 



**Todo:**
 * Make the session have the option to save intermediate  data sample during the analysis, if the user request this feature. 

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/analyzer.py#L61"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    collector: Union[Collector, OfflineCollector],
    processes: Sequence[Process],
    session: Session
) → None
```

Construct the analyzer.  



**Args:**
 
 - <b>`collector`</b> (pymddt.Collector):  The collector used to match the  timetracks of each individual data stream. 


 - <b>`processes`</b> (Sequence[pymddt.Process]):  A list of processes to be executed depending on their inputs and triggers. 


 - <b>`session`</b> (pymddt.Session):  The session that stores all of the latest data samples from original and generated data streams. 




---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/analyzer.py#L166"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `close`

```python
close() → None
```

Close routine that executes all other's closing routines. 

The ``Analyzer``'s closing routine closes all the ``Process``, ``Collector``, and ``DataStream`` by executing ``.close()``  methods in each respective component. 

---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/analyzer.py#L129"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `get_sample_pipeline`

```python
get_sample_pipeline(sample: DataSample) → Sequence[Process]
```

Get the processes that are dependent to this type of data sample. 



**Args:**
 
 - <b>`sample`</b> (pymddt.DataSample):  The data sample that contains the  data type used to select the data pipeline. 

---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/analyzer.py#L139"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `step`

```python
step(sample: DataSample) → None
```

Routine executed to process a data sample. 

Given a ``DataSample`` from one of the ``DataStream``s in the  ``Collector``, the ``Analyzer`` propagates the data sample through its respective data pipeline and saves intermediate  and final data samples into its ``Session`` attribute. 



**Args:**
 
 - <b>`sample`</b> (pymddt.DataSample):  The new input data sample to will be propagated though its corresponding pipeline and stored. 




---

_This file was automatically generated via [lazydocs](https://github.com/ml-tooling/lazydocs)._
