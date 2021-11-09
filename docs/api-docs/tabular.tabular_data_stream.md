<!-- markdownlint-disable -->

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/tabular/tabular_data_stream.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `tabular.tabular_data_stream`
Module focused on Tabular Data Stream implementation. 

Contains the following classes:  ``OfflineTabularDataStream`` 



---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/tabular/tabular_data_stream.py#L21"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `OfflineTabularDataStream`
Implementation of Offline DataStream focused on Tabular data. 



**Args:**
 
 - <b>`name`</b> (str):  The name of the data stream. 


 - <b>`data`</b> (pd.DataFrame):  The loaded Tabular data in pd.DataFrame form. 


 - <b>`time_column`</b> (str):  The column within the data that has the  time data. 


 - <b>`data_columns`</b> (List[str]):  A list of string containing the name of the data columns to select from. 



**Attributes:**
 
 - <b>`name`</b> (str):  The name of the data stream. 


 - <b>`data`</b> (pd.DataFrame):  The loaded Tabular data in pd.DataFrame form. 


 - <b>`data_columns`</b> (List[str]):  A list of string containing the name of the data columns to select from. 

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/tabular/tabular_data_stream.py#L44"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(name: str, data: DataFrame, time_column: str, data_columns: List[str])
```








---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/tabular/tabular_data_stream.py#L64"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>classmethod</kbd> `from_process_and_ds`

```python
from_process_and_ds(
    process: Process,
    in_ds: OfflineDataStream,
    verbose: bool = False
)
```

Class method to construct data stream from an applied process to a data stream. 



**Args:**
 
 - <b>`process`</b> (Process):  the applied process 
 - <b>`in_ds`</b> (OfflineTabularDataStream):  the incoming data stream to be processed 



**Returns:**
 
 - <b>`self`</b> (OfflineTabularDataStream):  the generated data stream 




---

_This file was automatically generated via [lazydocs](https://github.com/ml-tooling/lazydocs)._
