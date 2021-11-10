<!-- markdownlint-disable -->

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/data_sample.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `data_sample`
Module focused on ``DataSample`` implementation. 

Contains the following classes:  ``DataSample`` 



---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/data_sample.py#L14"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `DataSample`
Standard output data type for Data Streams. 



**Attributes:**
 
 - <b>`dtype`</b> (str):  Typically the name of the parent DataStream. 


 - <b>`data`</b> (Any):  The data content of the sample. 


 - <b>`time`</b> (pd.Timestamp):  the timestamp associated with the sample. 

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/data_sample.py#L26"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(dtype: str, time: Timestamp, data) → None
```

Construct the ``DataSample``. 



**Args:**
 
 - <b>`dtype`</b> (str):  Typically the name of the parent DataStream. 


 - <b>`data`</b> (Any):  The data content of the sample. 


 - <b>`time`</b> (pd.Timestamp):  the timestamp associated with the sample. 







---

_This file was automatically generated via [lazydocs](https://github.com/ml-tooling/lazydocs)._
