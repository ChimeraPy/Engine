<!-- markdownlint-disable -->

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/collector.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `collector`
Module focused on the ``Collector`` and its various implementations. 

Contains the following classes:  ``Collector``  ``OfflineCollector``  ``OnlineCollector`` 



---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/collector.py#L27"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `Collector`
Generic collector that stores a data streams. 



**Attributes:**
 
 - <b>`data_streams`</b> (Dict[str, mm.DataStream]):  A dictionary of the  data streams that its keys are the name of the data streams. 

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/collector.py#L35"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(data_streams: List[Union[DataStream, OfflineDataStream]]) → None
```

Constructor for ``OfflineCollector``. 



**Args:**
 
 - <b>`data_streams`</b> (List[mm.DataStream]):  A list of data streams. 





---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/collector.py#L44"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `OfflineCollector`
Generic collector that stores only offline data streams. 

The offline collector allows the use of both __getitem__ and __next__ to obtain the data pointer to a data stream to fetch the actual data. 



**Attributes:**
 
 - <b>`data_streams`</b> (Dict[str, mm.OfflineDataStream]):  A dictionary of the  data streams that its keys are the name of the data streams. 


 - <b>`global_timetrack`</b> (pd.DataFrame):  A data frame that stores the time, data stream type, and data pointers to allow the iteration over all samples in all data streams efficiently. 

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/collector.py#L60"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(data_streams: List[OfflineDataStream]) → None
```

Constructor for ``OfflineCollector``. 



**Args:**
 
 - <b>`data_streams`</b> (List[mm.OfflineDataStream]):  A list of offline data streams. 





---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/collector.py#L138"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `OnlineCollector`
TODO implementation. 

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/collector.py#L35"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(data_streams: List[Union[DataStream, OfflineDataStream]]) → None
```

Constructor for ``OfflineCollector``. 



**Args:**
 
 - <b>`data_streams`</b> (List[mm.DataStream]):  A list of data streams. 







---

_This file was automatically generated via [lazydocs](https://github.com/ml-tooling/lazydocs)._
