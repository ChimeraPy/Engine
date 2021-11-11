<!-- markdownlint-disable -->

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/data_stream.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `data_stream`
Module focus on DataStream and its various implementations. 

Contains the following classes:  ``DataStream``  ``OfflineDataStream``  ``OnlineDataStream`` 



---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/data_stream.py#L33"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `DataStream`
Generic data sample for both offline and online streaming. 

The DataStream class is a generic class that needs to be inherented and have its __iter__ and __next__ methods overwritten. 



**Raises:**
 
 - <b>`NotImplementedError`</b>:  The __iter__ and __next__ functions need to be implemented before calling an instance of this class. 

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/data_stream.py#L45"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(name: str) → None
```

Construct the ``DataStream``. 



**Args:**
 
 - <b>`name`</b> (str):  the name of the data stream. 




---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/data_stream.py#L93"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `close`

```python
close() → None
```

Close routine for ``DataStream``. 


---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/data_stream.py#L97"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `OfflineDataStream`
Generic data stream for offline processing. Mostly loading data files. 

OfflineDataStream is intended to be inherented and its __getitem__  method to be overwritten to fit the modality of the actual data. 



**Attributes:**
 
 - <b>`index`</b> (int):  Keeping track of the current sample to load in __next__. 



**Raises:**
 
 - <b>`NotImplementedError`</b>:  __getitem__ function needs to be implemented  before calling. 

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/data_stream.py#L112"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(name: str, timetrack: DataFrame)
```

Construct the OfflineDataStream. 



**Args:**
 
 - <b>`name`</b> (str):  the name of the data stream. 


 - <b>`timetrack`</b> (pd.DataFrame):  the time track of the data stream where timestamps are provided for each data point. 




---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/data_stream.py#L93"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `close`

```python
close() → None
```

Close routine for ``DataStream``. 

---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/data_stream.py#L203"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `set_index`

```python
set_index(new_index: int) → None
```

Set the index used for the __next__ method. 



**Args:**
 
 - <b>`new_index`</b> (int):  The new index to be set. 

---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/data_stream.py#L187"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `trim_after`

```python
trim_after(trim_time: Timestamp) → None
```

Remove data points after the trim_time timestamp. 



**Args:**
 
 - <b>`trim_time`</b> (pd.Timestamp):  The cut-off time. 

---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/data_stream.py#L171"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `trim_before`

```python
trim_before(trim_time: Timestamp) → None
```

Remove data points before the trim_time timestamp. 



**Args:**
 
 - <b>`trim_time`</b> (pd.Timestamp):  The cut-off time.  




---

_This file was automatically generated via [lazydocs](https://github.com/ml-tooling/lazydocs)._
