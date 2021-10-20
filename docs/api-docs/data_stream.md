<!-- markdownlint-disable -->

<a href="../mm/data_stream.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `data_stream`






---

<a href="../mm/data_stream.py#L17"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `DataStream`




<a href="../mm/data_stream.py#L19"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(name)
```









---

<a href="../mm/data_stream.py#L22"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `OfflineDataStream`




<a href="../mm/data_stream.py#L19"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(name)
```









---

<a href="../mm/data_stream.py#L31"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `OfflineCSVDataStream`




<a href="../mm/data_stream.py#L33"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(name: str, data: DataFrame, time_column: str, data_columns: list)
```








---

<a href="../mm/data_stream.py#L44"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `get_timestamps`

```python
get_timestamps() → DatetimeIndex
```






---

<a href="../mm/data_stream.py#L53"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `OfflineVideoDataStream`




<a href="../mm/data_stream.py#L55"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(name: str, video_path: Union[Path, str], start_time: datetime)
```








---

<a href="../mm/data_stream.py#L78"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `get_timestamps`

```python
get_timestamps() → DatetimeIndex
```








---

_This file was automatically generated via [lazydocs](https://github.com/ml-tooling/lazydocs)._
