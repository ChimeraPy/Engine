<!-- markdownlint-disable -->

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/video/video_data_stream.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `video.video_data_stream`
Module focused on Video Data Streams. 

Contains the following classes:  ``OfflineVideoDataStream`` 



---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/video/video_data_stream.py#L20"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `OfflineVideoDataStream`
Implementation of Offline DataStream focused on Video data. 



**Attributes:**
 
 - <b>`name`</b> (str):  The name of the data stream. 


 - <b>`video_path`</b> (Union[pathlib.Path, str]):  The path to the video file. 


 - <b>`start_time`</b> (pd.Timestamp):  The timestamp used to dictate the  beginning of the video. 

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/video/video_data_stream.py#L33"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(name: str, video_path: Union[Path, str], start_time: Timestamp) → None
```

Construct new ``OfflineVideoDataStream`` instance. 



**Args:**
 
 - <b>`name`</b> (str):  The name of the data stream. 


 - <b>`video_path`</b> (Union[pathlib.Path, str]):  The path to the video file 


 - <b>`start_time`</b> (pd.Timestamp):  The timestamp used to dictate the  beginning of the video. 




---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/video/video_data_stream.py#L119"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `close`

```python
close()
```

Close the ``OfflineVideoDataStream`` instance. 

---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/video/video_data_stream.py#L73"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `get_size`

```python
get_size() → Tuple[int, int]
```

Get the video frame's width and height. 



**Returns:**
 
 - <b>`size`</b> (Tuple[int, int]):  The frame's width and height. 

---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/video/video_data_stream.py#L84"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `set_index`

```python
set_index(new_index)
```

Set the video's index by updating the pointer in OpenCV. 




---

_This file was automatically generated via [lazydocs](https://github.com/ml-tooling/lazydocs)._
