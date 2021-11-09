<!-- markdownlint-disable -->

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/video/processes.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `video.processes`
Module focused on video process implementations. 

Contains the following classes:  ``ShowVideo``  ``SaveVideo`` 



---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/video/processes.py#L20"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `ShowVideo`
Basic process that shows the video in a CV window. 



**Attributes:**
 
 - <b>`inputs`</b> (List[str]):  A list of strings containing the inputs requred to execute ``ShowVideo``. In this case, it needs a video frame. 


 - <b>`ms_delay`</b> (int):  A millisecond delay between shown frame. 

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/video/processes.py#L31"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(inputs: List[str], ms_delay: int = 1)
```

Constructor for ShowVideo. 



**Args:**
 
 - <b>`inputs`</b> (List[str]):  A list of strings containing the inputs required to execute ``ShowVideo``. In this case, it needs a video frame. 


 - <b>`ms_delay`</b> (int):  A millisecond delay between shown frame. 





---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/video/processes.py#L58"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `SaveVideo`
Basic process that saves the video. 



**Attributes:**
 
 - <b>`inputs`</b> (List[str]):  A list of strings containing the inputs  required to execute ``SaveVideo``. In this case, it needs a video frame. 


 - <b>`fps`</b> (int):  The frames per second (FPS) used to save the video. 


 - <b>`size`</b> (Tuple[int, int]):  The width and height of the video. 


 - <b>`writer`</b> (cv2.VideoWriter):  The video writer from OpenCV used to write and save the video. 

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/video/processes.py#L74"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    inputs: List[str],
    filepath: Union[str, Path],
    fps: int,
    size: Tuple[int, int],
    trigger: Optional[str] = None
)
```

Constructor for SaveVideo. 



**Args:**
 
 - <b>`inputs`</b> (List[str]):  A list of strings containing the inputs  required to execute ``SaveVideo``. In this case, it needs a video frame. 


 - <b>`filepath`</b> (Union[str, pathlib.Path]):  The filepath to save the  new video file. 


 - <b>`fps`</b> (int):  The frames per second (FPS) used to save the video. 


 - <b>`size`</b> (Tuple[int, int]):  The width and height of the video. 


 - <b>`trigger`</b> (Optional[str]):  The possible trigger to save the video, instead of relying on the inputs' update. 




---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/video/processes.py#L130"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `close`

```python
close()
```

Closing the video writer and saving the video. 




---

_This file was automatically generated via [lazydocs](https://github.com/ml-tooling/lazydocs)._
