<!-- markdownlint-disable -->

<a href="../mm/visualization.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `visualization`






---

<a href="../mm/visualization.py#L7"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `Visualization`
Class intended to execute after all the processes to visualize data. 



**Attributes:**
 
 - <b>`inputs`</b> (list):  A list of data stream names that are needed to create the visualization 
 - <b>`outputs`</b> (list):  A list of output data streams. The list of names are used to create new datastreams. 

<a href="../mm/visualization.py#L17"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(per_update: int, inputs: List[str], outputs: List[str])
```








---

<a href="../mm/visualization.py#L22"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `forward`

```python
forward(x)
```

Function that generates the visualization. 



**Raises:**
 
 - <b>`NotImplementedError`</b>:  This function is needed to be overwritten in the implementation class that inherets the Visualization class. 




---

_This file was automatically generated via [lazydocs](https://github.com/ml-tooling/lazydocs)._
