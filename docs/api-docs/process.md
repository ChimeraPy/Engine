<!-- markdownlint-disable -->

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/process.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `process`
Module focused on the ``Process`` implementation. 

Contains the following classes:  ``Process`` 



---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/process.py#L51"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `MetaProcess`
A meta class to ensure that the output of the process is a DataSample. 

Information: https://stackoverflow.com/questions/57104276/python-subclass-method-to-inherit-decorator-from-superclass-method 





---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/process.py#L63"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `Process`
Generic class that compartmentalizes computational steps for a datastream. 



**Attributes:**
 
 - <b>`inputs`</b> (List[str]):  A list of strings that specific what type of  data stream inputs are needed to compute. The order in which they are provided imply the order in the arguments of the ``forward`` method. Whenever a new data sample is obtain for the input, this process is executed. 


 - <b>`output`</b> (Optional[str]):  The name used to store the output of the ``forward`` method. 


 - <b>`trigger`</b> (Optional[str]):  An optional parameter that overwrites the inputs as the trigger. Instead of executing this process everytime there is a new data sample for the input, it now only executes this process when a new sample with the ``data_type`` of  the trigger is obtain. 

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/process.py#L84"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    inputs: List[str],
    output: Optional[str] = None,
    trigger: Optional[str] = None
)
```

Construct the ``Process``. 



**Args:**
 
 - <b>`inputs`</b> (List[str]):  A list of strings that specific what type of  data stream inputs are needed to compute. The order in which they are provided imply the order in the arguments of the ``forward`` method. Whenever a new data sample is obtain for the input, this process is executed. 


 - <b>`output`</b> (Optional[str]):  The name used to store the output of the ``forward`` method. 


 - <b>`trigger`</b> (Optional[str]):  An optional parameter that overwrites the inputs as the trigger. Instead of executing this process everytime there is a new data sample for the input, it now only executes this process when a new sample with the ``data_type`` of  the trigger is obtain. 




---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/process.py#L142"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `close`

```python
close()
```

Close function performed to close the process. 

---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/process.py#L20"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>function</kbd> `wrapper`

```python
wrapper(*args, **kwargs)
```








---

_This file was automatically generated via [lazydocs](https://github.com/ml-tooling/lazydocs)._
