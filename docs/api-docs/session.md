<!-- markdownlint-disable -->

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/session.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `session`
Module focused on the ``Session`` implementation. 

Contains the following classes:  ``Session`` 



---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/session.py#L17"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `Session`
Data Storage that contains the latest version of all data types. 



**Attributes:**
 
 - <b>`records`</b> (Dict[str, DataSample]):  Stores the latest version of a ``data_type`` sample or the output of a process. 



**Todo:**
 * Allow the option to store the intermediate samples stored in the session. 

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/session.py#L30"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__() → None
```

``Session`` Constructor. 




---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/session.py#L45"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `apply`

```python
apply(process: Process) → Optional[DataSample]
```

Applies the process by obtaining the necessary inputs and stores the generated output in the records. 



**Args:**
 
 - <b>`process`</b> (mm.Proces):  The process to be executed with the records. 

---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/session.py#L75"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `close`

```python
close() → None
```

Function executed at the end of the data processing. 



**Todo:**
  * Add an argument to session to allow the saving of the session  values at the end. 

---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/mm/session.py#L35"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `update`

```python
update(sample: DataSample) → None
```

Function that stores the sample into the records. 



**Args:**
 
 - <b>`sample`</b> (mm.DataSample):  The sample to be stored in the records. 




---

_This file was automatically generated via [lazydocs](https://github.com/ml-tooling/lazydocs)._
