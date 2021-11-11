<!-- markdownlint-disable -->

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/session.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `session`
Module focused on the ``Session`` implementation. 

Contains the following classes:  ``Session`` 



---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/session.py#L17"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `Session`
Data Storage that contains the latest version of all data types. 



**Attributes:**
 
 - <b>`records`</b> (Dict[str, DataSample]):  Stores the latest version of a ``data_type`` sample or the output of a process. 



**Todo:**
 * Allow the option to store the intermediate samples stored in the session. 

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/session.py#L30"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__() → None
```

``Session`` Constructor. 




---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/session.py#L44"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `apply`

```python
apply(process: Process) → Optional[DataSample]
```

Apply the process by obtaining the necessary inputs and stores the generated output in the records. 



**Args:**
 
 - <b>`process`</b> (pymddt.Process):  The process to be executed with the records. 

---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/session.py#L72"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `close`

```python
close() → None
```

Close session. 



**Todo:**
  * Add an argument to session to allow the saving of the session  values at the end. 

---

<a href="https://github.com/edavalosanaya/PyMMDT/blob/main/pymmdt/session.py#L34"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `update`

```python
update(sample: DataSample) → None
```

Store the sample into the records. 



**Args:**
 
 - <b>`sample`</b> (pymddt.DataSample):  The sample to be stored in the records. 




---

_This file was automatically generated via [lazydocs](https://github.com/ml-tooling/lazydocs)._
