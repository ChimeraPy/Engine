# ChimeraDash: A Multimodal Data Dashboard

After all this work: loading your data, defining your pipeline,
compartmentalizing your processes, and saving your data, you want to see
you data! That is what ChimeraDash is for, to visualize multimodal data
in an single application. Below is a screenshot of an example use case
of ChimeraDash.

![ChimeraDash Screenshot](../_static/ChimeraDash.png)

ChimeraDash works by using the data that was logged by the ``Logger``. 
Once the pipeline has been executed and all the logged data is saved,
you can invoke ChimeraDash with a simple command:

```python
chimerapy --logdir <your experiment directory>
```

## Groups: User or Entry Name

The visualize data can then be sorted by the ``user`` (the 
``SingleRunner`` associated with your data) or the ``entry_name`` (the
name you provided when you called ``session.add_*``). The ability to 
sort data in different ways to help compare and contrast different 
individuals when using the ``GroupRunner``.


