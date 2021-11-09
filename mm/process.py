"""Module focused on the ``Process`` implementation.

Contains the following classes:
    ``Process``

"""

# Package Management
__package__ = 'mm'

# Built-in Imports
from typing import List, Optional

# Internal Imports
from .data_sample import DataSample

def _data_sample_construction_decorator(func):
    """Decorate to handle the convertion of process output to DataSample."""

    def wrapper(*args, **kwargs):
        
        # Detecting the input data sample latest's timestamp
        timestamps = [x.time for x in args[1:]]
        latest_timestamp = max(timestamps)

        # # Removing the DataSample wrapper
        # for arg in args:
        #     if isinstance(arg, DataSample):
        #         arg = arg.data
        # for k,v in kwargs.items():
        #     if isinstance(v, DataSample):
        #         kwargs[k] = v.data

        # Apply the forward function of the process
        rt = func(*args, **kwargs)

        # Only if there is a return item do we enclose it in a DataSample
        if type(rt) != type(None):

            # Construct a data sample around the results
            data_sample = DataSample(
                args[0].output, # the class is the first argument
                latest_timestamp,
                rt
            )

            # Return the sample instead of the results
            return data_sample
    return wrapper

class MetaProcess(type):
    """A meta class to ensure that the output of the process is a DataSample.

    Information: https://stackoverflow.com/questions/57104276/python-subclass-method-to-inherit-decorator-from-superclass-method
    """

    def __new__(cls, name, bases, attr):
        """Modify subclasses of Process to have ``forward`` wrapper."""
        attr["forward"] = _data_sample_construction_decorator(attr["forward"])

        return super(MetaProcess, cls).__new__(cls, name, bases, attr)

class Process(metaclass=MetaProcess):
    """Generic class that compartmentalizes computational steps for a datastream.

    Attributes:
            inputs (List[str]): A list of strings that specific what type of 
            data stream inputs are needed to compute. The order in which they
            are provided imply the order in the arguments of the ``forward`` method.
            Whenever a new data sample is obtain for the input, this process
            is executed.

            output (Optional[str]): The name used to store the output of
            the ``forward`` method.

            trigger (Optional[str]): An optional parameter that overwrites
            the inputs as the trigger. Instead of executing this process
            everytime there is a new data sample for the input, it now only
            executes this process when a new sample with the ``data_type`` of 
            the trigger is obtain.

    """

    def __init__(
            self, 
            inputs: List[str], 
            output: Optional[str]=None,
            trigger: Optional[str]=None,
        ):
        """Construct the ``Process``.

        Args:
            inputs (List[str]): A list of strings that specific what type of 
            data stream inputs are needed to compute. The order in which they
            are provided imply the order in the arguments of the ``forward`` method.
            Whenever a new data sample is obtain for the input, this process
            is executed.

            output (Optional[str]): The name used to store the output of
            the ``forward`` method.

            trigger (Optional[str]): An optional parameter that overwrites
            the inputs as the trigger. Instead of executing this process
            everytime there is a new data sample for the input, it now only
            executes this process when a new sample with the ``data_type`` of 
            the trigger is obtain.

        """
        self.inputs = inputs
        self.output = output
        self.trigger = trigger

    def __repr__(self):
        """Representation of ``Process``.
        
        Returns:
            str: The representation of ``Process``.

        """
        return f"{self.__class__.__name__}"

    def __str__(self):
        """Get String form of ``Process``.

        Returns:
            str: The string representation of ``Process``.

        """
        return self.__repr__()
    
    def forward(self, x: DataSample): 
        """Apply process onto data sample.

        Args:
            x: a sample (of any type) in to the process.

        Raises:
            NotImplementedError: forward method needs to be overwritten.
        """
        raise NotImplementedError("forward method needs to be implemented.")

    def close(self):
        """Close function performed to close the process."""
        ...

