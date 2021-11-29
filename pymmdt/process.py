"""Module focused on the ``Process`` implementation.

Contains the following classes:
    ``Process``

"""

# Package Management
__package__ = 'pymmdt'

# Built-in Imports
from typing import Sequence, Optional

# Third-party imports
import pandas as pd

# Internal Imports

# def _data_sample_construction_decorator(func):
#     """Decorate to handle the convertion of process output to DataSample."""

#     def wrapper(*args, **kwargs):

#         # Extract the first argument as the self
#         self = args[0]

#         # If the session is not set, then just use ``step`` method
#         if not self.session:
#             return func(*args, **kwargs)

#         # Since there is a session, check if the input data
#         # has been seen before

#         rt = func(*args, **kwargs)

#     return wrapper

# class MetaProcess(type):
#     """A meta class to check if the process already has accepted this input. If the process has seen it 
#     before, it provides the previously recorded output.

#     Information: https://stackoverflow.com/questions/57104276/python-subclass-method-to-inherit-decorator-from-superclass-method
#     """

#     def __new__(cls, name, bases, attr):
#         """Modify subclasses of Process to have ``step`` wrapper."""
#         attr["step"] = _data_sample_construction_decorator(attr["step"])

#         return super(MetaProcess, cls).__new__(cls, name, bases, attr)

class Process():
    """Generic class that compartmentalizes computational steps for a datastream.

    """
    session = None
    time = pd.Timedelta(0)

    def __init__(self):
        ...

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
    
    def start(self): 
        """Apply process onto data sample.

        Raises:
            NotImplementedError: ``start`` method needs to be overwritten.

        """
        raise NotImplementedError("``start`` method needs to be implemented.")
    
    def step(self, *args, **kwargs): 
        """Apply process onto data sample.

        Raises:
            NotImplementedError: ``step`` method needs to be overwritten.

        """
        raise NotImplementedError("``step`` method needs to be implemented.")
    
    def end(self): 
        """Apply process onto data sample.

        Raises:
            NotImplementedError: ``end`` method needs to be overwritten.

        """
        raise NotImplementedError("``end`` method needs to be implemented.")

    # def attach_session(self, session):

    #     # Grab the session and store it
    #     self.session = session
