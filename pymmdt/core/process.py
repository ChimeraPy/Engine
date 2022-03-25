"""Module focused on the ``Process`` implementation.

Contains the following classes:
    ``Process``

"""

# Package Management
__package__ = 'pymmdt'

# Built-in Imports
from typing import Optional, Union, List

# Third-party imports
import pandas as pd

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
    
    def step(self, *args, **kwargs) -> Optional[Union[pd.DataFrame, List[pd.DataFrame]]]: 
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
