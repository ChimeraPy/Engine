# Package Management
__package__ = 'chimerapy'

# Built-in Imports
from typing import Optional, Union, List

# Third-party imports
import pandas as pd

class Process():
    """Class that compartmentalizes computational steps for a datastream."""

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
    
    def step(self, *args, **kwargs) -> Optional[Union[pd.DataFrame, List[pd.DataFrame]]]: 
        """Apply process onto data sample.

        Raises:
            NotImplementedError: ``step`` method needs to be overwritten.

        """
        raise NotImplementedError("``step`` method needs to be implemented.")
