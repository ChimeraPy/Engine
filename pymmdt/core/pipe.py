"""Module focused on the ``Pipe`` implementation.

Contains the following classes:
    ``Pipe``

"""

# Package Management
__package__ = 'pymmdt'

# Built-in imports
from typing import Any, Dict, Optional, Union, List
import copy

# Third-party imports
import pandas as pd

# Local Imports
from pymmdt.core.process import Process
from pymmdt.core.session import Session
from pymmdt.core.collector import Collector

class Pipe:

    time = pd.Timedelta(0)
    session = None
    _processes = {}

    def __init__(self) -> None:
        ...

    def __repr__(self) -> str:

        # Construct a string that includes all the processes
        self_string = 'Pipe\n'

        # Combine the output string of the pipe and the processes.
        for process in self._processes.values():
            self_string += f'\t{process}\n'

        return self_string

    def __str__(self) -> str:
        return self.__repr__()

    def copy(self) -> 'Pipe':
        """Create a deep copy of the pipe."""
        return copy.deepcopy(self)

    def set_session(self, session: Session) -> None:

        # First make the session an attribute
        self.session = session

    def start(self) -> None:
        ...

    def step(self, data_samples: Union[Dict[str, pd.DataFrame], Dict[str, Dict[str, pd.DataFrame]]]) -> Optional[Union[pd.DataFrame, List[pd.DataFrame]]]:
        ...

    def end(self) -> None:
        ...
