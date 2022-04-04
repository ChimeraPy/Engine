# Package Management
__package__ = 'chimerapy'

# Built-in imports
from typing import Dict, Optional, Union, List
import copy

# Third-party imports
import pandas as pd

# Local Imports
from chimerapy.core.session import Session

class Pipeline:

    # All pipelines have a session attribute
    session = None

    def __init__(self) -> None:
        """Construct a ``Pipeline``.

        The constructor is minimial and not required. The user of the 
        library is more than allowed to expand this constructor to take
        more parameters. But ``super().__init__`` is not required.

        """
        ...

    def __str__(self) -> str:
        """Get string representation of ``Pipeline``.

        Returns:
            str: string representation.

        """
        return self.__repr__()

    def copy(self) -> 'Pipeline':
        """Create a deep copy of the pipe."""
        return copy.deepcopy(self)

    def set_session(self, session: Session) -> None:
        """Set the session to the ``Pipeline`` instance.

        Args:
            session (Session): The session that acts as the interface \
                between the ``Pipeline`` and the ``Logger``.

        """
        # First make the session an attribute
        self.session = session

    def start(self) -> None:
        """Starting routine for the ``Pipeline``.

        This method is intended to be overwritten by the user to execute
        code before the ``Pipeline`` starts processing data.

        """
        ...

    def step(
        self, 
        data_samples: Union[Dict[str, pd.DataFrame], Dict[str, Dict[str, pd.DataFrame]]]
        ) -> Optional[Union[pd.DataFrame, List[pd.DataFrame]]]:
        """Take one step in processing a time window collection of data.

        The ``step`` method is the heart of the user defined processing 
        of the data from the data streams. Here the user organizes the
        ordering and what type of processing will be performed to 
        achieve getting the desired output.

        Args:
            data_samples (Union[Dict[str, pd.DataFrame], Dict[str, Dict[str, pd.DataFrame]]]): \
                The data samples are provided by the ``Loader``. The first \
                level keys are the runners' (``SingleRunner`` and \
                ``GroupRunner``) names. The second level keys are then the \
                names of the data streams; hence, the second level values \
                are the data frames obtain from each data stream.

        Returns:
            Optional[Union[pd.DataFrame, List[pd.DataFrame]]]: The \
                output (if running in a ``SingleRunner`` that is \
                orchestrated by ``GroupRunner``) is then feed to the main \
                ``Pipeline`` of the ``GroupRunner`` to allow collective \
                analyze of multiple ``Pipelines``.
        """
        ...

    def end(self) -> None:
        """Ending routine of the ``Pipeline``

        This method is has a similar purpose as ``start``, where the
        user can define logic at the end of the processing by the 
        ``Pipeline``.

        """
        ...
