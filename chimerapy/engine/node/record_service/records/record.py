# Built-in Imports
import datetime
import pathlib
from typing import Dict, Any


class Record:
    """Abstract Entry to be saved in the session and tracking changes."""

    def __init__(self, dir: pathlib.Path, name: str, start_time: datetime.datetime):

        # Save parameters
        self.dir = dir
        self.name = name
        self.start_time = start_time

    def __repr__(self):
        """String representation of ``Entry``."""
        return f"{self.__class__.__name__} <name={self.name}, dir={self.dir}>"

    def __str__(self):
        """String representation of ``Entry``."""
        return self.__repr__()

    def write(self, data_chunk: Dict[str, Any]):
        """Write/Save changes and mark them as processed.

        Raises:
            NotImplementedError: ``Entry`` is an abstract class. The
            ``write`` needs to be implemented in concrete classes.

        """
        raise NotImplementedError("``write`` needs to be implemented.")

    def close(self):
        """Write/Save changes and mark them as processed.

        Raises:
            NotImplementedError: ``Entry`` is an abstract class. The
            ``close`` needs to be implemented in concrete classes.

        """
        raise NotImplementedError("``close`` needs to be implemented.")
