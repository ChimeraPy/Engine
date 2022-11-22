# Built-in Imports
from typing import Dict
import pathlib
import os

from .record import Record


class TabularRecord(Record):
    def __init__(
        self,
        dir: pathlib.Path,
        name: str,
    ):
        """Construct an Tabular Record.

        Args:
            dir (pathlib.Path): The directory to store the snap shots \
            of data.
            name (str): The name of the ``Record``.

        """
        # Saving the Record attributes
        self.dir = dir
        self.name = name

        # If the directory doesn't exists, create it
        if not self.dir.exists():
            os.mkdir(self.dir)
