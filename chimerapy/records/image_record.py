# Built-in Imports
from typing import Dict
import pathlib
import os

from .record import Record


class ImageRecord(Record):
    def __init__(
        self,
        dir: pathlib.Path,
        name: str,
    ):
        super().__init__()

        # Storing input parameters
        self.dir = dir
        self.name = name

        # For image entry, need to save to a new directory
        self.save_loc = self.dir / self.name
        os.mkdir(self.save_loc)
