# Built-in Imports
from typing import Dict
import pathlib
import os

from .record import Record


class AudioRecord(Record):
    def __init__(
        self,
        dir: pathlib.Path,
        name: str,
    ):

        # Storing input parameters
        self.dir = dir
        self.name = name
