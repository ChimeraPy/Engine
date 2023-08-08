# Built-in Imports
from typing import Dict
import pathlib
import datetime

# Third-party Imports
import pandas as pd

# Internal Import
from ..entry import TabularEntry
from .record import Record


class TabularRecord(Record):
    def __init__(self, dir: pathlib.Path, name: str, start_time: datetime.datetime):
        """Construct an Tabular Record.

        Args:
            dir (pathlib.Path): The directory to store the snap shots \
            of data.
            name (str): The name of the ``Record``.

        """
        super().__init__(dir=dir, name=name, start_time=start_time)

        # Saving the Record attributes
        self.dir = dir
        self.name = name
        self.tabular_file_path = self.dir / f"{self.name}.csv"

    def write(self, entry: TabularEntry):

        # Ensure that data is a pd.DataFrame
        if isinstance(entry.data, pd.DataFrame):
            df = entry.data
        elif isinstance(entry.data, pd.Series):
            df = entry.data.to_frame().T
        elif isinstance(entry.data, Dict):
            df = pd.Series(entry.data).to_frame().T
        else:
            raise RuntimeError("Invalid input data for Tabular Record.")

        # Write to a csv
        df.to_csv(
            str(self.tabular_file_path),
            mode="a",
            header=not self.tabular_file_path.exists(),
            index=False,
        )

    def close(self):
        ...
