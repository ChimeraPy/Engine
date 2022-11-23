# Built-in Imports
from typing import Dict, Any
import pathlib

# Third-party Imports
import pandas as pd

# Internal Import
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
        super().__init__()

        # Saving the Record attributes
        self.dir = dir
        self.name = name
        self.tabular_file_path = self.dir / f"{self.name}.csv"

    def write(self, data_chunk: Dict[str, Any]):

        # Ensure that data is a pd.DataFrame
        if isinstance(data_chunk["data"], pd.DataFrame):
            df = data_chunk["data"]
        elif isinstance(data_chunk["data"], pd.Series):
            df = data_chunk["data"].to_frame().T
        elif isinstance(data_chunk["data"], Dict):
            df = pd.Series(data_chunk["data"]).to_frame().T
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
