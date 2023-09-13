# Built-in Imports
import json
from typing import Dict, Any, Optional, IO
import pathlib

# Third-party Imports

# Internal Import
from .record import Record


class JSONRecord(Record):
    def __init__(
        self,
        dir: pathlib.Path,
        name: str,
    ):
        """Construct a JSON Lines Record.

        Args:
            dir (pathlib.Path): The directory to store the snap shots of data.
            name (str): The name of the ``Record``.
        """
        super().__init__()

        # Saving the Record attributes
        self.dir = dir
        self.name = name
        self.jsonl_path = self.dir / f"{self.name}.jsonl"
        self.first_frame = False
        self.file_handler: Optional[IO[str]] = None

    def write(self, data_chunk: Dict[str, Any]):
        if not self.first_frame:
            self.file_handler = self.jsonl_path.open("w")
            self.first_frame = True

        json_data = json.dumps(data_chunk["data"], indent=0)
        assert self.file_handler is not None
        self.file_handler.write(f"{json_data}\n")

    def close(self):
        if self.file_handler is not None:
            self.file_handler.close()
            self.file_handler = None
