# Built-in Imports
from typing import Dict, Any, Optional, IO
import pathlib

# Third-party Imports

# Internal Import
from .record import Record


class TextRecord(Record):
    def __init__(
        self,
        dir: pathlib.Path,
        name: str,
    ):
        """Construct a text file Record.

        Args:
            dir (pathlib.Path): The directory to store the snap shots of data.
            name (str): The name of the ``Record``.
            suffix (str): The suffix of the text file. Defaults to "txt".
        """
        super().__init__()

        # Saving the Record attributes
        self.dir = dir
        self.name = name
        self.first_frame = False
        self.file_handler: Optional[IO[str]] = None

    def write(self, data_chunk: Dict[str, Any]):
        if not self.first_frame:
            self.file_handler = (self.dir / f"{self.name}.{data_chunk['suffix']}").open(
                "w"
            )
            self.first_frame = True

        text_data = data_chunk["data"]
        assert self.file_handler is not None
        self.file_handler.write(text_data)

    def close(self):
        if self.file_handler is not None:
            self.file_handler.close()
            self.file_handler = None
