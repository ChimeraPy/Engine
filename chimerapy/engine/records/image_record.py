# Built-in Imports
import os
import pathlib
import shutil
from typing import Any, Dict

# Third-party Imports
import cv2

# Internal Imports
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
        self.zip_loc = None
        os.makedirs(self.save_loc, exist_ok=True)
        self.index = 0

    def write(self, data_chunk: Dict[str, Any]):

        # Save the image
        img_filepath = self.save_loc / f"{self.index}.png"
        cv2.imwrite(str(img_filepath), data_chunk["data"])

        # Update the counter
        self.index += 1

    def close(self):
        save_name = self.save_loc.parent / self.name
        shutil.make_archive(str(save_name), "zip", str(self.save_loc))
        self.zip_loc = save_name.with_suffix(".zip")

    def get_meta(self):
        return {
            "name": self.name,
            "path": self.zip_loc,
            "glob": None,
            "mime_type": "application/zip"
        }
