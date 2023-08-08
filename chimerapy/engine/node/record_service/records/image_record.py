# Built-in Imports
import pathlib
import os
import datetime

# Third-party Imports
import cv2

# Internal Imports
from ..entry import ImageEntry
from .record import Record


class ImageRecord(Record):
    def __init__(self, dir: pathlib.Path, name: str, start_time: datetime.datetime):
        super().__init__(dir=dir, name=name, start_time=start_time)

        # Storing input parameters
        self.dir = dir
        self.name = name

        # For image entry, need to save to a new directory
        self.save_loc = self.dir / self.name
        os.makedirs(self.save_loc, exist_ok=True)
        self.index = 0

    def write(self, entry: ImageEntry):

        # Save the image
        img_filepath = self.save_loc / f"{self.index}.png"
        cv2.imwrite(str(img_filepath), entry.data)

        # Update the counter
        self.index += 1

    def close(self):
        ...
