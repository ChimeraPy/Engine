# Built-in Imports
from typing import Dict, Any
import pathlib
import os

# Third-party Imports
import numpy as np
import cv2

# Internal Imports
from .record import Record


class VideoRecord(Record):
    def __init__(
        self,
        dir: pathlib.Path,
        name: str,
    ):
        """
        Args:
            dir (pathlib.Path): The directory/filepath to store the \
            generated data file.
            name (str): The name of ``Record``.

        """
        super().__init__()

        # Saving the Record attributes
        self.dir = dir
        self.name = name
        self.first_frame = True
        self.video_file_path = self.dir / f"{self.name}.mp4"
        # self.video_fourcc = cv2.VideoWriter_fourcc(*'MP4V')
        self.video_fourcc = cv2.VideoWriter_fourcc("m", "p", "4", "v")

    def write(self, data_chunk: Dict[str, Any]):
        """Commit the unsaved changes to memory."""

        # Determine the size
        frame = data_chunk["data"]
        fps = data_chunk["fps"]
        h, w = frame.shape[:2]

        # Determine if RGB or grey video
        grey = len(frame.shape) == 2

        if self.first_frame:

            if not grey:
                self.video_writer = cv2.VideoWriter(
                    str(self.video_file_path), self.video_fourcc, fps, (w, h)
                )
            # Opening the video writer given the desired parameters
            else:
                self.video_writer = cv2.VideoWriter(
                    str(self.video_file_path), self.video_fourcc, fps, (w, h), 0
                )

            self.first_frame = False

        # Write
        self.video_writer.write(np.uint8(frame))

    def close(self):

        # Close the video
        self.video_writer.release()
