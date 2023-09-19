# Built-in Imports
from typing import Dict, Any
import pathlib

# Third-party Imports
import numpy as np
import cv2

# Internal Imports
from .record import Record

from datetime import datetime


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

        # Handling unstable FPS
        self.frame_count: int = 0
        self.previous_frame: np.ndarray = np.array([])
        self.start_time: datetime = datetime.now()

    def write(self, data_chunk: Dict[str, Any]):
        """Commit the unsaved changes to memory."""

        # Determine the size
        frame = data_chunk["data"]
        fps = data_chunk["fps"]
        timestamp = data_chunk["timestamp"]
        elapsed = (timestamp - self.start_time).total_seconds()
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
            # Write
            self.first_frame = False
            self.video_writer.write(np.uint8(frame))

        else:

            # Account for possible unstable fps
            delta = elapsed - (self.frame_count / fps)

            # Case 1: Too late (padd with previous frame to match)
            num_missed_frames = int(delta // (1 / fps)) - 1
            if num_missed_frames > 0:
                for i in range(num_missed_frames):
                    self.video_writer.write(self.previous_frame)
                    self.frame_count += 1

                # Then use the latest frame
                self.video_writer.write(np.uint8(frame))
                self.frame_count += 1

            # Case 2: On-time (by a certain tolerance), write
            elif delta / (1 / fps) >= 0.9:
                self.video_writer.write(np.uint8(frame))
                self.frame_count += 1

            # Case 3: Too early (only update previous data)
            else:
                pass

        # Update previous data
        self.previous_frame = frame.astype(np.uint8)

    def close(self):

        # Close the video
        self.video_writer.release()
