# Subpackage management
__package__ = 'video'

from typing import Sequence, Tuple, Union, Optional
import pathlib
import datetime

import cv2
import numpy as np
import pandas as pd

from chimerapy.core.process import Process

class ShowVideo(Process):
    """Basic process that shows the video in a CV window.

    Attributes:
        ms_delay (int): A millisecond delay between shown frame.

    """

    def __init__(self, ms_delay: int=1):
        """Construct new ``ShowVideo`` instance.

        Args:
            ms_delay (int): A millisecond delay between shown frame.

        """
        super().__init__()
        self.ms_delay = ms_delay

    def step(self, frame: np.ndarray) -> None:
        """step propagate frame_sample.

        Args:
            frame_sample (mm.DataSample): The data sample that contains
            the video frame.

        """
        cv2.imshow("Video", frame)
        cv2.waitKey(self.ms_delay)

class SaveVideo(Process):
    """Basic process that saves the video.

    Attributes:
        fps (int): The frames per second (FPS) used to save the video.

        size (Tuple[int, int]): The width and height of the video.

        writer (cv2.VideoWriter): The video writer from OpenCV used to
        write and save the video.

    """

    def __init__(
            self, 
            filepath: Union[str, pathlib.Path], 
            fps: int,
            size: Tuple[int, int],
        ) -> None:
        """Construct new ``SaveVideo`` instance.

        Args:
            filepath (Union[str, pathlib.Path]): The filepath to save the 
            new video file.

            fps (int): The frames per second (FPS) used to save the video.

            size (Tuple[int, int]): The width and height of the video.

        """
        super().__init__()
        
        # Storing variables
        self.fps = fps
        self.size = size

        # Ensure that the filepath is a pathlib.Path object
        if isinstance(filepath, str):
            filepath = pathlib.Path(filepath)

        # Ensure that the file extension is .mp4
        safe_filepath = filepath.parent / (filepath.stem + ".avi")
        self.writer = cv2.VideoWriter(
            str(safe_filepath),
            cv2.VideoWriter_fourcc(*'DIVX'),
            self.fps, 
            self.size
        )

    def step(self, frame: np.ndarray) -> None:
        """step propagate the frame sample.

        Args:
            frame_sample (mm.DataSample): The data sample that contains
            the video frame.

        """
        # Write the frame to the video
        self.writer.write(frame)

    def close(self) -> None:
        """Close the video writer and save the video."""
        # Close the video writer
        self.writer.release()

class TimestampVideo(Process):
    """Basic process that writes the timestamp to the video."""

    def step(self, frame: np.ndarray, timestamp: pd.Timedelta) -> np.ndarray:
        """step propagate the frame sample.

        Args:
            frame (np.ndarray): The data sample that contains
            the video frame.

            timestamp (pd.Timedelta): The timestamp to place on frame.

        """

        if len(frame.shape) == 3: # RGB Image 
            h, w, c = frame.shape
        elif len(frame.shape) == 2: # Grey Image
            h, w = frame.shape
        else:
            raise RuntimeError(f"Invalid video frame type: {len(frame.shape)} should be 2 or 3")

        # Constructing a timestamp from timedelta
        total_seconds = timestamp.seconds
        text_timestamp = str(datetime.timedelta(seconds=total_seconds))

        # Account for it is negative
        if str(timestamp)[0] == '-':
            text_timestamp = '-' + text_timestamp
        
        # Drawing the text
        cv2.putText(frame, text_timestamp, (w-len(text_timestamp)*18, h), cv2.FONT_HERSHEY_SIMPLEX, 1, (0,0,255), 2)

        return frame

class CopyVideo(Process):
    """Basic process that makes a copy of the video frame."""

    def step(self, frame: np.ndarray) -> np.ndarray:
        """step propagate the frame sample.

        Args:
            frame (np.ndarray): The data sample that contains
            the video frame.

        """
        # Making a copy of the video
        drawn_video = frame.copy()

        # Returning the copied video
        return drawn_video
