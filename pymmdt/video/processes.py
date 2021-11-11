"""Module focused on video process implementations.

Contains the following classes:
    ``ShowVideo``
    ``SaveVideo``

"""

# Subpackage management
__package__ = 'video'

from typing import Sequence, Tuple, Union, Optional
import pathlib

import cv2

from pymmdt.process import Process
from pymmdt.data_sample import DataSample

class ShowVideo(Process):
    """Basic process that shows the video in a CV window.

    Attributes:
        inputs (Sequence[str]): A list of strings containing the inputs
        requred to execute ``ShowVideo``. In this case, it needs a video frame.

        ms_delay (int): A millisecond delay between shown frame.

    """

    def __init__(self, inputs: Sequence[str], ms_delay: int=1):
        """Construct new ``ShowVideo`` instance.

        Args:
            inputs (Sequence[str]): A list of strings containing the inputs
            required to execute ``ShowVideo``. In this case, it needs a video frame.

            ms_delay (int): A millisecond delay between shown frame.

        """
        super().__init__(inputs)
        self.ms_delay = ms_delay

    def forward(self, frame_sample: DataSample) -> None:
        """Forward propagate frame_sample.

        Args:
            frame_sample (pymmdt.DataSample): The data sample that contains
            the video frame.

        """
        # Extract the frame
        frame = frame_sample.data

        cv2.imshow("Video", frame)
        cv2.waitKey(self.ms_delay)

class SaveVideo(Process):
    """Basic process that saves the video.

    Attributes:
        inputs (Sequence[str]): A list of strings containing the inputs 
        required to execute ``SaveVideo``. In this case, it needs a video frame.

        fps (int): The frames per second (FPS) used to save the video.

        size (Tuple[int, int]): The width and height of the video.

        writer (cv2.VideoWriter): The video writer from OpenCV used to
        write and save the video.

    """

    def __init__(
            self, 
            inputs: Sequence[str], 
            filepath: Union[str, pathlib.Path], 
            fps: int,
            size: Tuple[int, int],
            trigger: Optional[str]=None
        ) -> None:
        """Construct new ``SaveVideo`` instance.

        Args:
            inputs (Sequence[str]): A list of strings containing the inputs 
            required to execute ``SaveVideo``. In this case, it needs a video frame.

            filepath (Union[str, pathlib.Path]): The filepath to save the 
            new video file.

            fps (int): The frames per second (FPS) used to save the video.

            size (Tuple[int, int]): The width and height of the video.

            trigger (Optional[str]): The possible trigger to save the video,
            instead of relying on the inputs' update.

        """
        super().__init__(inputs, None, trigger)
        
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

    def forward(self, frame_sample: DataSample) -> None:
        """Forward propagate the frame sample.

        Args:
            frame_sample (pymmdt.DataSample): The data sample that contains
            the video frame.

        """
        # Extract the frame
        frame = frame_sample.data
        # Write the frame to the video
        self.writer.write(frame)

    def close(self) -> None:
        """Close the video writer and save the video."""
        # Close the video writer
        self.writer.release()
