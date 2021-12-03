"""Module focused on Video Data Streams.

Contains the following classes:
    ``VideoDataStream``

"""

# Subpackage Management
__package__ = 'video'

# Built-in Imports
from typing import Union, Tuple, Optional
import pathlib
import threading
import queue
import time

# Third-party Imports
import cv2
import pandas as pd
import numpy as np
import tqdm

# Internal imports
from pymmdt.data_stream import DataStream

# From: https://stackoverflow.com/a/19846691/13231446 
def threaded(fn):
    def wrapper(*args, **kwargs):
        thread = threading.Thread(target=fn, args=args, kwargs=kwargs)
        # thread.start()
        thread.deamon = True
        return thread
    return wrapper

class VideoDataStream(DataStream):
    """Implementation of DataStream focused on Video data.

    Attributes:
        name (str): The name of the data stream.

        video_path (Optional[Union[pathlib.Path, str]]): The path to the video file

        start_time (pd.Timedelta): The timestamp used to dictate the 
        beginning of the video.

    """

    def __init__(self, 
            name: str, 
            start_time: pd.Timedelta,
            video_path: Optional[Union[pathlib.Path, str]]=None, 
            fps: Optional[int]=None,
            size: Optional[Tuple[int, int]]=None,
            max_queue_size: int=1000,
        ) -> None:
        """Construct new ``VideoDataStream`` instance.

        Args:
            name (str): The name of the data stream.

            video_path (Optional[Union[pathlib.Path, str]]): The path to the video file

            start_time (pd.Timedelta): The timestamp used to dictate the 
            beginning of the video.

        """
        # First determine if this video is a path or an empty stream
        if video_path:

            # Ensure that the video is a str
            if isinstance(video_path, str):
                video_path = pathlib.Path(video_path)
            
            # Ensure that the file exists
            assert video_path.is_file() and video_path.exists(), "Video file must exists."

            # constructing video capture object
            self.video = cv2.VideoCapture(str(video_path))
            self.mode = 'reading'

            # Obtaining FPS and total number of frames
            self.fps = int(self.video.get(cv2.CAP_PROP_FPS))
            self.nb_frames = int(self.video.get(cv2.CAP_PROP_FRAME_COUNT))
            
            # Constructing timetrack
            # timetrack = pd.date_range(start=start_time, periods=self.nb_frames, freq=f"{int(1e9/self.fps)}N").to_frame()
            timeline = pd.TimedeltaIndex(
                pd.timedelta_range(
                    start=start_time, 
                    periods=self.nb_frames, 
                    freq=f"{int(1e9/self.fps)}N"
                )
            )
            
            # Get the correct thread
            self.thread = self.load_video_to_queue()

        # Else, this is an empty video data stream
        else:

            # Store the video attributes
            self.fps = fps
            self.size = size
            self.nb_frames = 0
            self.mode = "writing"
            
            # Ensure that the file extension is .mp4
            self.video = cv2.VideoWriter()

            # Create an empty timeline
            timeline = pd.TimedeltaIndex([])

            # Get the correct thread
            self.thread = self.queue_to_save_video()

        # Apply the super constructor
        super().__init__(name, timeline)

        # Setting the index is necessary for video, even before __iter__
        self.index = 0
        self.data_index = 0

        # For either reading or writing - we can use a queue to allow the
        # pipeline move faster.

        # Create a frame queue and a thread for loading frames quickly
        self.max_queue_size = max_queue_size
        self.queue = queue.Queue(maxsize=max_queue_size)

        # After the queue is set, start running the thread
        self._thread_exit = threading.Event()
        self._thread_continue = threading.Event()
        self._thread_continue.set() # Set as True in the beginning
        self._thread_exit.clear() # Set as False in the beginning
        self.thread.start()

    @classmethod
    def empty(
            cls, 
            name:str, 
            start_time:pd.Timedelta, 
            fps:int, 
            size:Tuple[int, int],
            max_queue_size:int=1000
        ):

        return cls(name, start_time, None, fps, size, max_queue_size)

    def open_writer(self, filepath: pathlib.Path) -> None:
        """Set the video writer by opening with the filepath."""
        assert self.mode == 'writing' and self.nb_frames == 0
        self.video.open(
            str(filepath),
            cv2.VideoWriter_fourcc(*'DIVX'),
            self.fps,
            (self.size[1], self.size[0])
        )

    def get_frame_size(self) -> Tuple[int, int]:
        """Get the video frame's width and height.

        Returns:
            size (Tuple[int, int]): The frame's width and height.

        """
        frame_width = int(self.video.get(3))
        frame_height = int(self.video.get(4))
        return (frame_width, frame_height)

    @threaded
    def load_video_to_queue(self):
       
        while True:

            # If requested to end, exit now
            if self._thread_exit.is_set():
                break

            # Check if the thread has been pause
            self._thread_continue.wait()

            # Once it is not paused, load the frames into the queue
            res, frame = self.video.read()

            # Check if end of file
            if type(frame) == type(None):
                break

            while True:
                # Instead of just getting stuck, we need to check
                # if the thread is supposed to stop.
                # If not, keep trying to put the frame into the queue
                if self._thread_exit.is_set():
                    break
                elif self.queue.maxsize != self.queue.qsize():
                    self.queue.put(frame.copy(), block=True)
                else:
                    continue

    def __getitem__(self, index) -> Tuple[np.ndarray, pd.Timedelta]:
        """Get the indexed data sample from the ``VideoDataStream``.

        Args:
            index (int): The requested index.

        Returns:
            DataSample: The indexed data sample.

        """
        # Only reading is allowed to access frames
        assert self.mode == "reading"

        # Ensure that the video index is correct
        self.set_index(index)

        # Only read the frames from the Queue, not anyway else,
        # since reading from the video is not thread safe!
        # print(f"{self} GET __getitem__")
        frame = self.queue.get(block=True)

        # Get the time of the sample
        timestamp = self.timetrack.iloc[self.index].time
       
        # Increase the pointer counter for the video
        self.data_index += 1
        self.index += 1

        # Return frame
        return frame, timestamp

    def set_index(self, new_index):
        """Set the video's index by updating the pointer in OpenCV."""
        # Not only is the index, but the also the data index
        new_data_index = self.timetrack.iloc[new_index].ds_index

        # If the data index does not match the requested index,
        # it means that some jump or cut has happend.
        # We need to clear our the reading queue and set the video.
        if self.index != new_index or self.data_index != new_data_index:
            print("Video miss - reassigning index")
            if self.mode == "reading":

                # and then clear queue safely
                self._thread_continue.clear()
                
                sample_meta = self.timetrack.iloc[new_index]
                ds_index = sample_meta['ds_index']
                
                # Clear out the queue
                with self.queue.mutex:
                    self.queue.queue.clear()
                
                # Then set the correct location
                self.video.set(cv2.CAP_PROP_POS_FRAMES, new_index-1)
                self.data_index = ds_index
                self.index = new_index

                # Continue the thread again
                self._thread_continue.set()

            return True
        else:
            return False

    @threaded
    def queue_to_save_video(self):

        # Keeping track of the previous queue size after the exit event
        previous_qsize = None
        
        while True:

            # If requested ended, continue until the queue is empty
            # print("queue_to_save_video: ", self._thread_exit.is_set())
            if self._thread_exit.is_set():

                # If the first after thread exit event
                if not previous_qsize:
                    bar = tqdm.tqdm(total=self.queue.qsize())
                    previous_qsize = self.queue.qsize()
                else:
                    update = self.queue.qsize() - previous_qsize 
                    bar.update(update)
                    previous_qsize = self.queue.qsize()

                # If queue empty, exit
                if self.queue.qsize() == 0:
                    break

            # Get the lastest frame 
            # print(f"{self} GET queue_to_save_video")
            try:
                frame = self.queue.get(block=True, timeout=2)
                assert frame.shape[:2] == self.size, f"frame mismatch - actual: {frame.shape[:2]}, expected: {self.size}"
                self.video.write(frame.copy())
            except queue.Empty:
                continue

    def append(self, timestamp: pd.Timedelta, frame: np.ndarray):
        assert self.mode == "writing"

        # Add to the timetrack (cannot be inplace)
        self.timetrack = self.timetrack.append({
            'time': timestamp, 
            'ds_index': int(len(self.timetrack))
        }, ignore_index=True)

        # Appending the file to the video writer
        self.queue.put(frame.copy())
        self.nb_frames += 1

    def close(self):
        """Close the ``VideoDataStream`` instance."""
        # Stop the threading
        print(f"{self} - CLOSING")
        self._thread_exit.set()
        print(f"{self} CLOSING: ", self._thread_exit.is_set())
        self.thread.join()

        # Closing the video capture device
        self.video.release()
