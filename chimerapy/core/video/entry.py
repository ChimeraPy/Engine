# Package Management
__package__ = 'tabular'

# Built-in Imports
import pathlib
import gc
import os

# Third-party Imports
import pandas as pd

# Internal Imports
from chimerapy.core.entry import Entry
from chimerapy.core.video.data_stream import VideoDataStream

class VideoEntry(Entry):
    """

    Attributes:
        dir (pathlib.Path): The directory/path to store the generated 
        file.

        name (str): The name of the entry and its data.

        stream (DataStream): The DataSteam object to write the data.

    """

    def __init__(
        self, 
        dir:pathlib.Path,
        name:str,
        ):
        """

        Args:
            dir (pathlib.Path): The directory/filepath to store the 
            generated data file.

            name (str): The name of ``Entry``.

            stream (DataStream): The stream instance to store the data.
        """

        # Saving the Entry attributes
        self.dir = dir
        self.name = name

        # If the directory doesn't exists, create it 
        if not self.dir.exists():
            os.mkdir(self.dir)

        # Setting initial values
        self.unsaved_changes = pd.DataFrame(columns=['_time_', 'data'])
        self.num_of_total_changes = 0
        
        self.save_loc = self.dir / f"{self.name}.avi"
        self.stream = VideoDataStream.empty(name=name, startup_now=True)
    
    def flush(self):
        """Commit the unsaved changes to memory."""

        # If no new changes, end
        if len(self.unsaved_changes.index) == 0:
            return None

        # If this is the first time, set the video writer to match the 
        # input size
        if self.num_of_total_changes == 0:
            
            # This operation requires at least two samples
            # If there not enough samples, wait until we do
            if len(self.unsaved_changes['frames']) < 2:
                return None

            # Determine the size
            first_frame = self.unsaved_changes['frames'].iloc[0]
            w, h = first_frame.shape[0], first_frame.shape[1]

            # Determine if RGB or grey video
            is_grey = len(first_frame.shape) == 2

            # Determine the fps
            t1 = self.unsaved_changes['_time_'].iloc[0]
            t2 = self.unsaved_changes['_time_'].iloc[1]
            period = (t2.microseconds - t1.microseconds) / 1_000_000
            average_fps = (1 / period) - 0.000300003000028 # This is necessary for some reason ?

            # Opening the frame writer with the new data
            self.stream.open_writer(
                video_path=self.save_loc,
                fps=average_fps,
                size=(w,h),
                grey=is_grey
            )

        # Else, let's save the changes
        self.stream.append(self.unsaved_changes)
        
        # Update the counter and clear out the unsaved items
        self.num_of_total_changes += len(self.unsaved_changes)
        self.unsaved_changes = self.unsaved_changes.iloc[0:0]

        # Ensure that the garbage is collected
        self.unsaved_changes = pd.DataFrame(columns=['_time_', 'data'])

    def close(self):

        # Apply the last changes
        self.flush()

        # Close the video data stream
        self.stream.close()
