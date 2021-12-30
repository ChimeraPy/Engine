# Package Management
__package__ = 'tabular'

# Built-in Imports
from typing import Union, Dict, Optional, Any
import collections
import pathlib
import os
import shutil

# Third-party Imports
import pandas as pd
import numpy as np
import cv2

# Internal Imports
from pymmdt.entry import Entry
from pymmdt.video.data_stream import VideoDataStream

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

        # Setting initial values
        self.unsaved_changes = pd.DataFrame(columns=['_time_', 'data'])
        self.num_of_total_changes = 0
        
        self.save_loc = self.dir / f"{self.name}.avi"
        self.stream = VideoDataStream.empty(name=name)
    
    def flush(self):
        """Commit the unsaved changes to memory.

        TODO:
            - Might want to create a separate thread for this, as I/O
            processes can be very slow. Or maybe we can add the new 
            thread in the DataStream class ``save`` method.
        """

        # If no new changes, end
        if len(self.unsaved_changes.index) == 0:
            return None

        # If this is the first time, set the video writer to match the 
        # input size
        if self.num_of_total_changes == 0:

            # Determine the size
            first_frame = self.unsaved_changes['frames'].iloc[0]
            w, h = first_frame.shape[0], first_frame.shape[1]

            # Determine the fps
            time_column1 = self.unsaved_changes['_time_'].iloc[:-1].to_list()
            time_column2 = self.unsaved_changes['_time_'].iloc[1:].to_list()

            diff = 0
            for i in range(len(time_column1)):
                diff += (time_column2[i].microseconds - time_column1[i].microseconds) / 1000000

            average_period = diff / len(time_column1)
            average_fps = int(1 / average_period)

            # Opening the frame writer with the new data
            self.stream.open_writer(
                filepath=self.save_loc,
                fps=int(average_fps),
                size=(w,h)
            )

        # Else, let's save the changes
        self.stream.append(self.unsaved_changes)
        
        # Update the counter and clear out the unsaved items
        self.num_of_total_changes += len(self.unsaved_changes)
        self.unsaved_changes = self.unsaved_changes.iloc[0:0]
