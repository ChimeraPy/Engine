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
from .data_stream import TabularDataStream

class TabularEntry(Entry):

    def __init__(
        self, 
        dir:pathlib.Path,
        name:str,
        ):
        """__init__.

        Args:
            dir (pathlib.Path): The directory to store the snap shots 
            of data.

            name (str): The name of the ``Entry``.

            dtype (str): The type of data (image, tabular, json).

        """

        # Saving the Entry attributes
        self.dir = dir
        self.name = name

        # Setting initial values
        self.unsaved_changes = pd.DataFrame(columns=['_time_', 'data'])
        self.num_of_total_changes = 0

        # Data types that only need one file that gets appended
        self.save_loc = self.dir / f"{self.name}.csv"

        # Then add an empty container
        self.stream = TabularDataStream.empty(name=name)

        # Write the initial component now
        self.flush()

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

        # If this is the first time, we want the headers
        if self.num_of_total_changes == 0:
            self.unsaved_changes.to_csv(self.save_loc, mode='a', index=False, header=True)
        else:
            self.unsaved_changes.to_csv(self.save_loc, mode='a', index=False, header=False)

        # Update the counter and clear out the unsaved items
        self.num_of_total_changes += len(self.unsaved_changes)
        self.unsaved_changes = self.unsaved_changes.iloc[0:0]

class ImageEntry(Entry):

    def __init__(
        self, 
        dir:pathlib.Path,
        name:str,
        ):

        # Storing input parameters
        self.dir = dir
        self.name = name
        
        # Setting initial values
        self.unsaved_changes = pd.DataFrame(columns=['_time_', 'data'])
        self.num_of_total_changes = 0

        # For image entry, need to save to a new directory
        self.save_loc = self.dir / self.name
        os.mkdir(self.save_loc)

        # Create a tabular data stream for the data
        self.stream = TabularDataStream.empty(name=name)
    
    def flush(self):
        # Depending on different type of inputs, we should save data differently
        # For images, we just need to save each image logged
        # Save the unsaved changes
        for i, row in self.unsaved_changes.iterrows():
            filepath = self.save_loc / f"{self.num_of_total_changes+i}.jpg"
            cv2.imwrite(str(filepath), row.frames)

        # Update the counter and clear out the unsaved items
        self.num_of_total_changes += len(self.unsaved_changes)
        self.unsaved_changes = self.unsaved_changes.iloc[0:0]
