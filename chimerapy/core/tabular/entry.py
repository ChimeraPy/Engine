# Package Management
__package__ = 'tabular'

# Built-in Imports
import pathlib
import os
import gc

# Third-party Imports
import pandas as pd
import cv2

# Internal Imports
from chimerapy.core.entry import Entry

class TabularEntry(Entry):

    def __init__(
        self, 
        dir:pathlib.Path,
        name:str,
        ):
        """Construct an Tabular Entry.

        Args:
            dir (pathlib.Path): The directory to store the snap shots \
            of data.
            name (str): The name of the ``Entry``.

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

        # Data types that only need one file that gets appended
        self.save_loc = self.dir / f"{self.name}.csv"

        # # Then add an empty container
        # self.stream = TabularDataStream.empty(name=name)

        # Write the initial component now
        self.flush()

    def flush(self):
        """Commit the unsaved changes to memory."""

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

        # Collect the garbage
        del self.unsaved_changes
        self.unsaved_changes = pd.DataFrame(columns=['_time_', 'data'])
        gc.collect()

    def close(self):

        # Apply the last changes and that's it!
        self.flush()

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

    def flush(self):
        """Flush out unsaved changes to memory.

        For ``ImageEntry``, a folder is created, the images are stored
        inside, and finally a csv file is saved with the timestamps 
        pertaining to each image.

        """
        # Depending on different type of inputs, we should save data differently
        # For images, we just need to save each image logged
        # Save the unsaved changes
        for i, row in self.unsaved_changes.iterrows():
            # Storing the image
            filepath = self.save_loc / f"{self.num_of_total_changes+i}.jpg"
            cv2.imwrite(str(filepath), row.frames)
            # Storing timestamp data for all images
            meta_df = pd.DataFrame(
                {'_time_': [row._time_], 'idx': [self.num_of_total_changes+i]}
            )
            if self.num_of_total_changes == 0 and i == 0:
                meta_df.to_csv(self.save_loc / "timestamps.csv", mode='a', index=False, header=True)
            else: 
                meta_df.to_csv(self.save_loc / "timestamps.csv", mode='a', index=False, header=False)

        # Update the counter and clear out the unsaved items
        self.num_of_total_changes += len(self.unsaved_changes)
        self.unsaved_changes = self.unsaved_changes.iloc[0:0]

        # Collect the garbage
        del self.unsaved_changes
        self.unsaved_changes = pd.DataFrame(columns=['_time_', 'data'])
        gc.collect()

    def close(self):

        # Apply the last changes and that's it!
        self.flush()
