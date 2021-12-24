
# Package Management
__package__ = 'pymmdt'

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
from .data_stream import DataStream
from .video import VideoDataStream
from .tabular import TabularDataStream

class Entry:
    """Abstract Entry to be saved in the session and tracking changes."""

    def append(
        self,
        data:Any,
        timestamp:Optional[pd.Timedelta]=None
        ):
        """Add a new point of data to the ``Entry``.

        Any data points added are staged and not fully committed yet. 
        To write the data to memory, use the ``flush`` method. The 
        ``flush`` method is also called at the end of each ``start``, 
        ``step``, and ``end`` method of ``mm.Pipe``.

        Args:
            data (Any): The data point to append.

            timestamp (Optional[pd.Timedelta]): The timestamp associated
            to the data point.

        """
        # Create an entry to the unsaved_changes
        self.unsaved_changes = self.unsaved_changes.append({
            'time': timestamp,
            'data': data
        }, ignore_index=True)

    def flush(self):
        """Write/Save changes and mark them as processed.

        Raises:
            NotImplementedError: ``Entry`` is an abstract class. The 
            ``flush`` needs to be implemented in concrete classes.

        """
        raise NotImplementedError("``flush`` needs to be implemented.")        

class StreamEntry(Entry):
    """Concrete ``Entry`` that focuses on data streams.

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
        stream:DataStream,
        ):
        """Construct new ``StreamEntry``.

        Args:
            dir (pathlib.Path): The directory/filepath to store the 
            generated data file.

            name (str): The name of ``Entry``.

            stream (DataStream): The stream instance to store the data.
        """

        # Saving the Entry attributes
        self.dir = dir
        self.name = name
        self.stream = stream

        # Setting initial values
        self.unsaved_changes = pd.DataFrame(columns=['time', 'data'])
        self.num_of_total_changes = 0
        
        if isinstance(self.stream, VideoDataStream):
            self.filepath = self.dir / f"{self.name}.avi"
            self.stream.open_writer(self.filepath)

    def get_last_sample(self) -> Any:
        """Extract the last sample added to the ``Entry``.
        
        This function is important as it retrieves the last data sample.
        Given that sometimes the changes haven't been commited to memory,
        this function checks for unsaved changes and extract that first.
        Else, it obtains the last saved change.

        Returns:
            Any: Depending on the type of stream, the sample is returned.

        """
        # If there are unsaved changes, these are the last sample
        if len(self.unsaved_changes.index) != 0:
            return self.unsaved_changes.iloc[len(self.unsaved_changes)-1].data
        # Else, get the last saved changes
        else:
            # If the stream is empty, return None
            if len(self.stream) == 0:
                return None
            else:
                if isinstance(self.stream, TabularDataStream):
                    sample, time = self.stream[len(self.stream)-1]
                    return sample.to_dict()
                elif isinstance(self.stream, VideoDataStream):
                    sample, time = self.stream[len(self.stream)-1]
                    return sample

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

        # Else, let's save the changes
        for i, row in self.unsaved_changes.iterrows():
            self.stream.append(row.time, row.data)
        
        # Update the counter and clear out the unsaved items
        self.num_of_total_changes += len(self.unsaved_changes)
        self.unsaved_changes = self.unsaved_changes.iloc[0:0]

    def close(self):
        """Close the data stream stored in the ``Entry``.
        """
        # Close the data stream
        self.stream.close()

class PointEntry(Entry):
    """PointEntry.

    Attributes:
        dir (pathlib.Path): The directory/path to store the generated 
        file.

        name (str): The name of the entry and its data.

        stream (DataFrame): A DataFrame object that organizes the 
        temporal data.
    """


    def __init__(
        self, 
        dir:pathlib.Path,
        name:str,
        dtype:str,
        data:Any,
        timestamp:Optional[pd.Timedelta]=None
        ):
        """__init__.

        Args:
            dir (pathlib.Path): The directory to store the snap shots 
            of data.

            name (str): The name of the ``Entry``.

            dtype (str): The type of data (image, tabular, json).

            data (Any): The data to store as the initial value.

            start_time (Optional[pd.Timedelta]): The original timestamp.
        """

        # Saving the Entry attributes
        self.dir = dir
        self.name = name
        self.dtype = dtype

        # Setting initial values
        self.unsaved_changes = pd.DataFrame(columns=['time', 'data'])
        self.num_of_total_changes = 0
        self.start_time = timestamp

        # Data types that need a folder with multiple files
        if self.dtype in ['image']: 
            self.save_dir = self.dir / self.name
            os.mkdir(self.save_dir)

        # Data types that only need one file that gets appended
        if self.dtype in ['tabular']:
            self.file = self.dir / f"{self.name}.csv"

        # Adding the data sample
        self.unsaved_changes = self.unsaved_changes.append(
            {
                "time": timestamp,
                'data': data
            }
        )
        # Then add an empty container
        self.stream = pd.DataFrame(columns=['time', 'data'])

        # Write the initial component now
        self.flush()

    def get_last_sample(self):
        """Extract the last sample added to the ``Entry``.
        
        This function is important as it retrieves the last data sample.
        Given that sometimes the changes haven't been commited to memory,
        this function checks for unsaved changes and extract that first.
        Else, it obtains the last saved change.

        Returns:
            Any: Depending on the type of stream, the sample is returned.

        """
        # If there are unsaved changes, these are the last sample
        if len(self.unsaved_changes.index) != 0:
            return self.unsaved_changes.iloc[len(self.unsaved_changes)-1].data
        # Else, get the last saved changes
        else:
            return self.stream.iloc[len(self.stream)-1].data

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

        # Depending on different type of inputs, we should save data differently
        # For images, we just need to save each image logged
        if self.dtype == 'image':
            # Save the unsaved changes
            for i, row in self.unsaved_changes.iterrows():
                filepath = self.save_dir / f"{self.num_of_total_changes+i}.jpg"
                cv2.imwrite(str(filepath), row.data)

        # For tabular, one file gets appended to
        elif self.dtype == 'tabular':
            # If this is the first time, we want the headers
            for i, row in self.unsaved_changes.iterrows():
                df = row.data
                if self.num_of_total_changes + i == 0:
                    df.to_csv(self.file, mode='a', index=False, header=True)
                else:
                    df.to_csv(self.file, mode='a', index=False, header=False)

        # Update the counter and clear out the unsaved items
        self.num_of_total_changes += len(self.unsaved_changes)
        self.unsaved_changes = self.unsaved_changes.iloc[0:0]

    def close(self):
        """Close the data stream stored in the ``Entry``.
        """
        ...
