# Package Management
__package__ = 'mm'

# Built-in Imports
from typing import Union
import pathlib
import datetime
import collections
import types
import functools

# Third Party Imports
import tqdm
import pandas as pd
import cv2

# Internal Imports
from .data_sample import DataSample
from .process import Process

########################################################################
# Generic Classes
########################################################################

class DataStream():
    
    def __init__(self, name):
        self.name = name

    def __iter__(self):
        raise NotImplementedError

    def __next__(self):
        raise NotImplementedError

class OfflineDataStream(DataStream):

    def __init__(self, name: str, timetrack: pd.DataFrame):
        super().__init__(name)
        self.timetrack = timetrack

    def __iter__(self):
        self.index = 0
        return self

    def __next__(self) -> DataSample:
        if self.index >= len(self):
            raise StopIteration
        else:
            sample = self.__getitem__(self.index)
            self.index += 1
            return sample
    
    def __getitem__(self, index):
        raise NotImplementedError("__getitem__ needs to be implemented.")
    
    def __len__(self):
        return len(self.timetrack)

    def trim_before(self, trim_time: pd.Timestamp):

        # Obtain the mask 
        mask = self.timetrack['time'] > trim_time
        new_timetrack = self.timetrack[mask]
        new_timetrack.reset_index()

        # Store the new timetrack
        self.timetrack = new_timetrack

    def trim_after(self, trim_time: pd.Timestamp):

        # Obtain the mask 
        mask = self.timetrack['time'] < trim_time
        new_timetrack = self.timetrack[mask]
        new_timetrack.reset_index()

        # Store the new timetrack
        self.timetrack = new_timetrack

########################################################################
# Implementation Classes
########################################################################

class OfflineCSVDataStream(OfflineDataStream):

    def __init__(
            self, name: str, 
            data: pd.DataFrame, 
            time_column: str, 
            data_columns: list
        ):

        # Storing the data and which columns to find it
        self.data = data
        self.data_columns = data_columns

        # Need to construct the timetrack 
        timetrack = self.data[time_column].to_frame()
        timetrack.columns = ['time']
        timetrack['ds_index'] = [x for x in range(len(self.data))]

        # Applying the super constructor with the timetrack
        super().__init__(name, timetrack)

    @classmethod
    def from_process_and_ds(
            cls, 
            process: Process, 
            in_ds: OfflineDataStream,
            verbose: bool = False
        ):
        """Class method to construct data stream from an applied process to a data stream.

        Args:
            process (Process): the applied process
            in_ds (DataFrame): the incoming data stream to be processed

        Returns:
            self (OfflineCSVDataStream): the generated data stream

        """

        # Create data variable that will later be converted to a DataFrame
        data_store = collections.defaultdict(list)
        data_columns = set()

        # Iterate over all samples within the data stream
        for x in tqdm.tqdm(in_ds, total=len(in_ds), disable=verbose):

            # Process the sample and obtain the output
            y = process.forward(x)

            # If the output is None or an empty element, skip this time entry
            if not y:
                continue

            # Decompose the Data Sample
            data_store['time'].append(x.time)

            # If there is multiple outputs (dict), we need to store them in 
            # separate columns.
            if isinstance(y, dict):

                # Storing the output data
                for y_key in y.keys():
                    data_store[y_key].append(y[y_key])
                    data_columns.add(y_key)

            else: # Else, just store the value in the generic 'data' column
                data_store['data'].append(y)
                data_columns.add('data')

        # Convert the data to a pd.DataFrame
        df = pd.DataFrame(data_store)

        # Returning the construct object
        return cls(name=process.output, data=df, time_column='time', data_columns=list(data_columns))

    def __getitem__(self, index) -> DataSample:

        # Have to return a DataSample
        data_sample = DataSample(
            dtype=self.name,
            time=self.timetrack.iloc[index]['time'],
            data=self.data.iloc[index]
        )
        return data_sample 
    
class OfflineVideoDataStream(OfflineDataStream):
    
    def __init__(self, 
            name: str, 
            video_path: Union[pathlib.Path, str], 
            start_time: datetime.datetime
        ):

        # Ensure that the video is a str
        if isinstance(video_path, pathlib.Path):
            video_path = str(video_path)

        # constructing video capture object
        self.video_cap = cv2.VideoCapture(video_path)

        # Obtaining FPS and total number of frames
        self.fps = int(self.video_cap.get(cv2.CAP_PROP_FPS))
        self.nb_frames = int(self.video_cap.get(cv2.CAP_PROP_FRAME_COUNT))

        # Constructing timetrack
        timetrack = pd.date_range(start=start_time, periods=self.nb_frames, freq=f"{int(1e9/self.fps)}N").to_frame()
        timetrack.columns = ['time']
        timetrack['ds_index'] = [x for x in range(self.nb_frames)]

        # Apply the super constructor
        super().__init__(name, timetrack)

    def __getitem__(self, index) -> DataSample:
        # Only if the index does not match request index should we 
        # change the location of the buffer reader
        if self.index != index:
            self.video_cap.set(cv2.CAP_PROP_POS_FRAMES, index-1)

        # Load data
        res, frame = self.video_cap.read()

        # Creating a DataSample
        data_sample = DataSample(
            dtype=self.name,
            time=self.timetrack.iloc[index]['time'],
            data=frame
        )

        # Return frame
        return data_sample
