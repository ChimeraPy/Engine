# Package Management
__package__ = 'mm'

# Built-in Imports
from typing import List

# Third-party Imports
import pandas as pd

# Internal Imports
from .data_sample import DataSample

########################################################################
# Classes
########################################################################

class Collector:
    
    def __init__(self, data_streams: List):
        self.data_streams = {x.name:x for x in data_streams}

class OfflineCollector(Collector):

    def __init__(self, data_streams: List):
        super().__init__(data_streams)

        # Data Streams (DSS) times, just extracting all the time stamps
        # and then tagging them as the name of the stream
        dss_times= []
        for ds_name, ds in self.data_streams.items():

            # Obtaining each ds's timetrack and adding an ds_type 
            # identifier to know which data stream
            time_series = ds.timetrack.copy()
            stream_name_stamp = [ds.name for x in range(len(ds))]
            time_series['ds_type'] = stream_name_stamp

            # Storing the dataframe with all the other streams
            dss_times.append(time_series)

        # Converging the data streams tags to a global timetrack
        self.global_timetrack = pd.concat(dss_times, axis=0)

        # Sort by the time - only if there are more than 1 data stream
        if len(self.data_streams) > 1:
            self.global_timetrack = self.global_timetrack.sort_values(by='time')

        # For debugging purposes
        # self.global_timetrack.to_excel('test.xlsx', index=False)

    def __iter__(self):
        self.index = 0
        return self

    def __next__(self) -> DataSample:
        # Stop iterating when index has overcome the size of the data
        if self.index >= len(self):
            raise StopIteration
        else:

            # Determine which datastream is next
            next_ds_meta = self.global_timetrack.iloc[self.index]
            next_ds_pointer = next_ds_meta['ds_index']
            next_ds_name = next_ds_meta['ds_type']

            # Checking if the pointer aligns with the data stream
            if next_ds_pointer != self.data_streams[next_ds_name].index:
               self.data_streams[next_ds_name].set_index(next_ds_pointer)

            # Get the sample
            data_sample = next(self.data_streams[next_ds_name])

            # Change the index
            self.index += 1

            return data_sample

    def __getitem__(self, index):
        
        if index >= len(self):
            raise InvalidIndexError
        else:

            # Determine which datastream is next
            next_ds_meta = self.global_timetrack.iloc[index]
            next_ds_pointer = next_ds_meta['ds_index']
            next_ds_name = next_ds_meta['ds_type']

            # Get the sample
            data_sample = self.data_streams[next_ds_name][next_ds_pointer]

            return data_sample

    def __len__(self):
        return len(self.global_timetrack)
