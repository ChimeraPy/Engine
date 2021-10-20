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

            # Extracting the time column and generated tags and indecies
            time_series = ds.get_timestamps().to_frame()
            time_series.columns = ['time']
            stream_name_stamp = [ds.name for x in range(len(ds))]
            stream_index = [x for x in range(len(ds))]

            # Creating the dataframe
            time_series['ds_type'] = stream_name_stamp
            time_series['ds_index'] = stream_index

            # Storing the dataframe with all the other streams
            dss_times.append(time_series)

        # Converging the data streams tags to a global timetrack
        self.global_timetrack = pd.concat(dss_times, axis=0)

        # Sort by the time - only if there are more than 1 data stream
        if len(self.data_streams) > 1:
            self.global_timetrack = self.global_timetrack.sort_values(by='time')

        # Setting the index to zero to begin iterating over the data
        self.index = 0

    def __iter__(self):
        return self

    def __next__(self):
        # Stop iterating when index has overcome the size of the data
        if self.index >= len(self):
            raise StopIteration
        else:

            # Determine which datastream is next
            next_ds_meta = self.global_timetrack.iloc[self.index]
            next_ds_pointer = next_ds_meta['ds_index']
            next_ds_name = next_ds_meta['ds_type']
            next_ds_time = next_ds_meta['time']

            # Construct data sample (not loading data yet!)
            data_sample = DataSample(
                next_ds_name, 
                next_ds_time, 
                next_ds_pointer, 
                self.data_streams[next_ds_name]
            )

            # Change the index
            self.index += 1

            return data_sample

    def __len__(self):
        return len(self.global_timetrack)
