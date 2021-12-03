"""Module focused on the ``Collector`` and its various implementations.

Contains the following classes:
    ``Collector``

"""

# Package Management
__package__ = 'pymmdt'

# Built-in Imports
from typing import Sequence, Iterator, Dict, Any

# Third-party Imports
import pandas as pd

# Internal Imports
from .data_stream import DataStream

########################################################################
# Classes
########################################################################

class Collector:
    """Generic collector that stores only data streams.

    The offline collector allows the use of both __getitem__ and __next__
    to obtain the data pointer to a data stream to fetch the actual data.

    Attributes:
        data_streams (Dict[str, pymddt.DataStream]): A dictionary of the 
        data streams that its keys are the name of the data streams.

        global_timetrack (pd.DataFrame): A data frame that stores the time,
        data stream type, and data pointers to allow the iteration over
        all samples in all data streams efficiently.

    """

    def __init__(self, data_streams: Sequence[DataStream]) -> None:
        """Construct the ``OfflineCollector``.

        Args:
            data_streams (List[pymddt.DataStream]): A list of offline data streams.

        """
        # Constructing the data stream dictionary
        self.data_streams: Dict[str, DataStream] = {x.name:x for x in data_streams}

        # Data Streams (DSS) times, just extracting all the time stamps
        # and then tagging them as the name of the stream
        dss_times= []
        for dtype, ds in self.data_streams.items():

            # Obtaining each ds's timetrack and adding an ds_type 
            # identifier to know which data stream
            time_series = ds.timetrack.copy()
            stream_table_index = [int(x) for x in range(len(ds))]
            stream_name_stamp = [ds.name for x in range(len(ds))]
            time_series['ds_type'] = stream_name_stamp
            time_series['ds_index'] = stream_table_index

            # Storing the dataframe with all the other streams
            dss_times.append(time_series)

        # Converging the data streams tags to a global timetrack
        self.global_timetrack: pd.DataFrame = pd.concat(dss_times, axis=0)

        # Ensuring that the ds_index column is an integer
        self.global_timetrack['ds_index'] = self.global_timetrack['ds_index'].astype(int)

        # Sort by the time - only if there are more than 1 data stream
        if len(self.data_streams) > 1:
            self.global_timetrack.sort_values(by='time', inplace=True)

        # For debugging purposes
        # self.global_timetrack.to_excel('test.xlsx', index=False)

    def __iter__(self) -> Iterator[Any]:
        """Generate an iterator of ``DataSample`` from the ``Any``.
        
        Returns:
            Iterator[DataSample]: Iterator of data samples.

        """
        self.index = 0
        return self

    def __next__(self) -> Any:
        """Obtain next data sample from the Iterator[DataSample] instance.

        Returns:
            Anyt: The next upcoming data sample.

        """
        # Stop iterating when index has overcome the size of the data
        if self.index >= len(self):
            raise StopIteration
        else:

            # Use the __getitem__ function and obtain the 
            # data sample and its time
            timestamp, dtype, data_sample = self.__getitem__(self.index)

            # Change the index
            self.index += 1

            return timestamp, dtype, data_sample

    def __getitem__(self, index: int) -> Any:
        """Index certain data point of the ``DataStream``.

        Args:
            index (int): The requested index.

        Returns:
            Any: The requested data sample.

        """
        if index >= len(self):
            raise IndexError
        else:

            # Determine which datastream is next
            ds_meta = self.global_timetrack.iloc[index]
            timestamp = ds_meta['time']
            ds_pointer = ds_meta['ds_index']
            dtype = ds_meta['ds_type']
            
            # Checking if the pointer aligns with the data stream
            if ds_pointer != self.data_streams[dtype].index:
               self.data_streams[dtype].set_index(ds_pointer)

            # Get the sample
            data_sample = self.data_streams[dtype][ds_pointer]

            return timestamp, dtype, data_sample

    def __len__(self) -> int:
        """Get size of ``DataStream``.

        Returns:
            int: The size of the data stream.

        """
        return len(self.global_timetrack)

    def get_data_stream(self, name:str):
        if name in self.data_streams.keys():
            return self.data_streams[name]
        else:
            raise IndexError(f"{name} not found in saved data streams.")

    def close(self):

        print("CLOSING COLLECTOR")
        
        # For all data streams, apply their closing routines
        for dtype, ds in self.data_streams.items():
            ds.close()

