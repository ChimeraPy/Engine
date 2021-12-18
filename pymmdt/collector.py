"""Module focused on the ``Collector`` and its various implementations.

Contains the following classes:
    ``Collector``

"""

# Package Management
__package__ = 'pymmdt'

# Built-in Imports
from typing import Sequence, Iterator, Dict, Any, Union

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
        data_streams (Dict[str, mm.DataStream]): A dictionary of the 
        data streams that its keys are the name of the data streams.

        global_timetrack (pd.DataFrame): A data frame that stores the time,
        data stream type, and data pointers to allow the iteration over
        all samples in all data streams efficiently.

    """

    def __init__(
            self, 
            data_streams: Sequence[DataStream],
            time_window_size: pd.Timedelta,
        ) -> None:
        """Construct the ``OfflineCollector``.

        In the constructor, the global timeline/timetrack is generated
        that connects all the data streams. This universal timetrack
        only contains the pointers to the individual data stream values.
        By using only the pointers, the global timetrack is concise, 
        small, and cheap to generate on the fly.

        Once the timetrack is generated, the timetrack can be iterated
        from the beginning to end. The data stream pointers help
        retrieve the correct data in an orderly fashion.

        Args:
            data_streams (Sequene[mm.DataStream]): A list of offline 
            data streams.

            time_window_size (pd.Timedelta): The size of the time window
            from where samples are collected and process together.

        """
        # Constructing the data stream dictionary
        self.data_streams: Dict[str, DataStream] = {x.name:x for x in data_streams}
        self.time_window_size: pd.Timdelta = time_window_size

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
        
        # Split samples based on the time window size
        self.start = self.global_timetrack['time'][0]
        self.end = self.global_timetrack['time'][len(self.global_timetrack)]
        self.size = (self.end - self.start) / self.time_window_size

    def __iter__(self) -> Iterator[Any]:
        """Generate an iterator of the entire timetrack and its pointers.
        
        Returns:
            Iterator[DataSample]: Iterator of data samples.

        """
        self.index = 0
        return self

    def __next__(self) -> Any:
        """Obtain next data sample from the Iterator[Any] instance.

        The global timetrack/timeline is keep as a record. Then the 
        collector has an index that is updated throughout the iterator.
        This index is used to obtain the next upcoming data sample. 
        Each data stream has their own index, but the collector's index
        focuses on the current location in the global timetrack.

        Returns:
            Any: The next upcoming data sample.

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

            # Given the index, find the start_time and end_time of the 
            # window
            window_start = self.start + self.time_window_size * self.index
            window_end = self.start + self.time_window_size * (self.index + 1)

            # Obtain the data samples from all data streams given the 
            # window start and end time
            data_samples = {}
            for dtype, data_stream in self.data_streams.items():
                data_samples[dtype] = data_stream.get(window_start, window_end)

            return data_samples

    def __len__(self) -> int:
        """Get size of ``Collector``. Size of the global timetrack.

        Returns:
            int: The number of slices of the global timetrack given the 
            size of the time window
        """
        return self.size

    def trim_before(self, trim_time: pd.Timedelta) -> None:
        self.start = trim_time

    def trim_after(self, trim_time: pd.Timedelta) -> None:
        self.end = trim_time

    def get_data_stream(self, name:str):
        """Extracts the data stream requested from all data streams.

        Args:
            name (str): The name of the data stream.

        Returns:
            DataStream: The requested data stream

        Raises:
            IndexError: If the requested data stream is not found, this 
            error is raised.
        
        """
        if name in self.data_streams.keys():
            return self.data_streams[name]
        else:
            raise IndexError(f"{name} not found in saved data streams.")

    def close(self):
        """Propagate the closing routine to the stored data streams.

        This routine is important to tell the collector's data streams 
        to close - such as the VideoDataStream to release the video 
        capture and video writer.

        """
        # For all data streams, apply their closing routines
        for dtype, ds in self.data_streams.items():
            ds.close()

