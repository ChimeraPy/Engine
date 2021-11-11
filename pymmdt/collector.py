"""Module focused on the ``Collector`` and its various implementations.

Contains the following classes:
    ``Collector``
    ``OfflineCollector``
    ``OnlineCollector``

"""

# Package Management
__package__ = 'pymmdt'

# Built-in Imports
from typing import Sequence, Iterator, Type, Dict, Union

# Third-party Imports
import pandas as pd

# Internal Imports
from .data_sample import DataSample
from .data_stream import DataStream, OfflineDataStream

########################################################################
# Classes
########################################################################

class Collector:
    """Generic collector that stores a data streams.

    Attributes:
        data_streams (Dict[str, pymddt.DataStream]): A dictionary of the 
        data streams that its keys are the name of the data streams.

    """

    def __init__(self, data_streams: Sequence[DataStream]) -> None:
        """Construct the ``Collector``.

        Args:
            data_streams (List[pymddt.DataStream]): A list of data streams.

        """
        # Constructing the data stream dictionary
        self.data_streams: Dict[str, DataStream] = {x.name:x for x in data_streams}

class OfflineCollector(Collector):
    """Generic collector that stores only offline data streams.

    The offline collector allows the use of both __getitem__ and __next__
    to obtain the data pointer to a data stream to fetch the actual data.

    Attributes:
        data_streams (Dict[str, pymddt.OfflineDataStream]): A dictionary of the 
        data streams that its keys are the name of the data streams.

        global_timetrack (pd.DataFrame): A data frame that stores the time,
        data stream type, and data pointers to allow the iteration over
        all samples in all data streams efficiently.

    """

    def __init__(self, data_streams: Sequence[OfflineDataStream]) -> None:
        """Construct the ``OfflineCollector``.

        Args:
            data_streams (List[pymddt.OfflineDataStream]): A list of offline data streams.

        """
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
        self.global_timetrack: pd.DataFrame = pd.concat(dss_times, axis=0)

        # Sort by the time - only if there are more than 1 data stream
        if len(self.data_streams) > 1:
            self.global_timetrack.sort_values(by='time', )

        # For debugging purposes
        # self.global_timetrack.to_excel('test.xlsx', index=False)

    def __iter__(self) -> Iterator[DataSample]:
        """Generate an iterator of ``DataSample`` from the ``DataStream``.
        
        Returns:
            Iterator[DataSample]: Iterator of data samples.

        """
        self.index = 0
        return self

    def __next__(self) -> DataSample:
        """Obtain next data sample from the Iterator[DataSample] instance.

        Returns:
            DataSample: The next upcoming data sample.

        """
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

    def __getitem__(self, index: int) -> DataSample:
        """Index certain data point of the ``OfflineDataStream``.

        Args:
            index (int): The requested index.

        Returns:
            DataSample: The requested data sample.

        """
        if index >= len(self):
            raise IndexError
        else:

            # Determine which datastream is next
            next_ds_meta = self.global_timetrack.iloc[index]
            next_ds_pointer = next_ds_meta['ds_index']
            next_ds_name = next_ds_meta['ds_type']

            # Get the sample
            data_sample = self.data_streams[next_ds_name][next_ds_pointer]

            return data_sample

    def __len__(self) -> int:
        """Get size of ``OfflineDataStream``.

        Returns:
            int: The size of the data stream.

        """
        return len(self.global_timetrack)

class OnlineCollector(Collector):
    """TODO implementation."""

    ...
