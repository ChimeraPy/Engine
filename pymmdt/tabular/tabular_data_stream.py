"""Module focused on Tabular Data Stream implementation.

Contains the following classes:
    ``OfflineTabularDataStream``

"""

# Subpackage Management
__package__ = 'tabular'

from typing import Sequence
import collections
import tqdm

import pandas as pd

from pymmdt.data_stream import OfflineDataStream
from pymmdt.process import Process
from pymmdt.data_sample import DataSample

class OfflineTabularDataStream(OfflineDataStream):
    """Implementation of Offline DataStream focused on Tabular data.

    Attributes:
        name (str): The name of the data stream.

        data (pd.DataFrame): The loaded Tabular data in pd.DataFrame form.

        data_columns (Sequence[str]): A list of string containing the name
        of the data columns to select from.

    """

    def __init__(
            self, 
            name: str, 
            data: pd.DataFrame, 
            time_column: str, 
            data_columns: Sequence[str]
        ):
        """Construct ``OffineTabularDataStream.

        Args:
            name (str): The name of the data stream.

            data (pd.DataFrame): The loaded Tabular data in pd.DataFrame form.

            time_column (str): The column within the data that has the 
            time data.

            data_columns (Sequence[str]): A list of string containing the name
            of the data columns to select from.

        """
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
            process (Process): the applied process.
            in_ds (OfflineTabularDataStream): the incoming data stream to be processed.

        Returns:
            self (OfflineTabularDataStream): the generated data stream.

        """
        # Testing conditions that cause problems
        if not hasattr(process, 'output'):
            raise AttributeError("classmethod: from_process_and_ds requires process to have 'output' parameter.")

        # Create data variable that will later be converted to a DataFrame
        data_store = collections.defaultdict(list)
        data_columns = set()

        # Iterate over all samples within the data stream
        for x in tqdm.tqdm(in_ds, total=len(in_ds), disable=verbose):

            # Process the sample and obtain the output
            y = process.forward(x) # DataSample

            # If the output is None or an empty element, skip this time entry
            if not y:
                continue

            # Decompose the Data Sample
            data_store['time'].append(x.time)

            # If there is multiple outputs (dict), we need to store them in 
            # separate columns.
            if isinstance(y.data, dict):

                # Storing the output data
                for y_key in y.data.keys():
                    data_store[y_key].append(y.data[y_key])
                    data_columns.add(y_key)

            else: # Else, just store the value in the generic 'data' column
                data_store['data'].append(y.data)
                data_columns.add('data')

        # Convert the data to a pd.DataFrame
        df = pd.DataFrame(data_store)

        # Returning the construct object
        return cls(name=str(process.output), data=df, time_column='time', data_columns=list(data_columns))

    def __getitem__(self, index: int) -> DataSample:
        """Get indexed data sample from ``OfflineTabularDataStream``.

        Args:
            index (int): The index requested.

        Returns:
            DataSample: The indexed data sample from the data stream.

        """
        # Have to return a DataSample
        data_sample = DataSample(
            dtype=self.name,
            time=self.timetrack.iloc[index]['time'],
            data=self.data.iloc[index]
        )
        return data_sample 
