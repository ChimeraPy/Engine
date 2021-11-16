"""Module focused on Tabular Data Stream implementation.

Contains the following classes:
    ``TabularDataStream``

"""

# Subpackage Management
__package__ = 'tabular'

from typing import Sequence, Optional
import collections
import tqdm

import pandas as pd

from pymmdt.data_stream import DataStream
from pymmdt.process import Process
from pymmdt.data_sample import DataSample

class TabularDataStream(DataStream):
    """Implementation of DataStream focused on Tabular data.

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
            data_columns: Sequence[str]=None,
            time_column: Optional[str]=None, 
        ):
        """Construct ``TabularDataStream.

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

        # Handling if there is no known information about the Tabular data
        if data_columns:
            self.data_columns = data_columns
        else:
            self.data_columns = []

        # Need to construct the timetrack 
        if time_column:
            timetrack = pd.TimedeltaIndex(self.data[time_column])
        else:
            timetrack = pd.TimedeltaIndex([]) 

        # Applying the super constructor with the timetrack
        super().__init__(name, timetrack)

    @classmethod
    def from_process_and_ds(
            cls, 
            process: Process, 
            in_ds: DataStream,
            verbose: bool = False
        ):
        """Class method to construct data stream from an applied process to a data stream.

        Args:
            process (Process): the applied process.
            in_ds (DataStream): the incoming data stream to be processed.

        Returns:
            self (TabularDataStream): the generated data stream.

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
        """Get indexed data sample from ``TabularDataStream``.

        Args:
            index (int): The index requested.

        Returns:
            DataSample: The indexed data sample from the data stream.

        """
        # First use the table index to find the data index
        time = self.timetrack.iloc[index].time
        data_index = self.timetrack.iloc[index]['ds_index']

        # Have to return a DataSample
        data_sample = DataSample(
            dtype=self.name,
            time=time,
            data=self.data.iloc[data_index][self.data_columns]
        )
        return data_sample 

    @classmethod
    def empty(cls, name):
        # Create empty items
        empty_df = pd.DataFrame()
        
        # Returning the construct object
        return cls(
            name=name, 
            data=empty_df
        )

    def append(self, sample: DataSample) -> None:
        """Append a data sample to the end of the ``TabularDataStream``.

        Args:
            sample (pymmdt.DataSample): The appending data sample.

        """

        # Add to the timetrack (cannot be inplace)
        self.timetrack = self.timetrack.append({
            'time': sample.time, 
            'ds_index': int(len(self.timetrack))
        }, ignore_index=True)

        # If this is the first sample added, set the data columns
        self.data_columns = list(sample.data.keys())

        # Then add it to the data body (cannot be inplace)
        self.data = self.data.append(sample.data, ignore_index=True)
