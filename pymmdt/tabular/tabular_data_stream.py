"""Module focused on Tabular Data Stream implementation.

Contains the following classes:
    ``TabularDataStream``

"""

# Subpackage Management
__package__ = 'tabular'

from typing import Union, Optional, Tuple
import collections
import tqdm
import pathlib

import pandas as pd

from pymmdt.data_stream import DataStream
from pymmdt.process import Process

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
            time_column: Optional[str]=None, 
        ):
        """Construct ``TabularDataStream.

        Args:
            name (str): The name of the data stream.

            data (pd.DataFrame): The loaded Tabular data in pd.DataFrame form.

            time_column (str): The column within the data that has the 
            time data.

        """

        # Need to construct the timetrack 
        if time_column:
            # Convert the time column to standard to column name of ``_time_``
            data['_time_'] = pd.TimedeltaIndex(data[time_column])
            
            # Store the timetrack
            timetrack = data['_time_']

        else:
            timetrack = pd.TimedeltaIndex([]) 
        
        # Storing the data and which columns to find it
        self.data = data

        # Applying the super constructor with the timetrack
        super().__init__(name, timetrack)

    @classmethod
    def from_process_and_ds(
            cls, 
            name: str,
            process: Process, 
            in_ds: DataStream,
            verbose: bool = False,
            cache: Optional[pathlib.Path]=None
        ):
        """Class method to construct data stream from an applied process to a data stream.

        Args:
            process (Process): the applied process.
            in_ds (DataStream): the incoming data stream to be processed.

        Returns:
            self (TabularDataStream): the generated data stream.

        """
        # Before running this long operation, determine if there is a 
        # saved version of the data
        if cache and cache.exists():
            # Load the data and construct the TabularDataStream
            data = pd.read_csv(cache)
            ds = cls(
                name=name,
                data=data,
                time_column='time'
            )
            return ds

        # Create data variable that will later be converted to a DataFrame
        data_store = collections.defaultdict(list)
        data_columns = set()

        # Iterate over all samples within the data stream
        for x, t in tqdm.tqdm(in_ds, total=len(in_ds), disable=verbose):

            # Process the sample and obtain the output
            y = process.step(x) 

            # If the output is None or an empty element, skip this time entry
            if type(y) == type(None):
                continue

            # Decompose the Data Sample
            data_store['_time_'].append(t)

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

        # If there is a cache request, save the data to the specific path
        if cache and not cache.exists():
            df.to_csv(str(cache), index=False)

        # Returning the construct object
        return cls(name=name, data=df, time_column='time') 
    
    @classmethod
    def empty(cls, name):
        # Create empty items
        empty_df = pd.DataFrame()
        
        # Returning the construct object
        return cls(
            name=name, 
            data=empty_df
        )

    def get(self, start_time: pd.Timedelta, end_time: pd.Timedelta) -> pd.DataFrame:
        
        # Generate mask for the window data
        after_start_time = self.timetrack['time'] >= start_time
        before_end_time = self.timetrack['time'] < end_time
        time_window_mask = after_start_time & before_end_time

        # Extract the data
        data = self.data[time_window_mask]

        # Return the data sample
        return data

    def append(self, timestamp: pd.Timedelta, sample: Union[pd.DataFrame, pd.Series]) -> None:
        """Append a data sample to the end of the ``TabularDataStream``.

        Args:
            sample (pd.Series): The appending data sample.

        """

        # Add to the timetrack (cannot be inplace)
        self.timetrack = self.timetrack.append({
            'time': timestamp, 
            'ds_index': int(len(self.timetrack))
        }, ignore_index=True)
        self.timetrack['ds_index'] = self.timetrack['ds_index'].astype(int)

        # Then add it to the data body (cannot be inplace)
        self.data = self.data.append(sample, ignore_index=True)
