# Subpackage Management
__package__ = 'tabular'

from typing import Union, Optional, Tuple
import collections
import tqdm
import pathlib

import pandas as pd

from chimerapy.core.data_stream import DataStream
from chimerapy.core.process import Process
from chimerapy.core.tools import get_windows

class TabularDataStream(DataStream):
    """Implementation of DataStream focused on Tabular data.

    Attributes:
        name (str): The name of the data stream.
        data (pd.DataFrame): The loaded Tabular data in pd.DataFrame form.
        data_columns (Sequence[str]): A list of string containing the \
            name of the data columns to select from.

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
            time_column (str): The column within the data that has the \
                time data.

        """

        # Need to construct the timetrack 
        if time_column:
            # Convert the time column to standard to column name of ``_time_``
            data['_time_'] = pd.TimedeltaIndex(data[time_column])
            
            # Store the timeline
            timeline = data['_time_']

        else:
            timeline = pd.TimedeltaIndex([]) 
        
        # Storing the data and which columns to find it
        self.data = data

        # Applying the super constructor with the timeline
        super().__init__(name, timeline)

    def __eq__(self, other:'TabularDataStream') -> bool:
        if isinstance(other, TabularDataStream):
            return self.data.equals(other.data)
        else:
            return False

    def __len__(self):
        return len(self.data)

    @classmethod
    def from_process_and_ds(
            cls, 
            name:str,
            process:Process, 
            in_ds:DataStream,
            verbose:bool = False,
            cache:Optional[pathlib.Path]=None,
            time_window_size:pd.Timedelta=pd.Timedelta(seconds=10)
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
        
        # Split samples based on the time window size
        start_time = in_ds.timetrack['time'][0]
        end_time = in_ds.timetrack['time'].iloc[-1]
        windows = get_windows(start_time, end_time, time_window_size)

        # Create an empty dataframe
        df = pd.DataFrame()

        # Iterate over all samples within the data stream
        for window in tqdm.tqdm(windows, disable=verbose):

            # Get data for that window
            data: pd.DataFrame = in_ds.get(window.start, window.end)

            # Process the sample and obtain the output
            output: Optional[pd.DataFrame] = process.step(data) 

            # If the output is None or an empty element, skip this time entry
            if type(output) == type(None):
                continue

            # else, there is data that we should append to the dataframe
            df = df.append(output)

        # If there is a cache request, save the data to the specific path
        if cache and not cache.exists():
            df.to_csv(str(cache), index=False)

        # Returning the construct object
        return cls(name=name, data=df, time_column='_time_') 
    
    @classmethod
    def empty(cls, name):
        """Create an empty Tabular data stream.

        Args:
            name: The name given to the empty tabular data stream.

        """
        # Create empty items
        empty_df = pd.DataFrame()
        
        # Returning the construct object
        return cls(
            name=name, 
            data=empty_df
        )

    def set_start_time(self, start_time:pd.Timedelta):

        # Get the first element of the self.data
        initial_start_time = self.data['_time_'][0]

        # Then update the '_time_' column
        self.data['_time_'] += (start_time - initial_start_time)
       
        # Update the timetrack
        self.update_timetrack()

    def shift_start_time(self, diff_time:pd.Timedelta):

        # First, update the self.data['_time_']
        self.data['_time_'] += diff_time

        # Update the timetrack
        self.update_timetrack()

    def update_timetrack(self):

        # Extract the timeline and convert it to timetrack
        timeline = self.data['_time_']
        self.make_timetrack(timeline)

    def get(
            self, 
            start_time: pd.Timedelta, 
            end_time: pd.Timedelta
        ) -> pd.DataFrame:
        """Get time window data from ``start_time`` to ``end_time``.

        Args:
            start_time (pd.Timedelta): Start of the time window.
            end_time (pd.Timedelta): End of the time window.

        Returns:
            pd.DataFrame: The tabular data ranging from the start to end.
        """
        assert end_time > start_time, "``end_time`` should be greater than ``start_time``."
        
        # Generate mask for the window data
        after_start_time = self.timetrack['time'] >= start_time
        before_end_time = self.timetrack['time'] < end_time
        time_window_mask = after_start_time & before_end_time

        # Convert the time_window_mask to have the data indx
        data_index = self.timetrack[time_window_mask].ds_index

        # Extract the data
        data = self.data.iloc[data_index]

        # Return the data sample
        return data

    def append(
            self, 
            append_data:Union[pd.DataFrame, pd.Series], 
            time_column:str='_time_'
        ) -> None:
        # The append_data needs to have timedelta_index column
        assert time_column in append_data.columns
        time_column_data = append_data[time_column]
        assert isinstance(time_column_data.iloc[0], pd.Timedelta), "time column should be ``pd.Timedelta`` objects."

        # Create data frame for the timeline
        append_timetrack = pd.DataFrame(
            {
                'time': time_column_data, 
                'ds_index': 
                    [x for x in range(len(time_column_data))]
            }
        )

        # Add to the timetrack (cannot be inplace)
        self.timetrack = self.timetrack.append(append_timetrack)
        self.timetrack['ds_index'] = self.timetrack['ds_index'].astype(int)

        # Then add it to the data body (cannot be inplace)
        self.data = self.data.append(append_data)
