"""Module focused on the ``Session`` implementation.

Contains the following classes:
    ``Session``
"""

# Package Management
__package__ = 'pymmdt'

# Built-in Imports
from typing import Union, Dict, Optional, Any
import pathlib
import os
import threading
import queue
import time
import json
import collections

# Third-party Imports
import pandas as pd
import numpy as np

# Internal Imports
from .tools import threaded
from .data_stream import DataStream
from .video import VideoDataStream, VideoEntry
from .tabular import TabularDataStream, TabularEntry, ImageEntry

class Session:
    """Data Storage that contains the latest version of all data types.

    Attributes:
        records (Dict[str, DataSample]): Stores the latest version of a ``data_type`` sample
        or the output of a process.

    Todo:
        * Allow the option to store the intermediate samples stored in
        the session.

    """

    def __init__(
            self, 
            log_dir:Union[pathlib.Path, str],
            experiment_name:str,
        ) -> None:
        """``Session`` Constructor.

        Args:
            log_dir (Union[pathlib.Path, str]): The directory to store information from the session.

            experiment_name (str): The name of the experiment, typically just using the name of the 
            participant/user.

        """
        # Converting the str to pathlib.Path
        if isinstance(log_dir, str):
            log_dir = pathlib.Path(log_dir)

        # Create the filepath of the experiment
        self.experiment_name = experiment_name
        self.session_dir = log_dir / experiment_name

        # Create the folder if it doesn't exist 
        if not log_dir.exists():
            os.mkdir(log_dir)

        # Create the experiment dir
        if not self.session_dir.exists():
            os.mkdir(self.session_dir)

        # Create a JSON file with the session's meta
        self.meta_data = {'id': experiment_name, 'subsessions': [], 'records': {}}
        self._save_meta_data()

        # Create a record to the data
        self.records = {}
       
        # Keeping record of subsessions and elements changed
        self.subsessions = []

        # Keeping track of the number of logged information
        self.num_of_logged_data = 0

    def __getitem__(self, item:str) -> Any:
        """Get the item given the name of the ``Entry``.

        Args:
            item (str): The name of the requested name.

        Returns:
            Any: The last sample from that specified entry. To obtain
            the entire data stream, use ``session.records[item]``.
        """
        assert isinstance(item, str), f"{item} should be ``str``."

        # extract the entry
        entry = self.records[item]

        # Return the entry
        return entry

    def _save_meta_data(self):
        with open(self.session_dir / 'meta.json', "w") as json_file:
            json.dump(self.meta_data, json_file)
    
    def set_thread_exit(self, thread_exit:threading.Event):
        self._thread_exit = thread_exit
    
    def set_logging_queue(self, logging_queue:queue.Queue):
        self.logging_queue = logging_queue
    
    def add_image(
            self, 
            name:str, 
            data:np.ndarray, 
            timestamp:pd.Timedelta=None,
        ) -> None:
        """Log an image to an specified entry at an optional timestamp.

        Args:
            name (str): The name of the ``Entry`` to store it.

            data (np.ndarray): The image to store.

            timestamp (pd.Timedelta): The optional timestamp
            to tag the image with.

        """

        # Create a pd.DataFrame for the data
        df = pd.DataFrame({'frames': [data], '_time_': [timestamp]})

        # Put data in the logging queue
        data_chunk = {
            'name': name,
            'data': df,
            'dtype': 'image'
        }
        self.logging_queue.put(data_chunk.copy())

    def add_images(
            self,
            name:str,
            df:pd.DataFrame,
            time_column:str='_time_',
            data_column:str='frames'
        ) -> None:
        
        # Put data in the logging queue
        data_chunk = {
            'name': name,
            'data': df.rename(columns={data_column: 'frames', time_column: '_time_'}),
            'dtype': 'image'
        }
        self.logging_queue.put(data_chunk.copy())

    def add_video(
            self,
            name:str,
            df:pd.DataFrame,
            time_column:str='_time_',
            data_column:str='frames'
        ) -> None:
        # Put data in the logging queue
        data_chunk = {
            'name': name,
            'data': df.rename(columns={data_column: 'frames', time_column: '_time_'}),
            'dtype': 'video'
        }
        self.logging_queue.put(data_chunk.copy())

    def add_tabular(
            self, 
            name:str,
            data:Union[pd.Series, pd.DataFrame, Dict],
            time_column:str='_time_'
        ) -> None:

        # Convert the data to a DataFrame
        if isinstance(data, pd.Series):
            df = data.to_frame()
        elif isinstance(data, dict):
            df = pd.DataFrame(data)
        elif isinstance(data, pd.DataFrame):
            df = data
        else:
            raise TypeError(f"{type(data)} is an invalid type for ``add_tabular``.")

        # Put data in the logging queue
        data_chunk = {
            'name': name,
            'data': df.rename(columns={time_column: '_time_'}),
            'dtype': 'tabular'
        }
        self.logging_queue.put(data_chunk.copy())
 
    def create_subsession(self, name: str) -> 'Session':
        """create_subsession.

        Args:
            name (str): name

        Returns:
            'Session':
        """

        # Construct the subsession log_dir
        # and experiment_name
        log_dir = self.session_dir
        experiment_name = name
        
        # Create a new session instance
        subsession = self.__class__(
            log_dir,
            experiment_name,
        )

        # Store the subsession to this session
        self.subsessions.append(subsession)
        self.meta_data['subsessions'].append(experiment_name)
        self._save_meta_data()

        # Return the new session instance
        return self.subsessions[-1]

    def flush(self, data:Dict):
            
        dtype_to_class = {
            'tabular': TabularEntry,
            'image': ImageEntry,
            'video': VideoEntry
        }

        # Detecting if this is the first time
        if data['name'] not in self.records.keys():

            # Selecting the class
            entry_cls = dtype_to_class[data['dtype']]

            # Creating the entry and recording in meta data
            self.records[data['name']] = entry_cls(self.session_dir, data['name'])
            entry_meta_data = {
                'dtype': data['dtype'],
                'start_time': str(data['data'].iloc[0]._time_),
                'end_time': str(data['data'].iloc[-1]._time_),
            }
            self.meta_data['records'][data['name']] = entry_meta_data
            self._save_meta_data()

            # Append the data to the new entry
            self.records[data['name']].append(data)
            self.records[data['name']].flush()

        # Not the first time
        else:

            # Test that the new data entry is valid to the type of entry
            assert isinstance(self.records[data['name']], dtype_to_class[data['dtype']]), \
                f"Entry Type={self.records[data['name']]} should match input data dtype {data['dtype']}"

            # Need to update the end_time for meta_data
            if len(data['data']) > 0:
                end_time_stamp = str(data['data'].iloc[-1]._time_)
                self.meta_data['records'][data['name']]['end_time'] = end_time_stamp 

            # If everything is good, add the change to the track history
            # Append the data to the new entry
            self.records[data['name']].append(data)
            self.records[data['name']].flush()

    @threaded
    def load_data_to_log(self):

        # Continuously check if there are data to log and save
        while True: 

            # First check if there is an item in the queue
            if self.logging_queue.qsize() != 0:

                # Get the data frome the queue
                data = self.logging_queue.get(block=True)
                
                # Then process the data
                self.flush(data)

            else:
                time.sleep(0.5)

            # Break Condition
            if self._thread_exit.is_set() and self.logging_queue.qsize() == 0:
                break

    def close(self) -> None:

        # Then close all the entries
        for entry in self.records.values():
            entry.close()

        # Close all the subsessions
        for session in self.subsessions:
            session.close()
