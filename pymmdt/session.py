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

# Third-party Imports
import pandas as pd
import numpy as np

# Internal Imports
from .tools import threaded
from .data_stream import DataStream
from .video import VideoDataStream
from .tabular import TabularDataStream
from .entry import StreamEntry, PointEntry

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
            queue_max_size:Optional[int]=100
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
        self.session_dir = log_dir / experiment_name

        # Create the folder if it doesn't exist 
        if not log_dir.exists():
            os.mkdir(log_dir)

        # Create the experiment dir
        if not self.session_dir.exists():
            os.mkdir(self.session_dir)

        # Create a record to the data
        self.records = {}
       
        # Keeping record of subsessions and elements changed
        self.subsessions = []

        # Create a queue and thread for I/O logging in separate thread
        self.logging_queue = queue.Queue(maxsize=queue_max_size)
        self.logging_thread = self.load_data_to_log()
        self._thread_exit = threading.Event()
        self._thread_exit.clear() # Set as False in the beginning

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

        # Then ask the entry for the latest sample
        last_sample = entry.get_last_sample()
        return last_sample
    
    def create_stream(self, data_stream:DataStream) -> None:
        """create_stream.

        Args:
            data_stream (DataStream): The data stream to save in the 
            session.

        """
        assert isinstance(data_stream, DataStream)
        assert data_stream.name not in self.records.keys()

        # Create an entry
        entry = StreamEntry(
            dir=self.session_dir,
            name=data_stream.name,
            stream=data_stream,
        )
       
        # Append the stream
        self.records[entry.name] = entry
        
    def add_image(
            self, 
            name:str, 
            data:np.ndarray, 
            timestamp:Optional[pd.Timedelta]=None,
        ) -> None:
        """Log an image to an specified entry at an optional timestamp.

        Args:
            name (str): The name of the ``Entry`` to store it.

            data (np.ndarray): The image to store.

            timestamp (Optional[pd.Timedelta]): The optional timestamp
            to tag the image with.

        """

        # Put data in the logging queue
        data_chunk = {
            'name': name,
            'data': data,
            'timestamp': timestamp,
            'dtype': 'image'
        }
        self.logging_queue.put(data_chunk)

    def add_tabular(
            self, 
            name:str,
            data:Union[pd.Series, pd.DataFrame, Dict],
            timestamp:Optional[pd.Timedelta]=None,
        ) -> None:
        """add_tabular.

        Args:
            name (str): name
            data (Union[pd.Series, pd.DataFrame, Dict]): data
            timestamp (Optional[pd.Timedelta]): timestamp

        Returns:
            None:
        """

        # Ensure that the data is a dict
        if isinstance(data, (pd.Series, pd.DataFrame)):
            data_dict = data.to_dict()
        elif isinstance(data, dict):
            data_dict = data
        else:
            raise RuntimeError(f"{data} should be a dict, pd.DataFrame, or pd.Series")

        # Put data in the logging queue
        data_chunk = {
            'name': name,
            'data': data_dict,
            'timestamp': timestamp,
            'dtype': 'tabular'
        }
        self.logging_queue.put(data_chunk)
 
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

        # Return the new session instance
        return self.subsessions[-1]

    def flush(self, data: Dict):

        # Tabular
        # Detecting if this is the first time
        if data['name'] not in self.records.keys():

            # Create an entry
            entry = PointEntry(dir=self.session_dir, **data)

            # Store the entry to the records
            self.records[entry.name] = entry

        # Not the first time
        else:

            # Extended the entry with the same name
            entry = self.records[data['name']]

            # Test that the new data entry is valid to the type of entry
            assert entry.dtype == data['dtype'], "Entry dtype should match the input data type"

            # If everything is good, add the change to the track history
            # of the entry
            entry.append(
                data=data['data'],
                timestamp=data['timestamp']
            )

    @threaded
    def load_data_to_log(self):

        # Continuously check if there are data to log and save
        while True:

            # If exit event, exit the thread
            if self._thread_exit.is_set():
                break

            # Getting data from queue
            while True:
                # Instead of just getting stuck, we need to check
                # if the process is supposed to stop.
                # If not, keep trying to get the data into the queue
                if self._thread_exit.is_set():
                    data = 'END'
                    break
                
                elif self.logging_queue.qsize() != 0:
                    # Get the data frome the queue
                    data = self.logging_queue.get()
                    break
                
                else:
                    time.sleep(0.1)
                    continue

            # If end message, end the thread
            if data == 'END':
                break

            # Then process the data
            self.flush(data)

    def close(self) -> None:
        """close.

        Args:

        Returns:
            None:
        """

        # Stop the thread and wait until complete
        self._thread_exit.set()
        self.logging_thread.join()
        
        # Then close all the entries
        for entry in self.records.values():
            entry.close()

        # Close all the subsessions
        for session in self.subsessions:
            session.close()
