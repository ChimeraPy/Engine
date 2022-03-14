"""Module focused on the ``Session`` implementation.

Contains the following classes:
    ``Session``
"""

# Package Management
__package__ = 'pymmdt'

# Built-in Imports
from typing import Union, Dict, Optional, Any
import uuid
import multiprocessing as mp
import json
import collections

# Third-party Imports
import pandas as pd
import numpy as np

# Internal Imports
from pymmdt.core.tools import get_memory_data_size

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
            name:str,
            logging_queue:mp.Queue,
        ) -> None:
        """``Session`` Constructor.

        Args:
            log_dir (Union[pathlib.Path, str]): The directory to store information from the session.

            experiment_name (str): The name of the experiment, typically just using the name of the 
            participant/user.

        """

        # Storing the logging queue and name for the session
        self.name = name
        self.logging_queue = logging_queue
        self.runner = None

    def set_runner(self, runner:'Runner'):
        self.runner = runner

    def record_memory_useage(self, data_chunk: Dict[str, Any]):

        if type(self.runner) != type(None):
            self.runner.logging_queue_memory_chunks[data_chunk['uuid']] = get_memory_data_size(data_chunk)
    
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

        # If the data is not a numpy array, raise error
        if type(data) == type(None):
            return None
        elif not isinstance(data, np.ndarray):
            raise TypeError(f"Image data is {type(data)}, not the expected np.ndarray.")

        # Create a pd.DataFrame for the data
        df = pd.DataFrame({'frames': [data], '_time_': [timestamp]})

        # Put data in the logging queue
        data_chunk = {
            'uuid': uuid.uuid4(),
            'session_name': self.name,
            'name': name,
            'data': df,
            'dtype': 'image'
        }
        self.logging_queue.put(data_chunk.copy())

        # Accounting for memory usage
        self.record_memory_useage(data_chunk)

    def add_images(
            self,
            name:str,
            df:pd.DataFrame,
            time_column:str='_time_',
            data_column:str='frames'
        ) -> None:

        # If the data is empty, skip it
        if len(df) == 0:
            return None

        # Renaming and extracting only the content we want
        # images_df = df[[data_column, time_column]].copy()
        images_df = df[[data_column, time_column]]
        images_df = images_df.rename(columns={data_column: 'frames', time_column: '_time_'})
        
        # Put data in the logging queue
        data_chunk = {
            'uuid': uuid.uuid4(),
            'session_name': self.name,
            'name': name,
            'data': images_df,
            'dtype': 'image'
        }
        self.logging_queue.put(data_chunk.copy())
        
        # Accounting for memory usage
        self.record_memory_useage(data_chunk)
        
    def add_video(
            self,
            name:str,
            df:pd.DataFrame,
            time_column:str='_time_',
            data_column:str='frames'
        ) -> None:
        
        # If the data is empty, skip it
        if len(df) == 0:
            return None

        # Renaming and extracting only the video content
        # video_df = df[[data_column, time_column]].copy()
        video_df = df[[data_column, time_column]]
        video_df = video_df.rename(columns={data_column: 'frames', time_column: '_time_'})

        # Put data in the logging queue
        data_chunk = {
            'uuid': uuid.uuid4(),
            'session_name': self.name,
            'name': name,
            'data': video_df,
            'dtype': 'video'
        }
        self.logging_queue.put(data_chunk.copy())
        
        # Accounting for memory usage
        self.record_memory_useage(data_chunk)
        
    def add_tabular(
            self, 
            name:str,
            data:Union[pd.Series, pd.DataFrame, Dict],
            time_column:str='_time_'
        ) -> None:

        # If the data is empty, skip it
        if len(data) == 0:
            return None

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
            'uuid': uuid.uuid4(),
            'session_name': self.name,
            'name': name,
            'data': df.rename(columns={time_column: '_time_'}),
            'dtype': 'tabular'
        }
        self.logging_queue.put(data_chunk.copy())

        # Accounting for memory usage
        self.record_memory_useage(data_chunk)
