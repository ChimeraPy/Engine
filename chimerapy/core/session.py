# Package Management
__package__ = 'chimerapy'

# Built-in Imports
from typing import Union, Dict, Any
import uuid
import multiprocessing as mp

# Third-party Imports
import pandas as pd
import numpy as np

# Internal Imports
from chimerapy.core.tools import get_memory_data_size, PortableQueue

class Session:
    """Interface to the ``Logger``. 

    The ``Session`` class aids in formating and feed data to logging 
    queue in a standardized manner. Aside from this, it a relatively
    simple class.

    """

    def __init__(
            self, 
            name:str,
            logging_queue:PortableQueue,
        ) -> None:
        """``Session`` Constructor.

        Args:
            name (str): The name of the session.
            logging_queue (PortableQueue): The queue where all formatted 
            data chunks are ``put``.

        """
        # Storing the logging queue and name for the session
        self.name = name
        self.logging_queue = logging_queue
        self.runner = None

    def set_runner(self, runner:'Runner'):
        """Set the ``SingleRunner`` or ``GroupRunner`` to the session.

        This is important for helping the runner in tracking the memory
        inside the logging queue. It provides the runner required in 
        ``record_memory_usage`` method.

        Args:
            runner ('Runner'): The runner that the session is working 
            for. Mostly correlated to the runner's ``Pipeline``.
        """
        self.runner = runner

    def record_memory_usage(self, data_chunk: Dict[str, Any]):
        """Record the memory usage of newly ``put`` data in logging queue.

        Args:
            data_chunk (Dict[str, Any]): The recently ``put`` data chunk.
        """

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
        self.record_memory_usage(data_chunk)

    def add_images(
            self,
            name:str,
            df:pd.DataFrame,
            time_column:str='_time_',
            data_column:str='frames'
        ) -> None:
        """Log images stored in a pd.DataFrame.

        Args:
            name (str): Name of the images' entry.
            df (pd.DataFrame): The data frame containing the images.
            time_column (str): The name of the time column.
            data_column (str): The name of the data column containing 
            the images.

        """
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
        self.record_memory_usage(data_chunk)
        
    def add_video(
            self,
            name:str,
            df:pd.DataFrame,
            time_column:str='_time_',
            data_column:str='frames'
        ) -> None:
        """Log frames to form a video.

        Args:
            name (str): Name of the video's entry.
            df (pd.DataFrame): Data frame with video frames.
            time_column (str): The name of the time column.
            data_column (str): The name of the data column contanting
            the video's frames.

        """
        # If the data is empty, skip it
        if len(df) == 0:
            return None

        # Renaming and extracting only the video content
        video_df = df[[data_column, time_column]].copy()
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
        self.record_memory_usage(data_chunk)
        
    def add_tabular(
            self, 
            name:str,
            data:Union[pd.Series, pd.DataFrame, Dict],
            time_column:str='_time_'
        ) -> None:
        """Log tabular data.

        Args:
            name (str): Name of the tabular data.
            data (Union[pd.Series, pd.DataFrame, Dict]): Tabular data.
            time_column (str): Name of the time column.

        """
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
        self.record_memory_usage(data_chunk)
