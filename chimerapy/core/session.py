# Package Management
__package__ = 'chimerapy'

# Built-in Imports
from typing import Union, Dict, Any
import uuid

# Third-party Imports
import pandas as pd
import numpy as np

# Internal Imports
# from chimerapy.utils.tools import PointCloudTransmissionFormat, PortableQueue,\
        # PointCloudTransmissionFormat
from chimerapy.utils.tools import PortableQueue
from chimerapy.utils.memory_manager import MemoryManager, get_memory_data_size

class Session:
    """Interface to the ``Writer``. 

    The ``Session`` class aids in formating and feed data to writing
    queue in a standardized manner. Aside from this, it a relatively
    simple class.

    """

    def __init__(
            self, 
            name:str,
            queue:PortableQueue,
            memory_manager:MemoryManager
        ) -> None:
        """``Session`` Constructor.

        Args:
            name (str): The name of the session.
            queue (PortableQueue): The queue where all formatted 
            data chunks are ``put``.

        """
        # Storing the writing queue and name for the session
        self.name = name
        self.queue = queue
        self.memory_manager = memory_manager

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

        # Put data in the writing queue
        data_chunk = {
            'uuid': uuid.uuid4(),
            'session_name': self.name,
            'name': name,
            'data': df,
            'dtype': 'image'
        }
        self.queue.put(data_chunk)

        # Accounting for memory usage
        self.memory_manager.add(data_chunk, which='writer')

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
        # images_df = df[[data_column, time_column]]
        images_df = df[[data_column, time_column]]
        images_df = images_df.rename(columns={data_column: 'frames', time_column: '_time_'})
        
        # Put data in the writing queue
        data_chunk = {
            'uuid': uuid.uuid4(),
            'session_name': self.name,
            'name': name,
            'data': images_df,
            'dtype': 'image'
        }
        self.queue.put(data_chunk)
        
        # Accounting for memory usage
        self.memory_manager.add(data_chunk, which='writer')
        
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
        video_df = df[[data_column, time_column]]
        video_df = video_df.rename(columns={data_column: 'frames', time_column: '_time_'})

        # Check that the data is valid (e.g. pd.NA)
        video_df = video_df.dropna()

        # If after cleaning the df it is empty, just skip it!
        if len(video_df) == 0:
            return None

        # Put data in the writing queue
        data_chunk = {
            'uuid': uuid.uuid4(),
            'session_name': self.name,
            'name': name,
            'data': video_df,
            'dtype': 'video'
        }
        self.queue.put(data_chunk)
        
        # Accounting for memory usage
        self.memory_manager.add(data_chunk, which='writer')
        
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

        # Put data in the writing queue
        data_chunk = {
            'uuid': uuid.uuid4(),
            'session_name': self.name,
            'name': name,
            'data': df.rename(columns={time_column: '_time_'}),
            'dtype': 'tabular'
        }
        self.queue.put(data_chunk)

        # Accounting for memory usage
        self.memory_manager.add(data_chunk, which='writer')

    # def add_point_clouds(
    #         self,
    #         name:str,
    #         df:pd.DataFrame,
    #         time_column:str='_time_',
    #         data_column:str='pcd'
    #     ) -> None:
    #     """Log point clouds stored in a pd.DataFrame.

    #     Args:
    #         name (str): Name of the point cloud's entry.
    #         df (pd.DataFrame): The data frame containing the point clouds.
    #         time_column (str): The name of the time column.
    #         data_column (str): The name of the data column containing 
    #         the point clouds.

    #     """

    #     # If the data is empty, skip it
    #     if len(df) == 0:
    #         return None

    #     # Renaming and extracting only the content we want
    #     pcd_df = df[[data_column, time_column]]
    #     pcd_df = pcd_df.rename(columns={data_column: 'unsafe_pcd', time_column: '_time_'})

    #     # Making PointClouds pickeable https://github.com/isl-org/Open3D/issues/218#issuecomment-923016145
    #     # https://github.com/isl-org/Open3D/issues/218#issuecomment-923016145
    #     pcd_df['pcd'] = pcd_df.apply(lambda x: PointCloudTransmissionFormat(x.unsafe_pcd), axis=1)
    #     pcd_df = pcd_df.drop(['unsafe_pcd'], axis=1)
        
    #     # Put data in the writing queue
    #     data_chunk = {
    #         'uuid': uuid.uuid4(),
    #         'session_name': self.name,
    #         'name': name,
    #         'data': pcd_df,
    #         'dtype': 'point_cloud'
    #     }
    #     self.queue.put(data_chunk)
        
    #     # Accounting for memory usage
    #     self.memory_manager.add(data_chunk)
