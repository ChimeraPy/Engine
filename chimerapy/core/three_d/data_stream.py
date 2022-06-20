# Subpackage Management
__package__ = 'three_d'

# Built-in Imports
from typing import Union, Tuple, Optional
import multiprocessing as mp
import pathlib

# Third-party Imports
import pandas as pd
import open3d as o3d

# Internal Imports
from chimerapy.core.data_stream import DataStream
from chimerapy.utils.tools import PointCloudTransmissionFormat

class PointCloudDataStream(DataStream):

    def __init__(self, 
            name:str, 
            point_cloud_dir:Optional[Union[pathlib.Path, str]]=None, 
        ) -> None:

        # Ensure that point_cloud_dir is pathlib
        if isinstance(point_cloud_dir, str):
            point_cloud_dir = pathlib.Path(point_cloud_dir)

        # Save the parameters
        self.name = name
        self.point_cloud_dir = point_cloud_dir
        
        # First, determine if the data stream is empty or not
        if self.point_cloud_dir.exists():

            # This is the "reading" mode
            self.mode = "reading"

            # Create the path of the timestamps
            timestamps = self.point_cloud_dir / 'timestamps.csv'
            
            # Ensure that the path is a directory
            assert self.point_cloud_dir.is_dir() and timestamps.exists(), \
                f"point_cloud_dir parameter should be a directory with a ``timestamps.csv`` file: {self.point_cloud_dir}"

            # Load the timeline
            self.timeline = pd.read_csv(timestamps)

        else:

            # This is the "writing" mode
            self.mode = "writing"

            # Create an empty timeline
            self.timeline = pd.TimedeltaIndex([])

        # Apply the super constructor
        super().__init__(name, self.timeline)

    @classmethod
    def empty(cls):
        raise NotImplementedError

    def __len__(self):
        return len(self.timeline)

    def set_start_time(self, start_time:pd.Timedelta):

        # Get the first element of the self.data
        initial_start_time = self.timeline['_time_'][0]

        # Then update the '_time_' column
        self.timeline['_time_'] += (start_time - initial_start_time)
       
        # Update the timetrack
        self.update_timetrack()

    def shift_start_time(self, diff_time:pd.Timedelta):

        # First, update the self.data['_time_']
        self.timeline['_time_'] += diff_time

        # Update the timetrack
        self.update_timetrack()

    def update_timetrack(self):

        # Extract the timeline and convert it to timetrack
        timeline = self.timeline['_time_']
        self.make_timetrack(timeline)

    def load_pcd(self, row:pd.Series):
        
        # Create the path to the point cloud file
        pcd_filepath = self.point_cloud_dir / f"{row['idx']}.ply"

        # Load the point cloud file
        unsafe_pcd = o3d.io.read_point_cloud(str(pcd_filepath))

        # Prepare pcd to be put inside a queue
        pcd = PointCloudTransmissionFormat(unsafe_pcd)

        return pcd

    def get_start_end(self, start_time: pd.Timedelta, end_time: pd.Timedelta) -> pd.DataFrame:
        assert end_time > start_time, "``end_time`` should be greater than ``start_time``."
        assert self.mode == "reading", "``get`` currently works in ``reading`` mode."

        # Generate mask for the window data
        after_start_time = self.timetrack['time'] >= start_time
        before_end_time = self.timetrack['time'] < end_time
        time_window_mask = after_start_time & before_end_time
        
        # Convert the time_window_mask to have the data indx
        data_index = self.timetrack[time_window_mask].ds_index

        df = self.timeline.iloc[data_index]
        df['pcd'] = df.apply(lambda x: self.load_pcd(x), axis=1)

        return df
