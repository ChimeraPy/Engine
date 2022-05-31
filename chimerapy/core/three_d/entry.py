# Package Management
__package__ = 'three_d'

# Built-in Imports
import pathlib
import os
import gc

# Third-party Imports
import pandas as pd
import open3d as o3d

# Internal Imports
from chimerapy.core.entry import Entry

# Important information about point clouds
# https://towardsdatascience.com/how-to-automate-3d-point-cloud-segmentation-and-clustering-with-python-343c9039e4f5
# https://towardsdatascience.com/guide-to-real-time-visualisation-of-massive-3d-point-clouds-in-python-ea6f00241ee0

class PointCloudEntry(Entry):

    def __init__(
        self, 
        dir:pathlib.Path,
        name:str,
        ):

        # Storing input parameters
        self.dir = dir
        self.name = name
                
        # Setting initial values
        self.unsaved_changes = pd.DataFrame(columns=['_time_', 'data'])
        self.num_of_total_changes = 0

        # For image entry, need to save to a new directory
        self.save_loc = self.dir / self.name
        os.mkdir(self.save_loc)

    def flush(self):
        """Flush out unsaved changes to memory.

        For ``PointCloudEntry``, a folder is created, the images are stored
        inside, and finally a csv file is saved with the timestamps 
        pertaining to each image.

        """
        # Depending on different type of inputs, we should save data differently
        # For images, we just need to save each image logged
        # Save the unsaved changes
        for i, row in self.unsaved_changes.iterrows():
            # Storing the image
            filepath = self.save_loc / f"{self.num_of_total_changes+i}.ply"
            o3d.io.write_point_cloud(
                filename=str(filepath), 
                pointcloud=row.pcd.create_pointcloud()
            )

            # Storing timestamp data for all images
            meta_df = pd.DataFrame(
                {'_time_': [row._time_], 'idx': [self.num_of_total_changes+i]}
            )
            if self.num_of_total_changes == 0 and i == 0:
                meta_df.to_csv(self.save_loc / "timestamps.csv", mode='a', index=False, header=True)
            else: 
                meta_df.to_csv(self.save_loc / "timestamps.csv", mode='a', index=False, header=False)

        # Update the counter and clear out the unsaved items
        self.num_of_total_changes += len(self.unsaved_changes)
        self.unsaved_changes = self.unsaved_changes.iloc[0:0]

        # Collect the garbage
        del self.unsaved_changes
        self.unsaved_changes = pd.DataFrame(columns=['_time_', 'data'])
        gc.collect()

    def close(self):

        # Apply the last changes and that's it!
        self.flush()
