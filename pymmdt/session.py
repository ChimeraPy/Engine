"""Module focused on the ``Session`` implementation.

Contains the following classes:
    ``Session``
"""

# Package Management
__package__ = 'pymmdt'

# Built-in Imports
from typing import Union, Dict, Optional, Any
import collections
import pathlib
import os
import shutil

# Third-party Imports
import pandas as pd
import numpy as np
import cv2

# Internal Imports
from .data_stream import DataStream

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
            log_dir: Union[pathlib.Path, str],
            experiment_name: str,
            # clear_dir: bool = False
        ) -> None:
        """``Session`` Constructor.

        Args:
            log_dir (Union[pathlib.Path, str]): The directory to store information from the session.

            experiment_name (str): The name of the experiment, typically just using the name of the 
            participant/user.

            clear_dir (bool): To clear the sessions' directory.

        """
        # Converting the str to pathlib.Path
        if isinstance(log_dir, str):
            log_dir = pathlib.Path(log_dir)

        # Create the filepath of the experiment
        self.session_dir = log_dir / experiment_name
        # self.clear_dir = clear_dir

        # Create the folder if it doesn't exist 
        if not log_dir.exists():
            os.mkdir(log_dir)

        # # Delete if clear_dir is True
        # if self.clear_dir and self.session_dir.exists():
        #     # Determine the number of files and folders to delete
        #     num_of_files = sum([len(files) for r, d, files in os.walk(self.session_dir)])
        #     num_of_dir = sum([len(d) for r, d, files in os.walk(self.session_dir)])

        #     # If there are files to delete, ask first!
        #     if num_of_files != 0 or num_of_dir != 0:
        #         print(f"Deleting {num_of_files} files and {num_of_dir} folders from {self.session_dir}.")
        #         if input("Is this okay (y/n)? ").lower() == "y":
        #         # if True:
        #             shutil.rmtree(self.session_dir)
        #             print("Deleted directory")
        #         else:
        #             print("Didn't delete directory")

        # Create the experiment dir
        if not self.session_dir.exists():
            os.mkdir(self.session_dir)

        # Create a record to the data
        self.records = {}
        self.num_of_changes = collections.defaultdict(int)
        self.variables = {}
        self.streams = [] 
       
        # Keeping record of subsessions and elements changed
        self.subsessions = []
        self.changed = []

    def __getitem__(self, item:str):
        assert isinstance(item, str), f"{item} should be ``str``."
        return self.records[item]
    
    def create_stream(self, data_stream:DataStream):
        assert isinstance(data_stream, DataStream)
        self.records[data_stream.name] = data_stream
        self.streams.append(data_stream.name)
        
    def add_image(
            self, 
            name:str, 
            data:np.ndarray, 
            timestamp:Optional[pd.Timedelta]=None,
        ):
        # Detecting if this is the first time
        if name not in self.variables:
            self.variables[name] = 'image'

        assert self.variables[name] == 'image'

        # Storing the data
        self.records[name] = data
        
        # Keeping track of the changed elements
        self.changed.append(
            {'dtype': 'image', 'name': name, 'data': data, 'time': timestamp}
        )

    def add_tabular(
            self, 
            name:str,
            data:Union[pd.Series, pd.DataFrame, Dict],
            timestamp:Optional[pd.Timedelta]=None,
        ):
        # Detecting if this is the first time
        if name not in self.variables:
            self.variables[name] = 'tabular'

        assert self.variables[name] == 'tabular'

        # Convert the data into a pd.Series
        if isinstance(data, dict):
            data = pd.Series(data)

        if isinstance(data, pd.Series):
            df = data.to_frame().T
        elif isinstance(data, pd.DataFrame):
            df = data
        else:
            raise RuntimeError(f"{data} should be pd.Series, pd.DataFrame or dict")

        # Storing the data frame
        self.records[name].append(timestamp, df)

        # Keeping track of the changed elements
        self.changed.append(
            {'dtype': 'tabular', 'name': name, 'data': df, 'time': timestamp}
        )
 
    def create_subsession(self, name):

        # Construct the subsession log_dir
        # and experiment_name
        log_dir = self.session_dir
        experiment_name = name
        
        # Create a new session instance
        subsession = self.__class__(
            log_dir,
            experiment_name,
            # self.clear_dir
        )

        # Store the subsession to this session
        self.subsessions.append(subsession)

        # Return the new session instance
        return self.subsessions[-1]

    def flush(self) -> None:
     
        # For each change, process the change according to its type
        for change in self.changed:

            # For images, save the image to a simple .jpg
            if change['dtype'] == 'image':
                # Save the image
                filepath = self.session_dir / (change['name'] + '.jpg')
                cv2.imwrite(str(filepath), change['data'])

            # For tabular, append it to the .csv file
            elif change['dtype'] == 'tabular':
                # Extend the tabular file
                filepath = self.session_dir / (change['name'] + '.csv')

                # If this is the first time, we want the headers
                if self.num_of_changes[change['name']] == 0:
                    change['data'].to_csv(filepath, mode='a', index=False, header=True)
                else:
                    change['data'].to_csv(filepath, mode='a', index=False, header=False)

            # Tracking the number of changes
            self.num_of_changes[change['name']] += 1

        # After processing everything, clear out the tracked changes 
        self.changed.clear()

    def close(self) -> None:
        """Close session.

        """
        # Flush out the session data
        self.flush()

        # Close all the subsessions
        for session in self.subsessions:
            session.close()
