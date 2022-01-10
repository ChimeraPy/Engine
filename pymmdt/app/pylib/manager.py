# Resource:
# https://stackoverflow.com/questions/64505389/cant-reference-existing-qml-elements-from-python

# Built-in Imports
import json
import collections

# Third-party Imports
import pandas as pd

# PyQt5 Imports
from PyQt5.QtCore import QTimer, QObject, pyqtProperty, pyqtSignal

# Interal Imports
from .dashboard_model import DashboardModel

class Manager(QObject):
    modelChanged = pyqtSignal()

    def __init__(self, args):
        super().__init__()

        # Store the CI arguments
        self.args = args

        # Keeping track of all the data in the logdir
        self.logdir_records = {}

        # Creating the used dashboard model
        self._dashboard_model = DashboardModel()

        # Apply the update to the meta data
        self.update()

        # Using a timer to periodically check for new data
        self.timer = QTimer()
        self.timer.setInterval(500) # 500 milliseconds = 0.5 second
        self.timer.timeout.connect(self.update)
        self.timer.start()

    @pyqtProperty(DashboardModel, notify=modelChanged)
    def dashboard_model(self):
        return self._dashboard_model

    def get_meta(self):
        # Obtain all the meta files
        root_meta = self.args.logdir / 'meta.json' 

        # If no meta, then provide error message through a page.
        if not root_meta.exists():
            return None

        # Else, get the initial and all other meta files
        with open(root_meta, 'r') as f:
            meta_data = json.load(f)

        # Check if there is any subsessions and get their meta
        total_meta = {'root': meta_data}
        for sub_id in meta_data['subsessions']:
            with open(self.args.logdir / sub_id / 'meta.json', 'r') as f:
                total_meta[sub_id] = json.load(f)

        return total_meta

    def update(self):

        # In the update function, we need to check the data stored in 
        # the logdir.
        new_logdir_records = self.get_meta()

        # If the new is different and not None, we need to update!
        if new_logdir_records and new_logdir_records != self.logdir_records:

            # Construct pd.DataFrame for the data
            # For each session data, store entries by modality
            entries = collections.defaultdict(list)
            for session_name, session_data in new_logdir_records.items():
                for entry_name, entry_data in session_data['records'].items():

                    # For now, skip any tabular data since we don't have a way to visualize
                    if entry_data['dtype'] == 'tabular':
                        continue

                    entries['user'].append(session_name)
                    entries['entry_name'].append(entry_name)
                    entries['dtype'].append(entry_data['dtype'])
                    entries['start_time'].append(entry_data['start_time'])
                    entries['end_time'].append(entry_data['end_time'])

            # Construct the dataframe
            self.entries = pd.DataFrame(dict(entries))

            # Add the data to the dashboard
            self._dashboard_model.update_data(self.entries)
            
            # Then overwrite the records
            self.logdir_records = new_logdir_records
