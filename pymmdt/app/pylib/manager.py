# Resource:
# https://stackoverflow.com/questions/64505389/cant-reference-existing-qml-elements-from-python

# Built-in Imports
import json
import collections

# Third-party Imports
import pandas as pd

# PyQt5 Imports
from PyQt5.QtCore import QTimer, QObject, pyqtProperty, pyqtSignal, pyqtSlot

# Interal Imports
from .dashboard_model import DashboardModel
from .timetrack_model import TimetrackModel
from .sliding_bar_object import SlidingBarObject
from .pausable_timer import QPausableTimer

class Manager(QObject):
    modelChanged = pyqtSignal()
    buttonPressed = pyqtSignal()

    def __init__(self, args):
        super().__init__()

        # Store the CI arguments
        self.args = args
        self.time_step = 10 # milliseconds

        # Keeping track of all the data in the logdir
        self.logdir_records = {}

        # Creating the used dashboard model
        self._dashboard_model = DashboardModel()
        self._timetrack_model = TimetrackModel()
        self._sliding_bar = SlidingBarObject()

        # Keeping track of the pause/play state and the start, end time
        self._is_play = True
        self.app_end_time = pd.Timedelta(seconds=0)
        self.app_current_time = pd.Timedelta(seconds=0)
        self.session_complete = False

        # Apply the update to the meta data
        self.meta_update()

        # Using a timer to periodically check for new data
        self.meta_check_timer = QTimer()
        self.meta_check_timer.setInterval(1000) # 1000 milliseconds = 1 second
        self.meta_check_timer.timeout.connect(self.meta_update)
        self.meta_check_timer.start()

        # Using a timer to update all the content in the application
        self.app_global_timer = QPausableTimer()
        # self.app_global_timer.setInterval(10) # 10 milliseconds
        self.app_global_timer.setInterval(self.time_step) 
        self.app_global_timer.timeout.connect(self.app_update)
        self.app_global_timer.start()

    @pyqtProperty(DashboardModel, notify=modelChanged)
    def dashboard_model(self):
        return self._dashboard_model

    @pyqtProperty(TimetrackModel, notify=modelChanged)
    def timetrack_model(self):
        return self._timetrack_model

    @pyqtProperty(SlidingBarObject)
    def sliding_bar(self):
        return self._sliding_bar

    @pyqtProperty(bool, notify=buttonPressed)
    def is_play(self):
        return self._is_play

    @pyqtSlot()
    def play_pause(self):

        # Change the state
        self._is_play = not self._is_play

        # Restarting
        if self.is_play:
            # First, check if the session has been run complete, if so
            # restart it
            if self.session_complete:
                self.session_complete = False
                self.app_current_time = pd.Timedelta(seconds=0)
                self.app_update()

            # Then, restart the app global timer
            self.app_global_timer.resume()
            
        # Stopping 
        else:
            # First, pause the app global timer
            self.app_global_timer.pause()


        # Update the button icon's and other changes based on is_play property
        self.buttonPressed.emit()

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
                total_meta[sub_id]['is_subsession'] = True

        return total_meta

    def meta_update(self):

        # In the update function, we need to check the data stored in 
        # the logdir.
        new_logdir_records = self.get_meta()

        # If the new is different and not None, we need to update!
        if new_logdir_records and new_logdir_records != self.logdir_records:

            # TODO: Instead of just updating data without checking differences,
            # determine the differences and call a different function call
            # when its just a difference.

            # We need to determine the earliest start_time and the latest
            # end_time
            self.app_start_time = pd.Timedelta(seconds=0)
            self.app_end_time = pd.Timedelta(seconds=0)

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
                    entries['start_time'].append(pd.to_timedelta(entry_data['start_time']))
                    entries['end_time'].append(pd.to_timedelta(entry_data['end_time']))

                    if entries['end_time'][-1] > self.app_end_time:
                        self.app_end_time = entries['end_time'][-1]

            # Construct the dataframe
            self.entries = pd.DataFrame(dict(entries))

            # Add the data to the dashboard
            self._dashboard_model.update_data(self.entries)
            self._timetrack_model.update_data(self.entries, self.app_start_time, self.app_end_time)
            
            # Then overwrite the records
            self.logdir_records = new_logdir_records

    def app_update(self):

        # Update the current time based on the time step
        self.app_current_time += pd.Timedelta(seconds=self.time_step/1000)

        # Check if we have finished the session
        if self.app_current_time > self.app_end_time:
            self.session_complete = True
            self.app_current_time = min(self.app_current_time, self.app_end_time)
            self.play_pause() # to pause!

        # Update the sliding bar
        self._sliding_bar.state = self.app_current_time / (self.app_end_time) 
        print(self.app_current_time)

        # Update the content in the homePage

