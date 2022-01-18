# Resource:
# https://stackoverflow.com/questions/64505389/cant-reference-existing-qml-elements-from-python

# Built-in Imports
from typing import Optional, List, Dict, Sequence
import pathlib
import json
import collections
import queue
import threading
import time

# Third-party Imports
import numpy as np
import pandas as pd
import tqdm

# Setting pandas to stop annoying warnings
pd.options.mode.chained_assignment = None

# PyQt5 Imports
from PyQt5.QtCore import QTimer, QObject, pyqtProperty, pyqtSignal, pyqtSlot

# Interal Imports
from .dashboard_model import DashboardModel
from .timetrack_model import TimetrackModel
from .sliding_bar_object import SlidingBarObject
from .pausable_timer import QPausableTimer

# PyMMDT Library Imports
import pymmdt as mm
import pymmdt.video as mmv
import pymmdt.tabular as mmt

class Manager(QObject):
    modelChanged = pyqtSignal()
    playPauseChanged = pyqtSignal()
    pageChanged = pyqtSignal()
    dataLoadedChanged = pyqtSignal()

    signals = [modelChanged, playPauseChanged, pageChanged, dataLoadedChanged]

    def __init__(
            self,
            logdir:str,
            verbose:Optional[bool]=False,
            time_step:Optional[int]=10,
            replay_speed:Optional[int]=1,
            time_window:Optional[pd.Timedelta]=pd.Timedelta(seconds=0.1),
            meta_check_step:Optional[int]=5000
        ):
        super().__init__()

        # Store the CI arguments
        self.logdir = pathlib.Path(logdir)
        self.time_step = time_step # milliseconds
        self.replay_speed = replay_speed
        self.time_window = time_window
        self.meta_check_step = meta_check_step # milliseconds
        self.verbose = verbose

        # Keeping track of all the data in the logdir
        self._page = "homePage"
        self.logdir_records = None

        # Creating the used dashboard model
        self._dashboard_model = DashboardModel()
        self._timetrack_model = TimetrackModel()
        self._sliding_bar = SlidingBarObject()
        # TODO: Add loading bar with buffer data

        # Create an empty Collector that later is filled with data streams
        self.collector = mm.Collector(empty=True)
        self.load_data_queue = queue.Queue(maxsize=int(30*1e9/time_window.value))
        self.collector.set_loading_queue(self.load_data_queue)

        # Parameters for tracking progression
        self.current_window = -1
        self.current_window_data = None
        self.entries_time_tracker = None

        # Thread for uploading the content to the frontend
        self.content_data_queue = queue.Queue(maxsize=100)
        self.content_thread = self.update_content()

        # Closing information
        self.thread_exit = threading.Event()
        self.thread_exit.clear()

        # Keeping track of the pause/play state and the start, end time
        self._data_is_loaded = False
        self._is_play = False
        self.end_time = pd.Timedelta(seconds=0)
        self.current_time = pd.Timedelta(seconds=0)
        self.session_complete = False

        # Apply the update to the meta data
        self.meta_update()

        # Using a timer to periodically check for new data
        self.meta_check_timer = QTimer()
        self.meta_check_timer.setInterval(self.meta_check_step) 
        self.meta_check_timer.timeout.connect(self.meta_update)
        self.meta_check_timer.start()

        # Using a timer to update all the content in the application
        self.global_timer = QPausableTimer()
        self.global_timer.setInterval(self.time_step) 
        self.global_timer.timeout.connect(self.app_update)
        # self.global_timer.start()

    @pyqtProperty(str, notify=pageChanged)
    def page(self):
        return self._page

    @pyqtProperty(DashboardModel, notify=modelChanged)
    def dashboard_model(self):
        return self._dashboard_model

    @pyqtProperty(TimetrackModel, notify=modelChanged)
    def timetrack_model(self):
        return self._timetrack_model

    @pyqtProperty(SlidingBarObject)
    def sliding_bar(self):
        return self._sliding_bar

    @pyqtProperty(bool, notify=playPauseChanged)
    def is_play(self):
        return self._is_play

    @pyqtProperty(bool, notify=dataLoadedChanged)
    def data_is_loaded(self):
        return self._data_is_loaded

    @pyqtSlot()
    def play_pause(self):

        # Before enabling data, we need to check if the meta data is 
        # valid, if not, then do nothing
        if not self._data_is_loaded:
            return None

        # Change the state
        self._is_play = not self._is_play
        
        # Restarting
        if self.is_play:
            # First, check if the session has been run complete, if so
            # restart it
            if self.session_complete:
                self.session_complete = False
                self.current_time = pd.Timedelta(seconds=0)
                self.app_update()

            # Then, restart the app global timer
            self.global_timer.resume()
            
        # Stopping 
        else:
            # First, pause the app global timer
            self.global_timer.pause()
        
        # Update the button icon's and other changes based on is_play property
        self.playPauseChanged.emit()

    def get_meta(self):

        # Obtain all the meta files
        root_meta = self.logdir / 'meta.json' 

        # If no meta, then provide error message through a page.
        if not root_meta.exists(): # Change the page to Home page
            if self._page != "homePage":
                self._page = "homePage"
                self.pageChanged.emit()
            return None # Exit early

        else: # Change the page to Dashboard page
            self._page = "dashboardPage"
            self.pageChanged.emit()

        # Else, get the initial and all other meta files
        with open(root_meta, 'r') as f:
            meta_data = json.load(f)
            meta_data['is_subsession'] = False

        # Check if there is any subsessions and get their meta
        total_meta = {'root': meta_data}
        for sub_id in meta_data['subsessions']:
            with open(self.logdir / sub_id / 'meta.json', 'r') as f:
                total_meta[sub_id] = json.load(f)
                total_meta[sub_id]['is_subsession'] = True

        return total_meta

    def create_entries_df(self, new_logdir_records:Dict) -> pd.DataFrame:

        # Construct pd.DataFrame for the data
        # For each session data, store entries by modality
        entries = collections.defaultdict(list)
        for session_name, session_data in new_logdir_records.items():
            for entry_name, entry_data in session_data['records'].items():

                # For now, skip any tabular data since we don't have a way to visualize
                if entry_data['dtype'] == 'tabular':
                    continue

                print(entry_data)

                entries['user'].append(session_name)
                entries['entry_name'].append(entry_name)
                entries['dtype'].append(entry_data['dtype'])
                entries['start_time'].append(pd.to_timedelta(entry_data['start_time']))
                entries['end_time'].append(pd.to_timedelta(entry_data['end_time']))
                entries['is_subsession'].append(session_data['is_subsession'])

                if entries['end_time'][-1] > self.end_time:
                    self.end_time = entries['end_time'][-1]

        # Construct the dataframe
        entries = pd.DataFrame(dict(entries))

        print(entries)

        return entries

    def load_data_streams(self, unique_users:List, users_meta:Dict) -> Dict[str, Sequence[mm.DataStream]]:

        # Loading data streams # TODO!
        users_data_streams = collections.defaultdict(list)
        for user_name, user_meta in tqdm.tqdm(zip(unique_users, users_meta), disable=not self.verbose):
            for index, row in user_meta.iterrows():
                # Extract useful meta
                entry_name = row['entry_name']
                dtype = row['dtype']

                # Construct the directory the file/directory is found
                if row['is_subsession']:
                    file_dir = self.logdir / row['user']
                else:
                    file_dir = self.logdir
                
                # Load the data
                if dtype == 'video':
                    ds = mmv.VideoDataStream(
                        name=entry_name,
                        start_time=row['start_time'],
                        video_path=file_dir/f"{entry_name}.avi"
                    )
                elif dtype == 'image':

                    # Load the meta CSV
                    df = pd.read_csv(file_dir/entry_name/'timestamps.csv')

                    # Load all the images into a data frame
                    img_filepaths = []
                    for index, row in df.iterrows():
                        img_fp = file_dir / entry_name / f"{row['idx']}.jpg"
                        img_filepaths.append(img_fp)
                        # imgs.append(mm.tools.to_numpy(Image.open(img_fp)))
                    df['img_filepaths'] = img_filepaths
                    
                    # Create ds
                    ds = mmt.TabularDataStream(
                        name=entry_name,
                        data=df,
                        time_column='_time_'
                    )
                elif dtype == 'tabular':
                    raise NotImplementedError("Tabular visualization is still not implemented.")
                else:
                    raise RuntimeError(f"{dtype} is not a valid option.")

                # Store the data in Dict
                users_data_streams[user_name].append(ds)

        # Return the Dict[str, List[DataStream]]
        return users_data_streams

    def meta_update(self):
        
        # In the update function, we need to check the data stored in 
        # the logdir.
        new_logdir_records = self.get_meta()

        # If the new is different and not None, we need to update!
        if new_logdir_records: 

            # If this is the first time loading data
            if type(self.logdir_records) == type(None):

                # We need to determine the earliest start_time and the latest
                # end_time
                self.start_time = pd.Timedelta(seconds=0)
                self.end_time = pd.Timedelta(seconds=0)

                # Construct the entries in a pd.DataFrame
                self.entries = self.create_entries_df(new_logdir_records)

                # Add the data to the dashboard
                self._dashboard_model.update_data(self.entries)
                self._timetrack_model.update_data(self.entries, self.start_time, self.end_time)

                # Loading the data for the Collector
                unique_users = self.entries['user'].unique()
                users_meta = [self.entries.loc[self.entries['user'] == x] for x in unique_users]

                # Reset index and drop columns
                for i in range(len(users_meta)):
                    users_meta[i].reset_index(inplace=True)
                    users_meta[i] = users_meta[i].drop(columns=['index'])

                # Load the data from the entries into DataStreams
                users_data_streams = self.load_data_streams(unique_users, users_meta)

                # Adding data to the Collector
                self.collector.set_data_streams(users_data_streams, self.time_window)
                self.loading_thread = self.loading_content()
                # self.loading_thread = self.collector.load_data_to_queue()
                # self.loading_thread.start()
                self.loading_thread.start()
                self.content_thread.start()
                
                # Update the flag tracking if data is loaded
                self._data_is_loaded = True
                self.dataLoadedChanged.emit()

            # If this is now updating already loaded data
            elif new_logdir_records != self.logdir_records:
                # TODO: add constant updating
                ...

        # The meta data is invalid or not present
        else:
            if self._data_is_loaded:
                self._data_is_loaded = False
                self.dataLoadedChanged.emit()
            
        # Then overwrite the records
        self.logdir_records = new_logdir_records

    @mm.tools.threaded
    def loading_content(self):

        # Calculating the windows only after the thread has been created
        self.windows = mm.tools.get_windows(self.start_time, self.end_time, self.time_window)
        print(f"start_time: {self.start_time}, end_time: {self.end_time}, windows: {len(self.windows)}")
        past_window = -1

        # Get the data continously
        while not self.thread_exit.is_set():

            if self.current_window != past_window:

                # Update the past window
                past_window = self.current_window

                # Get the current window information
                window = self.windows[int(self.current_window)]

                # Extract the start and end time from window
                start, end = window.start, window.end 

                # Get the data
                data = self.collector.get(start, end)

                # Put the data into the queue
                self.load_data_queue.put(data.copy(), block=True)

            else:
                time.sleep(1)

    @mm.tools.threaded
    def update_content(self):
        
        while not self.thread_exit.is_set():
            
            # Get the data
            try:
                current_window_data = self.load_data_queue.get(timeout=1).copy()
            except queue.Empty:
                continue

            # If 'END' message, continue
            if isinstance(current_window_data, str):
                continue

            # Adding a column tracking which rows have been used
            for user, entries in current_window_data.items():
                for entry_name, entry_data in entries.items():
                    used = [False for x in range(len(entry_data))]
                    current_window_data[user][entry_name].loc[:,'used'] = used

            print("NEW DATA")
            print(current_window_data.keys())
            for user, entries in current_window_data.items():
                print(user, ': ', entries.keys())

            # Now we check if we need to update content for entries in the 
            # current window data
            for user, entries in current_window_data.items():
                for entry_name, entry_data in entries.items():

                    print(f"manager - starting: {user} - {entry_name}")
                    print(entry_data.keys())

                    # # If not empty, continue
                    if not entry_data.empty:

                        print(f"manager - not_empty: {user} - {entry_name}")

                        # Get the index for new samples
                        new_samples_loc = (entry_data['used'] == False)

                        # If there is any samples after the time window
                        if new_samples_loc.any():
                            
                            print(f"manager - updating: {user} - {entry_name}")

                            latest_sample_idx = np.where(new_samples_loc)[0][0]
                            print(latest_sample_idx)
                            entry_data.at[latest_sample_idx, 'used'] = True
                            latest_sample = entry_data.iloc[latest_sample_idx]

                            self._dashboard_model.update_content(user, entry_name, latest_sample)

            print("LOOP")

    def app_update(self):

        # Update the current time based on the time step
        self.past_time = self.current_time
        self.current_time += self.replay_speed * pd.Timedelta(seconds=self.time_step/1000)

        # Check if we have finished the session
        if self.current_time > self.end_time:
            self.session_complete = True
            self.current_time = min(self.current_time, self.end_time)
            self.play_pause() # to pause!

        # Update the sliding bar
        self._sliding_bar.state = self.current_time / (self.end_time) 

        # Update the content in the homePage
        # Resource: https://stackoverflow.com/questions/52944567/unable-to-stream-frames-from-camera-to-qml/52954271#52954271  

        # First, check if we enter a new time window 
        current_window = (self.current_time.value/1e9) // (self.time_window.value/1e9)
        if self.current_window != current_window:

            # Update the current_window
            self.current_window = current_window

        # # If change, get the data from the queue
        # if self.load_data_queue.qsize() != 0:
        #     current_window_data = self.load_data_queue.get()
        #     self.content_data_queue.put(current_window_data)

    @pyqtSlot()
    def exit(self):

        print("Closing")

        # Inform all the threads to end
        self.thread_exit.set()
        print("Stopping threads!")

        # Then wait for threads
        self.content_thread.join()
        print("Content thread join")
        self.loading_thread.join()
        print("Loading thread join")

        print("Finished closing")
