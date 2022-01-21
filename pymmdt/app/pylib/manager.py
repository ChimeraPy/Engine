# Resource:
# https://stackoverflow.com/questions/64505389/cant-reference-existing-qml-elements-from-python

# Built-in Imports
from typing import Optional, List, Dict, Sequence
import datetime
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
# import apscheduler.schedulers.qt
import apscheduler.schedulers.background

# Setting pandas to stop annoying warnings
pd.options.mode.chained_assignment = None

# PyQt5 Imports
from PyQt5.QtCore import QTimer, QObject, pyqtProperty, pyqtSignal, pyqtSlot

# Interal Imports
from .dashboard_model import DashboardModel
from .timetrack_model import TimetrackModel
from .sliding_bar_object import SlidingBarObject
# from .pausable_timer import QPausableTimer
from .loading_bar_object import LoadingBarObject

# PyMMDT Library Imports
import pymmdt as mm
import pymmdt.video as mmv
import pymmdt.tabular as mmt

class Manager(QObject):
    modelChanged = pyqtSignal()
    playPauseChanged = pyqtSignal()
    pageChanged = pyqtSignal()
    dataLoadedChanged = pyqtSignal()
    slidingBarChanged = pyqtSignal()

    signals = [modelChanged, playPauseChanged, pageChanged, dataLoadedChanged]

    def __init__(
            self,
            logdir:str,
            verbose:Optional[bool]=False,
            time_step:Optional[int]=10,
            replay_speed:Optional[int]=1,
            time_window:Optional[pd.Timedelta]=pd.Timedelta(seconds=1),
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
        self._loading_bar = LoadingBarObject()
        
        # Parameters for tracking progression
        self.current_window = 0
        self.current_window_data = None
        self.entries_time_tracker = None
        self.windows: List = []
        self.loaded_windows: List = []
        
        # Closing information
        self.thread_exit = threading.Event()
        self.thread_exit.clear()

        # Create an empty Collector that later is filled with data streams
        self.collector = mm.Collector(empty=True)
        self.load_data_queue = queue.Queue(maxsize=int(30*1e9/time_window.value))
        self.loading_thread = self.loading_content()

        # Thread for organizing the content to be updated to the frontend
        self.sort_content_thread = self.sort_content()
        self.sort_content_data_queue = queue.Queue(maxsize=int(60*30*1e9/time_window.value))

        # Thread for uploading the content to the frontend
        self.update_content_thread = self.update_content()

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
        
        # Start threads
        self.loading_thread.start()
        self.sort_content_thread.start()
        self.update_content_thread.start()

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

    @pyqtProperty(LoadingBarObject)
    def loading_bar(self):
        return self._loading_bar

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
                self.current_window = -1
                self.app_update()

            # Then, restart the app global timer
            # self.global_timer.resume()
            
        # Stopping 
        # else:
        #     # First, pause the app global timer
        #     self.global_timer.pause()
        
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
                self.windows = mm.tools.get_windows(self.start_time, self.end_time, self.time_window)
                
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

        # Setting the initial value of the loading window
        self.loading_window = self.current_window

        # Get the data continously
        while not self.thread_exit.is_set():

            # Only load windows if there are more to load
            if self.loading_window < len(self.windows):

                # Get the window information of the window to load to queue
                window = self.windows[int(self.loading_window)]

                # Extract the start and end time from window
                start, end = window.start, window.end 

                # Get the data
                data = self.collector.get(start, end)
                timetrack = self.collector.get_timetrack(start, end)

                data_chunk = {
                    # 'start': start,
                    # 'end': end,
                    'data': data,
                    'timetrack': timetrack
                }

                # Put the data into the queue
                while not self.thread_exit.is_set():
                    try:
                        self.load_data_queue.put(data_chunk.copy(), timeout=1)
                        self.loaded_windows.append(self.loading_window)
                        break
                    except queue.Full:
                        time.sleep(0.1)

                # Updating the loading bar
                self._loading_bar.state = (end / self.end_time)

                # Update the loading window pointer
                self.loading_window += 1

            else:
                time.sleep(0.5)

    @mm.tools.threaded
    def sort_content(self):
        
        # Continously update the content
        while not self.thread_exit.is_set():

            # If not playing, just avoid updating
            if self._is_play == False:
                time.sleep(0.1)
                continue

            # Get the next window of information if done processing 
            # the previous window data
            try:
                data_chunk = self.load_data_queue.get(timeout=1)
            except queue.Empty:
                time.sleep(0.1)
                continue
            
            # Decomposing the window item
            current_window_idx = self.loaded_windows.pop()
            current_window_data = data_chunk['data']
            current_window_timetrack = data_chunk['timetrack']
            # current_window_start = data_chunk['start']
            # current_window_end = data_chunk['end']
            
            # If 'END' message, continue
            if isinstance(current_window_data, str):
                continue

            # Adding a column tracking which rows have been used
            for user, entries in current_window_data.items():
                for entry_name, entry_data in entries.items():
                    used = [False for x in range(len(entry_data))]
                    current_window_data[user][entry_name].loc[:,'used'] = used
           
            for index, row in current_window_timetrack.iterrows():

                # Extract row and entry information
                user = row.group
                entry_name = row.ds_type
                entry_time = row.time
                entry_data = current_window_data[user][entry_name]

                # if not 'video' in entry_name: # DEBUGGING!
                #     continue
                # if 'video' in entry_name:
                #     continue

                # If the entry is not empty
                if not entry_data.empty:

                    # Get the entries that have not been used
                    new_samples_loc = (entry_data['used'] == False)

                    # If there are remaining entries, use them!
                    if new_samples_loc.any():

                        # Get the entries idx
                        lastest_content_idx = np.where(new_samples_loc)[0][0] 
                        lastest_content = entry_data.iloc[lastest_content_idx]

                        # Creating the data chunk
                        data_chunk = {
                            'index': index,
                            'user': user,
                            'entry_time': entry_time,
                            'entry_name': entry_name,
                            'content': lastest_content
                        }

                        # But the entry into the queue
                        while not self.thread_exit.is_set():
                            try:
                                self.sort_content_data_queue.put(data_chunk.copy(), timeout=1)
                                entry_data.at[lastest_content_idx, 'used'] = True
                                break
                            except queue.Full:
                                time.sleep(0.1)

            print(f"LOOP")

    @mm.tools.threaded
    def update_content(self):

        previous_time = None
        
        # Continously update the content
        while not self.thread_exit.is_set():

            # If not playing, just avoid updating
            if self._is_play == False:
                time.sleep(0.1)
                continue

            # Get the next entry information!
            try:
                data_chunk = self.sort_content_data_queue.get(timeout=1)
            except queue.Empty:
                time.sleep(0.1)
                continue

            if previous_time:
                diff = (data_chunk['entry_time'] - previous_time)
                time.sleep(0.8*diff.value/1e9)
                previous_time = data_chunk['entry_time']
            else:
                previous_time = data_chunk['entry_time']

            # Updating sliding bar
            self._sliding_bar.state = data_chunk['entry_time'] / (self.end_time) 

            # Update the content
            self._dashboard_model.update_content(
                data_chunk['index'],
                data_chunk['user'],
                data_chunk['entry_name'],
                data_chunk['content']
            )

    @pyqtSlot()
    def exit(self):

        print("Closing")

        # Inform all the threads to end
        self.thread_exit.set()
        print("Stopping threads!")

        # Then wait for threads
        self.loading_thread.join()
        print("Loading thread join")
        self.sort_content_thread.join()
        print("Sort thread join")
        self.update_content_thread.join()
        print("Content thread join")

        print("Finished closing")
