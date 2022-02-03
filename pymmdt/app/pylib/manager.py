# Resource:
# https://stackoverflow.com/questions/64505389/cant-reference-existing-qml-elements-from-python

# Built-in Imports
from typing import Optional, List, Dict, Sequence
import os
import sys
import datetime
import pathlib
import json
import collections
import queue
import threading
import time
import multiprocessing as mp
import multiprocessing.managers as mpm

# Third-party Imports
import numpy as np
import pandas as pd
import tqdm
# import apscheduler.schedulers.qt
# import apscheduler.schedulers.background

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
from .data_pipeline import DataLoadingProcess, DataSortingProcess

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
                # users_data_streams = self.load_data_streams(unique_users, users_meta)

                # Setting up the data pipeline
                self.init_data_pipeline(unique_users, users_meta)
                
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
    def check_messages(self):

        # Set the flag to check if new message
        loading_message = None
        loading_message_new = False
        sorting_message = None
        sorting_message_new = False
        
        # Constantly check for messages
        while not self.thread_exit.is_set():
            
            # Prevent blocking, as we need to check if the thread_exist
            # is set.
            while not self.thread_exit.is_set():

                # Checking the loading message queue
                try:
                    loading_message = self.message_from_loading_queue.get(timeout=0.1)
                    loading_message_new = True
                except queue.Empty:
                    loading_message_new = False

                # Ckecking the sorting message queue
                try:
                    sorting_message = self.message_from_sorting_queue.get(timeout=0.1)
                    sorting_message_new = True
                except queue.Empty:
                    sorting_message_new = False

                # If new message, process it right now
                if loading_message_new or sorting_message_new:
                    break

                # else, placing a sleep timer
                time.sleep(0.1)
              
            # Processing new loading messages
            if loading_message_new:

                # Handle the incoming message
                print(f"NEW LOADING MESSAGE - {loading_message}")

                # Handling UPDATE events
                if loading_message['header'] == 'UPDATE':
                    
                    # Handling counter changes
                    if loading_message['body']['type'] == 'COUNTER':
                        new_state = loading_message['body']['content']['loading_window'] / len(self.windows)
                        self.loading_bar.state = new_state

                # Handling META events
                elif loading_message['header'] == 'META':
                    ...

                    # Handling END type events
                    if loading_message['body']['type'] == 'END':
                        ...

            # Processing new sorting messages
            if sorting_message_new:

                # Handle the incoming message
                print(f"NEW SORTING MESSAGE - {sorting_message}")

    @mm.tools.threaded
    def update_content(self):

        # previous_time = None
        
        # Continously update the content
        while not self.thread_exit.is_set():

            # If not playing, just avoid updating
            if self._is_play == False:
                time.sleep(0.1)
                continue

            # Get the next entry information!
            try:
                data_chunk = self.sorted_data_queue.get(timeout=1)
            except queue.Empty:
                time.sleep(0.1)
                continue

            # if previous_time:
            #     diff = (data_chunk['entry_time'] - previous_time)
            #     time.sleep(0.8*diff.value/1e9)
            #     previous_time = data_chunk['entry_time']
            # else:
            #     previous_time = data_chunk['entry_time']

            # Updating sliding bar
            self._sliding_bar.state = data_chunk['entry_time'] / (self.end_time) 

            # Update the content
            self._dashboard_model.update_content(
                data_chunk['index'],
                data_chunk['user'],
                data_chunk['entry_name'],
                data_chunk['content']
            )

    def init_data_pipeline(self, unique_users, users_meta):
                
        # Get the necessary information to create the Collector 
        self.windows = mm.tools.get_windows(self.start_time, self.end_time, self.time_window)
      
        # Creating queues for the data
        num_of_w_in_30_sec = int(30/(1e-9*self.time_window.value))
        self.loading_data_queue = mp.Queue(maxsize=num_of_w_in_30_sec)
        self.sorted_data_queue = mp.Queue(maxsize=num_of_w_in_30_sec*10)

        # Creating queues for communication
        self.message_to_loading_queue = mp.Queue(maxsize=num_of_w_in_30_sec)
        self.message_from_loading_queue = mp.Queue(maxsize=num_of_w_in_30_sec)
        self.message_to_sorting_queue = mp.Queue(maxsize=num_of_w_in_30_sec)
        self.message_from_sorting_queue = mp.Queue(maxsize=num_of_w_in_30_sec)

        # Storing all the queues
        self.queues = [
            self.loading_data_queue, self.sorted_data_queue,
            self.message_to_loading_queue, self.message_from_loading_queue,
            self.message_to_sorting_queue, self.message_from_sorting_queue,
        ]

        # Starting the Manager's messaging thread
        self.check_messages_thread = self.check_messages()
        self.check_messages_thread.start()

        # Starting the content update thread
        self.update_content_thread = self.update_content()
        self.update_content_thread.start()

        # Creating the loading process that uses the window loading 
        # system to get the data.
        self.loading_process = DataLoadingProcess(
            logdir=self.logdir,
            loading_data_queue=self.loading_data_queue,
            message_to_queue=self.message_to_loading_queue,
            message_from_queue=self.message_from_loading_queue,
            windows=self.windows,
            time_window=self.time_window,
            unique_users=unique_users,
            users_meta=users_meta,
            verbose=True
        )
        
        # Creating a subprocess that takes the windowed data and loads
        # it to a queue sorted in a time-chronological fashion.
        self.sorting_process = DataSortingProcess(
            loading_data_queue=self.loading_data_queue,
            sorted_data_queue=self.sorted_data_queue,
            message_to_queue=self.message_to_sorting_queue,
            message_from_queue=self.message_from_sorting_queue,
            verbose=True
        )

        # Start the processes
        self.loading_process.start()
        self.sorting_process.start()

    @pyqtSlot()
    def exit(self):

        print("Closing")

        # Inform all the threads to end
        self.thread_exit.set()
        print("Stopping threads!")
        self.check_messages_thread.join()
        print("Checking message thread join")

        # Informing all process to end
        end_message = {
            'header': 'META',
            'body': {
                'type': 'END',
                'content': {},
            }
        }
        for message_to_queue in [self.message_to_sorting_queue, self.message_to_loading_queue]:
            message_to_queue.put(end_message)

        # Wait for process confirmation to end
        # for message_from_queue in [self.message_from_sorting_queue, self.message_from_loading_queue]:
        #     while True:
        #         message = message_from_queue.get()

        #         # Check if end message as well
        #         if message == end_message:
        #             break

        # Clearing Queues to permit the processes to shutdown
        for queue in self.queues:
            print(queue.qsize())
            mm.tools.clear_queue(queue)

        # Then wait for threads and process
        while self.loading_data_queue.qsize():
            mm.tools.clear_queue(self.loading_data_queue)
            time.sleep(0.2)

        self.loading_process.join()
        print("Loading process join")

        time.sleep(0.3)
        
        # Clearing Queues to permit the processes to shutdown
        for queue in self.queues:
            mm.tools.clear_queue(queue)

        while self.sorted_data_queue.qsize():
            print(self.sorted_data_queue.qsize())
            mm.tools.clear_queue(self.sorted_data_queue)
            time.sleep(0.2)

        self.sorting_process.join()
        print("Sorting process join")

        # self.update_content_thread.join()
        # print("Content thread join")

        print("Finished closing")
