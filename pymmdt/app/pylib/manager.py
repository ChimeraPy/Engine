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
from .pausable_timer import QPausableTimer
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
            loading_sec_limit:Optional[int]=5,
            time_step:Optional[int]=100,
            replay_speed:Optional[int]=1,
            time_window:Optional[pd.Timedelta]=pd.Timedelta(seconds=1),
            meta_check_step:Optional[int]=5000
        ):
        super().__init__()

        # Store the CI arguments
        self.logdir = pathlib.Path(logdir)
        self.loading_sec_limit = loading_sec_limit
        self.time_step: int = time_step # milliseconds
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
        self._sorting_bar = LoadingBarObject()
        
        # Parameters for tracking progression
        self.timetrack = None
        self.current_time = pd.Timedelta(seconds=0)
        self.current_window = 0
        self.windows: List = []
        self.loaded_windows: List = []
        
        # Closing information
        self.thread_exit = threading.Event()
        self.thread_exit.clear()

        # Keeping track of the pause/play state and the start, end time
        self._data_is_loaded = False
        self._is_play = False
        self.end_time = pd.Timedelta(seconds=0)
        self.session_complete = False

        # Apply the update to the meta data
        self.meta_update()

        # Using a timer to periodically update content
        # self.current_time_update = QPausableTimer()
        # self.current_time_update.setInterval(self.time_step)
        # self.current_time_update.timeout.connect(self.update_content)
        # self.current_time_update.start()

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

    @pyqtProperty(LoadingBarObject)
    def sorting_bar(self):
        return self._sorting_bar

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
       
        # If now we are playing,
        if self.is_play:

            # Continue the update timer
            # self.current_time_update.resume()

            # First, check if the session has been run complete, if so
            # restart it
            if self.session_complete:

                # print("Restarted detected!")
                self.restart()

        # else, we are stopping!
        else:
            ...

            # # Pause the update timer
            # self.current_time_update.pause()

        # Update the button icon's and other changes based on is_play property
        self.playPauseChanged.emit()

    @pyqtSlot()
    def restart(self):

        print("Restarting!")

        # Send message to loading and sorting process to halt!
        pause_message = {
            'header': 'META',
            'body': {
                'type': 'PAUSE',
                'content': {},
            }
        }
        for message_to_queue in [self.message_to_sorting_queue, self.message_to_loading_queue]:
            message_to_queue.put(pause_message)

        # Waiting until the processed halted
        time.sleep(0.05)

        # Clear the queues
        while self.loading_data_queue.qsize():
            mm.tools.clear_queue(self.loading_data_queue)
        while self.sorted_data_queue.qsize():
            mm.tools.clear_queue(self.sorted_data_queue)

        # Set the time window to starting loading data
        loading_window_message = {
            'header': 'UPDATE',
            'body': {
                'type': 'LOADING_WINDOW',
                'content': {
                    'loading_window': 0
                }
            }
        }
        self.message_to_loading_queue.put(loading_window_message)

        # Waiting until the loading window message is processed
        time.sleep(0.05)

        # Continue the processes
        resume_message = {
            'header': 'META',
            'body': {
                'type': 'RESUME',
                'content': {}
            }
        }
        for message_to_queue in [self.message_to_sorting_queue, self.message_to_loading_queue]:
            message_to_queue.put(resume_message)

        # Set the time to the start_time
        self.current_time = self.start_time
        self._sliding_bar.state = self.current_time / (self.end_time) 

        # Typically after rewind, the video is played
        self._is_play = True
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
                # print(f"NEW LOADING MESSAGE - {loading_message}")

                # Handling UPDATE events
                if loading_message['header'] == 'UPDATE':
                    
                    # Timetrack Update
                    if loading_message['body']['type'] == 'TIMETRACK':
                        self.timetrack = loading_message['body']['content']['timetrack']
 
                    # Handling counter changes
                    if loading_message['body']['type'] == 'COUNTER':
                        new_state = loading_message['body']['content']['loading_window'] / len(self.windows)
                        self.loading_bar.state = new_state

                # Handling META events
                elif loading_message['header'] == 'META':
                   
                    # Handling END type events
                    if loading_message['body']['type'] == 'END':
                        ...

            # Processing new sorting messages
            if sorting_message_new:

                # Handle the incoming message
                # print(f"NEW SORTING MESSAGE - {sorting_message}")

                # Handling UPDATE events
                if sorting_message['header'] == 'UPDATE':

                    # Handling general counter updating
                    if sorting_message['body']['type'] == 'COUNTER':
                        new_state = sorting_message['body']['content']['loaded_time'] / self.end_time
                        self.sorting_bar.state = new_state
                    
                    # Handling finishing sorting
                    if sorting_message['body']['type'] == 'END':
                        self.sorting_bar.state = 1

    @mm.tools.threaded
    def update_content(self):

        tic = time.time()

        # Keep repeating until exiting the app
        while not self.thread_exit.is_set():

            # Only processing if we have the global timetrack
            if type(self.timetrack) == type(None):
                time.sleep(1)
                continue

            # Also, not processing if not playing or the session is complete
            if not self._is_play or self.session_complete:
                time.sleep(0.05)
                continue

            # Compute the average time it takes to update content, and its ratio 
            # to the expect time, clipping at 1
            average_update_delta = sum(self.update_content_times)/max(1, len(self.update_content_times))
            update_delta_ratio = (self.time_step/1000) / max(self.time_step/1000,average_update_delta)
            update_delta_ratio *= 0.98 # Applying a compensation factor
            # print("AVERAGE: ", average_update_delta ," UPDATE DELTA RATIO: ", update_delta_ratio)
            
            # Start timing the process of uploading content
            tic = time.time()

            # Calculating the t_{x+1} time
            next_time = self.current_time + pd.Timedelta(milliseconds=self.time_step)

            # Determine the number of data chunks to process given the 
            # time_step window
            time_window_idx = ((self.timetrack['time'] >= self.current_time) & \
                (self.timetrack['time'] < next_time))
            to_process_data_chunks = self.timetrack[time_window_idx]

            # If there is no data chunks to process, update and continue
            if len(to_process_data_chunks) == 0:
                self.current_time = next_time
                self._sliding_bar.state =  self.current_time / (self.end_time)
                continue
            
            # Storing all the data chunks from t_x to t_{x+1}
            data_chunks = []

            # Get all the data chunks from t_x to t_{x+1}
            while True:

                # Get the next entry information!
                try:
                    data_chunk = self.sorted_data_queue.get(timeout=2*self.time_step/1000)
                    # print(data_chunk['index'], ' - GOT DATA CHUNK TIME: ', data_chunk['entry_time'])
                except queue.Empty:
                    # Check if all the data has been loaded, if so that means
                    # that all the data has been played
                    if self.sorting_bar.state == 1:
                        print("Session end detected!")
                        self.session_complete = True
                        self._is_play = False
                        self.playPauseChanged.emit()
                    
                    # Regardless, break
                    break

                # Storing data only if its during or after the time window
                if (to_process_data_chunks['time'] >= data_chunk['entry_time']).sum():
                    data_chunks.append(data_chunk)
                
                # Once gotten the data, check if we have collected all the needed
                # data
                if len(data_chunks) == len(to_process_data_chunks):
                    break

            # Update to the content
            for data_chunk in data_chunks:

                # Calculate the delta between the current time and the entry's time
                if data_chunk['entry_time'] > self.current_time:
                    time_delta = (data_chunk['entry_time'] - self.current_time).value / 1e9
                    time_delta = max(0,time_delta-0.05) # Accounting for updating
                    time.sleep(update_delta_ratio*time_delta)

                # Updating sliding bar
                self.current_time = data_chunk['entry_time']
                self._sliding_bar.state = self.current_time / (self.end_time) 

                # Update the content
                self._dashboard_model.update_content(
                    data_chunk['index'],
                    data_chunk['user'],
                    data_chunk['entry_name'],
                    data_chunk['content']
                )

            # Computing the time difference of uploading content
            tac = time.time()
            delta = tac - tic
            # print(f"UPDATING DELTA: {delta}")

            # Sleeping the difference
            time_delta = max(0, self.time_step/1000 - delta)
            # print("LOOP END SLEEP: ", time_delta)
            time.sleep(update_delta_ratio*time_delta)

            # Finally update the sliding bar
            toe = time.time()
            final_delta = toe-tic
            self.update_content_times.append(final_delta)
            # print(f"FINAL DELTA: {final_delta}")
            self.current_time = next_time
            # print('LOOP END: ', self.current_time)
            self._sliding_bar.state = self.current_time / (self.end_time) 

    def init_data_pipeline(self, unique_users, users_meta):
        
        # Changing the time_window_size based on the number of entries
        div_factor = min(len(self.entries), 10)
        self.time_window = pd.Timedelta(seconds=(self.time_window.value/1e9)/div_factor)
                
        # Get the necessary information to create the Collector 
        self.windows = mm.tools.get_windows(self.start_time, self.end_time, self.time_window)
        self.update_content_times = collections.deque(maxlen=100)
     
        # Creating queues for the data
        max_qsize_allowed= int(self.loading_sec_limit/(1e-9*self.time_window.value))
        self.loading_data_queue = mp.Queue(maxsize=max_qsize_allowed)
        self.sorted_data_queue = mp.Queue(maxsize=max_qsize_allowed*15*len(self.entries))

        # Creating queues for communication
        self.message_to_loading_queue = mp.Queue(maxsize=max_qsize_allowed)
        self.message_from_loading_queue = mp.Queue(maxsize=max_qsize_allowed)
        self.message_to_sorting_queue = mp.Queue(maxsize=max_qsize_allowed)
        self.message_from_sorting_queue = mp.Queue(maxsize=max_qsize_allowed)

        # Storing all the queues
        self.queues = {
            'loading': self.loading_data_queue,
            'sorting': self.sorted_data_queue,
            'm_to_loading': self.message_to_loading_queue, 
            'm_from_loading': self.message_from_loading_queue,
            'm_to_sorting': self.message_to_sorting_queue, 
            'm_from_sorting': self.message_from_sorting_queue,
        }

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
            entries=self.entries,
            verbose=True
        )

        # Start the processes
        self.loading_process.start()
        self.sorting_process.start()

    @pyqtSlot()
    def exit(self):

        print("Closing")

        # Only execute the thread and process ending if data is loaded
        if self._data_is_loaded:

            # Inform all the threads to end
            self.thread_exit.set()
            # print("Stopping threads!")

            self.check_messages_thread.join()
            # print("Checking message thread join")
            
            self.update_content_thread.join()
            # print("Content update thread join")
            # self.current_time_update.stop()
            # print("Content timer stopped")

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

            # Clearing Queues to permit the processes to shutdown
            # print("Clearing queues")
            for q_name, queue in self.queues.items():
                # print(f"Clearing {q_name} queue")
                mm.tools.clear_queue(queue)

            # Then wait for threads and process
            # print("Clearing loading queue again")
            while self.loading_data_queue.qsize():
                mm.tools.clear_queue(self.loading_data_queue)
                time.sleep(0.2)

            self.loading_process.join()
            # print("Loading process join")

            time.sleep(0.3)
            
            # Clearing Queues to permit the processes to shutdown
            # print("Clearing all queues again")
            for q_name, queue in self.queues.items():
                # print(f"clearing {q_name}.")
                mm.tools.clear_queue(queue)

            # print("Clearing sorting queue again")
            while self.sorted_data_queue.qsize():
                # print(self.sorted_data_queue.qsize())
                mm.tools.clear_queue(self.sorted_data_queue)
                time.sleep(0.2)

            self.sorting_process.join()
            # print("Sorting process join")

        # print("Finished closing")
