# Resource:
# https://stackoverflow.com/questions/64505389/cant-reference-existing-qml-elements-from-python

# Built-in Imports
from typing import List, Dict, Sequence
import pathlib
import json
import collections
import queue
import threading
import time
import multiprocessing as mp

# Third-party Imports
import pandas as pd
import tqdm
import psutil

# Setting pandas to stop annoying warnings
pd.options.mode.chained_assignment = None

# PyQt5 Imports
from PyQt5.QtCore import QTimer, QObject, pyqtProperty, pyqtSignal, pyqtSlot
from PyQt5.QtGui import QKeySequence
from PyQt5.QtWidgets import QShortcut

# Interal Imports
from .dashboard_model import DashboardModel
from .timetrack_model import TimetrackModel
from .sliding_bar_object import SlidingBarObject
from .loading_bar_object import LoadingBarObject

# ChimeraPy Library Imports
from chimerapy.loader import Loader
from chimerapy.sorter import Sorter
from chimerapy.core.tools import clear_queue, threaded, get_windows
from chimerapy.core.data_stream import DataStream
from chimerapy.core.video.data_stream import VideoDataStream
from chimerapy.core.tabular.data_stream import TabularDataStream

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
            time_window:pd.Timedelta=pd.Timedelta(seconds=0.5),
            meta_check_step:int=5000,
            max_message_queue_size:int=1000,
            max_loading_queue_size:int=1000,
            memory_limit:float=0.8,
            loader_memory_ratio:float=0.1,
            verbose:bool=False,
        ):
        """Construct the ``Manager``.

        Args:
            logdir (str): Directory containing the saved logged data.
            time_window (pd.Timedelta): Size of the time window.
            meta_check_step (int): The period for checking meta changes.
            max_message_queue_size (int): Max messaging queue size.
            max_loading_queue_size (int): Max loading queue size.
            memory_limit (float): Percentage of available RAM memory
            that will be permitted to be used by the library. The memory
            limit restricts the use of additional memory.

            loader_memory_ratio (float): Memory factor to account for 
            possible lag in memory consumption tracking.

            verbose (bool): Debugging printout.

        """
        super().__init__()

        if verbose:
            print("DEBUGGING MODE ENABLED!")

        # Store the CI arguments
        self.logdir = pathlib.Path(logdir)
        self.time_window = time_window
        self.meta_check_step = meta_check_step # milliseconds
        self.max_message_queue_size = max_message_queue_size
        self.max_loading_queue_size = max_loading_queue_size
        self.verbose = verbose

        # Keeping track of all the data in the logdir
        self._page = "homePage"
        self.meta_data = None

        # Define the protocol for processing messages from the loader and logger
        self.respond_message_protocol = {
            'LOADER':{
                'UPDATE': {
                    'TIMETRACK': self.respond_loader_message_timetrack,
                    'COUNTER': self.respond_loader_message_counter
                },
                'META': {
                    'END': self.respond_loader_message_end
                }
            },
            'SORTER':{
                'UPDATE': {
                    'COUNTER': self.respond_sorter_message_counter,
                    'MEMORY': self.respond_sorter_message_memory
                },
                'META': {
                    'END': self.respond_sorter_message_end
                }
            }
        }

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

        # Keeping track of memory usage
        self.loader_memory_ratio = loader_memory_ratio
        self.total_available_memory = memory_limit * psutil.virtual_memory().available
        self.loading_queue_memory_chunks = {}
        self.sorting_queue_memory_chunks = {}
        self.total_memory_used = 0
        
        # Closing information
        self.thread_exit = threading.Event()
        self.thread_exit.clear()

        # Keeping track of the choice of sorting
        # self.sort_by = "entry_name"
        self.sort_by = "user"

        # Keeping track of the pause/play state and the start, end time
        self.stop_everything = False
        self._data_is_loaded = False
        self._is_play = False
        self.end_time = pd.Timedelta(seconds=0)
        self.session_complete = False

        # Apply the update to the meta data
        self.meta_update()

        # Using a timer to periodically check for new data
        self.meta_check_timer = QTimer()
        self.meta_check_timer.setInterval(self.meta_check_step) 
        self.meta_check_timer.timeout.connect(self.meta_update)
        self.meta_check_timer.start()

        # # Providing keyboard bindings for convienience # (TODO)
        # self.shortcuts = {
        #     'zoom_in': QShortcut(QKeySequence('Ctrl++'), self),
        #     'zoom_out': QShortcut(QKeySequence('Ctr+-'), self),
        # }        
        # self.shortcuts['zoom_in'].activated.connect(self.zoom_in)
        # self.shortcuts['zoom_out'].activated.connect(self.zoom_out)

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
        """Pressing the pause/play button."""

        # Before enabling data, we need to check if the meta data is 
        # valid, if not, then do nothing
        if not self._data_is_loaded:
            return None

        # Change the state
        self._is_play = not self._is_play
       
        # If now we are playing,
        if self.is_play:

            # First, check if the session has been run complete, if so
            # restart it
            if self.session_complete:

                # print("Restarted detected!")
                self.restart()

        # Update the button icon's and other changes based on is_play property
        self.playPauseChanged.emit()
    
    @pyqtSlot()
    def restart(self):
        """Pressing the restart button."""
       
        # Pause the loader, sorter, and manager play thread
        self.pre_change_stop()
        
        # Clear the queues
        while self.loading_queue.qsize():
            clear_queue(self.loading_queue)
        while self.sorting_queue.qsize():
            clear_queue(self.sorting_queue)

        # Delete all the memory
        self.loading_queue_memory_chunks = {}
        self.sorting_queue_memory_chunks = {}
        
        # Set the time to the start_time
        self.current_time = self.start_time
        self._sliding_bar.state = self.current_time / (self.end_time) 

        # Set the window of the loader and reset the session variable
        self.session_complete = False
        self.message_set_loading_window_loader(0)

        # And reset the content
        self._dashboard_model.reset_content()
        self.modelChanged.emit()

        # Resume the loader, sorter, and manager play thread
        self.post_change_resume()
        
        # Typically after rewind, the video is played
        self._is_play = True
        self.playPauseChanged.emit()

    @pyqtSlot()
    def sort_by_user(self):
        """Pressing the 'Sort-By-User' button."""
       
        # If already sorted as user, ignore
        if self.sort_by == "user":
            return None
        
        # Change to user
        self.change_sort("user")

    @pyqtSlot()
    def sort_by_entry(self):
        """Pressing the 'Sort-By-Entry button."""
        
        # If already sorted as entry, ignore
        if self.sort_by == "entry_name":
            return None
        
        # Now sort by entry_name
        self.change_sort("entry_name")

    def change_sort(self, sort_by:str):
        """Changing the sorting of the content in the dashboard. 

        There are two possible configuration for the dashboard: "user"
        and "entry_name". 

        Args:
            sort_by (str): "user" or "entry_name" options.

        """
        # Store if the session was playing
        was_playing = self._is_play

        # Updating the variable
        self.sort_by = sort_by

        # Stop loader, sorter, and main content thread
        self.pre_change_stop()

        # Update the content
        self._dashboard_model.reset_content()
        self._dashboard_model.update_data(self.entries, self.sort_by)
        self.modelChanged.emit()
        
        # Resume loader, sorter, and main content thread
        self.post_change_resume()

        # If the session was playing, continue
        self._is_play = was_playing
        self.playPauseChanged.emit()

    def pre_change_stop(self):
        """Steps required before changing timeline components."""
        
        # Stopping playing 
        self._is_play = False
        self.playPauseChanged.emit()

        # Send message to loading and sorting process to halt!
        self.stop_everything = True
        self.message_pause_loader()
        self.message_pause_sorter()
        self.sorter_paused = True
        self.loader_paused = True

    def post_change_resume(self):
        """Steps required after changing timeline components."""

        # Continue the processes
        self.message_resume_loader()
        self.message_resume_sorter()
        self.sorter_paused = False
        self.loader_paused = False
        self.stop_everything = False

    def message_set_loading_window_loader(self, loading_window:int):
        """Messaging loader to set value of loading_window.

        Args:
            loading_window (int): The value to set to the ``Loader``.

        """
        # Set the time window to starting loading data
        loading_window_message = {
            'header': 'UPDATE',
            'body': {
                'type': 'LOADING_WINDOW',
                'content': {
                    'loading_window': loading_window
                }
            }
        }
        self.message_to_loading_queue.put(loading_window_message)
    
    def message_pause_loader(self):
        """Messaging loader to pause."""

        # Sending the message to loader to pause!
        message = {
            'header': 'META',
            'body': {
                'type': 'PAUSE',
                'content': {},
            }
        }
        self.message_to_loading_queue.put(message)

    def message_pause_sorter(self):
        """Messaging sorter to pause."""

        # Sending the message to sorter to pause!
        message = {
            'header': 'META',
            'body': {
                'type': 'PAUSE',
                'content': {},
            }
        }
        self.message_to_sorting_queue.put(message)
    
    def message_resume_loader(self):
        """Messaging loader to resume."""

        # Sending the message to loader to resume!
        message = {
            'header': 'META',
            'body': {
                'type': 'RESUME',
                'content': {},
            }
        }
        self.message_to_loading_queue.put(message)
    
    def message_resume_sorter(self):
        """Messaging sorter to resume."""

        # Sending the message to sorter to resume!
        message = {
            'header': 'META',
            'body': {
                'type': 'RESUME',
                'content': {},
            }
        }
        self.message_to_sorting_queue.put(message)

    def message_end_loading_and_sorting(self):
        """Messaging loader and sorter to shutdown."""
        
        # Informing all process to end
        message = {
            'header': 'META',
            'body': {
                'type': 'END',
                'content': {},
            }
        }
        for message_to_queue in [self.message_to_sorting_queue, self.message_to_loading_queue]:
            message_to_queue.put(message)

    def respond_loader_message_timetrack(
            self, 
            timetrack:pd.DataFrame, 
            windows:list
        ):
        """Responding to loader's initialization message.

        Args:
            timetrack (pd.DataFrame): Collector's global timetrack.
            windows (list): List of windows (Window: namedtuple with 
            ``start`` and ``end`` attributes.)

        """
        self.timetrack = timetrack
        self.num_of_windows = len(windows)

    def respond_loader_message_counter(
            self, 
            uuid:str, 
            loading_window:int, 
            data_memory_usage:int
        ):
        """Respond to loader message about new loaded data chunk.

        This information includes the essential data memory usage.

        Args:
            uuid (str): Data chunk's uuid.
            loading_window (int): Loading window id.
            data_memory_usage (int): The size of the data chunk.

        """
        self.latest_window_loaded = loading_window
        self.loading_queue_memory_chunks[uuid] = data_memory_usage
        
        # Updating the ``Loader``'s loading bar.
        new_state = loading_window / len(self.windows)
        self.loading_bar.state = new_state

    def respond_loader_message_end(self):
        """Responding to the loader inform us that it finished!"""
        self.loader_finished = True

    def respond_sorter_message_counter(
            self, 
            uuid:str, 
            loaded_time:pd.Timedelta, 
            data_memory_usage:int
        ):
        """Responding to sorter informing batch of series data is loaded.

        Args:
            uuid (str): batch id.
            loaded_time (pd.Timedelta): The batch's latest timestamp.
            data_memory_usage (int): The size of the batch.

        """
        self.sorting_queue_memory_chunks[uuid] = data_memory_usage

        # Updating the ``Sorter``'s loading bar.
        new_state = loaded_time / self.end_time
        self.sorting_bar.state = new_state

    def respond_sorter_message_memory(self, uuid:str):
        """Respond to sorter inform that it ``put`` from loading queue.

        Args:
            uuid (str): The loading queue data chunk's uuid.
        
        """
        # Waiting until recorded memory usage is up-to-date.
        while uuid not in self.loading_queue_memory_chunks:
            print(f"Trying to delete {uuid} from {self.loading_queue_memory_chunks}")
            time.sleep(0.01)

        # Delete the data from the tracked memory.
        del self.loading_queue_memory_chunks[uuid]

    def respond_sorter_message_end(self):
        """Responding to sorter inform that it has ended sorting."""
        self.sorter_finished = True
        self.sorting_bar.state = 1

    @threaded
    def check_loader_messages(self):
        """Thread dedicated to listening for ``Loader`` messages.

        This thread also manages the ``Loader``'s memory consumption.

        """

        # Set the flag to check if new message
        loading_message = None
        loading_message_new = False
        
        # Constantly check for messages
        while not self.thread_exit.is_set():

            # Checking the loading message queue
            try:
                loading_message = self.message_from_loading_queue.get(timeout=0.01)
                loading_message_new = True
            except queue.Empty:
                loading_message_new = False

            # Processing new loading messages
            if loading_message_new:

                # Printing if verbose
                if self.verbose:
                    print("NEW LOADING MESSAGE: ", loading_message)

                # Obtain the function and execute it, by passing the 
                # message
                func = self.respond_message_protocol['LOADER'][loading_message['header']][loading_message['body']['type']]

                # Execute the function and pass the message
                func(**loading_message['body']['content'])

            # Check if the memory limit is passed
            self.sorter_memory_used = sum(list(self.sorting_queue_memory_chunks.values()))
            self.loader_memory_used = sum(list(self.loading_queue_memory_chunks.values()))
            self.total_memory_used = self.sorter_memory_used + self.loader_memory_used

            # Priting if verbose
            if self.verbose:
                print(f"LOADER [{self.loading_queue.qsize()}]: \
                    {self.loader_memory_used/self.total_available_memory}, \
                    SORTER [{self.sorting_queue.qsize()}]: \
                    {self.sorter_memory_used/self.total_available_memory}, \
                    RATIO: {self.total_memory_used / self.total_available_memory}")

            # If the system is supposed to continue operating, keep 
            # regulating the memory.
            if self.stop_everything is False:

                # Check if we need to pause the sorter
                if self.loader_memory_used > self.loader_memory_ratio * self.total_available_memory \
                    and not self.loader_paused:
                    # Pause the sorting and wait until memory is cleared!
                    self.message_pause_loader()
                    self.loader_paused = True
                elif self.loader_memory_used < self.loader_memory_ratio *self.total_available_memory \
                    and self.loader_paused:
                    self.message_resume_loader()
                    self.loader_paused = False

    @threaded
    def check_sorter_messages(self):
        """Thread dedicated to listening ``Sorter`` messages.

        This thread also manages the ``Sorter``'s memory consumption.
        
        """
        # Set the flag to check if new message
        sorting_message = None
        sorting_message_new = False
        
        # Constantly check for messages
        while not self.thread_exit.is_set():

            # Ckecking the sorting message queue
            try:
                sorting_message = self.message_from_sorting_queue.get(timeout=0.1)
                sorting_message_new = True
            except queue.Empty:
                sorting_message_new = False

            # Processing new sorting messages
            if sorting_message_new:
                
                # Printing if verbose
                if self.verbose:
                    print("NEW SORTING MESSAGE: ", sorting_message)

                # Obtain the function and execute it, by passing the 
                # message
                func = self.respond_message_protocol['SORTER'][sorting_message['header']][sorting_message['body']['type']]

                # Execute the function and pass the message
                func(**sorting_message['body']['content'])
            
            # Check if the memory limit is passed
            self.sorter_memory_used = sum(list(self.sorting_queue_memory_chunks.values()))

            # If the system is supposed to continue operating, keep 
            # regulating the memory.
            if self.stop_everything is False:

                # Check if we need to pause the sorter
                if self.sorter_memory_used > (1-self.loader_memory_ratio)*self.total_available_memory and not self.sorter_paused:
                    # Pause the sorting and wait until memory is cleared!
                    self.message_pause_sorter()
                    self.sorter_paused = True
                elif self.sorter_memory_used < (1-self.loader_memory_ratio)*self.total_available_memory and self.sorter_paused:
                    self.message_resume_sorter()
                    self.sorter_paused = False
 
    def get_meta(self):
        """Get the meta found in the ``logdir`` directory."""

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
            # meta_data['is_subsession'] = False

        # return total_meta
        return meta_data

    def load_data_streams(self, unique_users:List, users_meta:Dict) -> Dict[str, Sequence[DataStream]]:
        """Load the data streams found in the ``logdir``.

        Args:
            unique_users (List): The expected unique users.
            users_meta (Dict): The meta data for each user.

        Returns:
            Dict[str, Sequence[DataStream]]: Dictionary organized by 
            keys (names of the data streams) and values (list of data
            streams).

        """
        # Loading data streams
        users_data_streams = collections.defaultdict(list)
        for user, user_meta in tqdm.tqdm(zip(unique_users, users_meta), disable=not self.verbose):
            for index, row in user_meta.iterrows():

                # Extract useful meta
                entry_name = row['entry_name']
                dtype = row['dtype']

                # Construct the directory the file/directory is found
                if row['user'] != 'root':
                    file_dir = self.logdir / row['user']
                else:
                    file_dir = self.logdir
                
                # Load the data
                if dtype == 'video':
                    video_path = file_dir/f"{entry_name}.avi"
                    assert video_path.exists()
                    ds = VideoDataStream(
                        name=entry_name,
                        start_time=row['start_time'],
                        video_path=video_path
                    )
                elif dtype == 'image':

                    # Load the meta CSV
                    df = pd.read_csv(file_dir/entry_name/'timestamps.csv')

                    # Load all the images into a data frame
                    img_filepaths = []
                    for index, row in df.iterrows():
                        img_fp = file_dir / entry_name / f"{row['idx']}.jpg"
                        img_filepaths.append(img_fp)

                    df['img_filepaths'] = img_filepaths
                    
                    # Create ds
                    ds = TabularDataStream(
                        name=entry_name,
                        data=df,
                        time_column='_time_'
                    )
                elif dtype == 'tabular':
                    raise NotImplementedError("Tabular visualization is still not implemented.")
                else:
                    raise RuntimeError(f"{dtype} is not a valid option.")

                # Store the data in Dict
                users_data_streams[user].append(ds)

        return users_data_streams

    def create_entries_df(self, meta_data:Dict) -> pd.DataFrame:
        """Create the meta data frame of the entries.

        Args:
            meta_data (Dict): The original meta data of the entries.

        Returns:
            pd.DataFrame: The meta data frame of the entries.
        
        """
        # Construct pd.DataFrame for the data
        # For each session data, store entries by modality
        entries = collections.defaultdict(list)
        for session_name, session_data in meta_data['records'].items():
            for entry_name, entry_data in session_data.items():

                # For now, skip any tabular data since we don't have a way to visualize
                if entry_data['dtype'] == 'tabular':
                    continue

                entries['user'].append(session_name)
                entries['entry_name'].append(entry_name)
                entries['dtype'].append(entry_data['dtype'])
                entries['start_time'].append(pd.to_timedelta(entry_data['start_time']))
                entries['end_time'].append(pd.to_timedelta(entry_data['end_time']))
                # entries['is_subsession'].append(session_data['is_subsession'])

                if entries['end_time'][-1] > self.end_time:
                    self.end_time = entries['end_time'][-1]

        # Construct the dataframe
        entries = pd.DataFrame(dict(entries))

        return entries

    def meta_update(self):
        """The periodic function to check if meta data has changed.

        The goal is that eventually both the ``SingleRunner`` and
        ``GroupRunner`` will run along with the front-end to visualize,
        in real time, the output of the ``Pipeline``.

        """
        # In the update function, we need to check the data stored in 
        # the logdir.
        meta_data = self.get_meta()

        # If the new is different and not None, we need to update!
        if meta_data: 

            # If this is the first time loading data
            if type(self.meta_data) == type(None):

                # We need to determine the earliest start_time and the latest
                # end_time
                self.start_time = pd.Timedelta(seconds=0)
                self.end_time = pd.Timedelta(seconds=0)

                # Construct the entries in a pd.DataFrame
                self.entries = self.create_entries_df(meta_data)

                # Add the data to the dashboard
                self._dashboard_model.update_data(self.entries, self.sort_by)
                self._timetrack_model.update_data(self.entries, self.start_time, self.end_time)
                self.modelChanged.emit()

                # Loading the data for the Collector
                unique_users = self.entries['user'].unique()
                users_meta = [self.entries.loc[self.entries['user'] == x] for x in unique_users]

                # Reset index and drop columns
                for i in range(len(users_meta)):
                    users_meta[i].reset_index(inplace=True)
                    users_meta[i] = users_meta[i].drop(columns=['index'])

                # Load the data streams to later pass to the Loader
                self.users_data_streams = self.load_data_streams(unique_users, users_meta)

                # Setting up the data pipeline
                self.setup()
                
                # Update the flag tracking if data is loaded
                self._data_is_loaded = True
                self.dataLoadedChanged.emit()

            # If this is now updating already loaded data
            elif meta_data != self.meta_data:
                # TODO: add constant updating
                ...

        # The meta data is invalid or not present
        else:
            if self._data_is_loaded:
                self._data_is_loaded = False
                self.dataLoadedChanged.emit()
            
        # Then overwrite the records
        self.meta_data = meta_data
   
    def init_loader(self):
        """Initialize the ``Loader``."""
        
        # Then, create the data queues for loading and logging
        self.loading_queue = mp.Queue(maxsize=self.max_loading_queue_size)

        # Then create the message queues for the loading subprocess
        self.message_to_loading_queue = mp.Queue(maxsize=self.max_message_queue_size)
        self.message_from_loading_queue = mp.Queue(maxsize=self.max_message_queue_size)

        # Create variables to track the loader's data
        self.num_of_windows = 0
        self.latest_window_loaded = 0
        self.loader_finished = False
        self.loader_paused = False

        # Create the Loader with the specific parameters
        self.loader = Loader(
            loading_queue=self.loading_queue,
            message_to_queue=self.message_to_loading_queue,
            message_from_queue=self.message_from_loading_queue,
            users_data_streams=self.users_data_streams,
            time_window=self.time_window,
            verbose=self.verbose
        )
        
        # Storing the loading queues
        self.queues.update({
            'loading': self.loading_queue,
            'm_to_loading': self.message_to_loading_queue, 
            'm_from_loading': self.message_from_loading_queue,
        })

    def init_sorter(self):
        """Initialize the ``Sorter``."""
        # Create the queue for sorted data 
        self.sorting_queue = mp.Queue()
        
        # Then create the message queues for the sorting subprocess
        self.message_to_sorting_queue = mp.Queue(maxsize=self.max_message_queue_size)
        self.message_from_sorting_queue = mp.Queue(maxsize=self.max_message_queue_size)

        # Variables to track the sorter
        self.sorter_paused = False
        self.sorter_finished = False

        # Storing the sorting queues
        self.queues.update({
            'sorting': self.sorting_queue,
            'm_to_sorting': self.message_to_sorting_queue, 
            'm_from_sorting': self.message_from_sorting_queue,
        })
        
        # Creating a subprocess that takes the windowed data and loads
        # it to a queue sorted in a time-chronological fashion.
        self.sorter = Sorter(
            loading_queue=self.loading_queue,
            sorting_queue=self.sorting_queue,
            message_to_queue=self.message_to_sorting_queue,
            message_from_queue=self.message_from_sorting_queue,
            entries=self.entries,
            verbose=self.verbose
        )
    
    def setup(self):
        """Peform the application's complete setup."""
        
        # Changing the time_window_size based on the number of entries
        div_factor = min(len(self.entries), 10)
        self.time_window = pd.Timedelta(seconds=(self.time_window.value/1e9)/div_factor)
                
        # Get the necessary information to create the Collector 
        self.windows = get_windows(self.start_time, self.end_time, self.time_window)
        self.update_content_times = collections.deque(maxlen=100)
     
        # Starting the Manager's messaging thread and the content update thread
        self.check_loader_messages_thread = self.check_loader_messages()
        self.check_sorter_messages_thread = self.check_sorter_messages()
        self.update_content_thread = self.update_content()

        # Create container for all queues
        self.queues = {}

        # Starting the loader and sorter
        self.init_loader()
        self.init_sorter()

        # Start the threads
        self.check_loader_messages_thread.start()
        self.check_sorter_messages_thread.start()
        self.update_content_thread.start()

        # Start the processes
        self.loader.start()
        self.sorter.start()

    @threaded
    def update_content(self):
        """Main thread for updating the content on the dashboard.

        This thread grabs the output of the ``Sorter`` and uses it to 
        update the images in the dashboard. Here is the data flow.

        ``Loader`` --> ``Sorter`` --> ``update_content`` Thread

        """
        # Keep repeating until exiting the app
        while not self.thread_exit.is_set():

            # Only processing if we have the global timetrack
            if type(self.timetrack) == type(None):
                time.sleep(1)
                continue

            # Also, not processing if not playing or the session is complete
            if not self._is_play or self.session_complete:
                time.sleep(0.1)
                continue
 
            # Get the next entry information!
            try:
                data_chunk = self.sorting_queue.get(timeout=0.1)
            except queue.Empty:
                print('EMPTY')
                # Check if all the data has been loaded, if so that means
                # that all the data has been played
                if self.sorting_bar.state == 1:
                   
                    # Make that the session is complate and stop playing!
                    print("Session end detected!")
                    self.session_complete = True
                    self._is_play = False
                    self.playPauseChanged.emit()

                    # Also reset the content
                    self._dashboard_model.reset_content()
                    self.modelChanged.emit()
                
                # Regardless, continue
                continue
            
            # Get the time before updating
            tic = time.time()

            # Update the memory used by the sorting queue
            if data_chunk['uuid']:
                while data_chunk['uuid'] not in self.sorting_queue_memory_chunks:
                    time.sleep(0.01)
                del self.sorting_queue_memory_chunks[data_chunk['uuid']]
            
            # Compute the average time it takes to update content, and its ratio 
            # to the expect time, clipping at 1
            average_update_delta = sum(self.update_content_times)/max(1, len(self.update_content_times))

            # Calculate the delta between the current time and the entry's time
            time_delta = (data_chunk['entry_time'] - self.current_time).value / 1e9
            time_delta = max(0,time_delta-average_update_delta) # Accounting for updating
            # print(time_delta)
            time.sleep(time_delta)

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

            # Finally update the sliding bar
            self.update_content_times.append(delta)
    
    def exit(self):
        """Exiting the application.
        
        For subprocess to fully exit, their connecting queues must be
        completely empty. Therefore, below you will see plenty of 
        attempts and safety checks to ensure that the messaging and 
        data queues are complete empty before executing the ``join``
        method of the subprocesses.

        """
        print("Closing")

        # Only execute the thread and process ending if data is loaded
        if self._data_is_loaded:

            # Inform all the threads to end
            self.thread_exit.set()
            # print("Stopping threads!")

            self.check_loader_messages_thread.join()
            self.check_sorter_messages_thread.join()
            # print("Checking message thread join")
            
            self.update_content_thread.join()
            # print("Content update thread join")
            # self.current_time_update.stop()
            # print("Content timer stopped")

            self.message_end_loading_and_sorting()

            # Clearing Queues to permit the processes to shutdown
            # print("Clearing queues")
            for q_name, queue in self.queues.items():
                # print(f"Clearing {q_name} queue")
                clear_queue(queue)

            # Then wait for threads and process
            # print("Clearing loading queue again")
            while self.loading_queue.qsize():
                clear_queue(self.loading_queue)
                time.sleep(0.2)

            self.loader.join()
            # print("Loading process join")

            time.sleep(0.3)
            
            # Clearing Queues to permit the processes to shutdown
            # print("Clearing all queues again")
            for q_name, queue in self.queues.items():
                # print(f"clearing {q_name}.")
                clear_queue(queue)

            # print("Clearing sorting queue again")
            while self.sorting_queue.qsize():
                # print(self.sorting_queue.qsize())
                clear_queue(self.sorting_queue)
                time.sleep(0.2)

            self.sorter.join()
            # print("Sorting process join")

        # print("Finished closing")
