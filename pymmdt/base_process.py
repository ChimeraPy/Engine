from typing import List, Dict, Sequence
import multiprocessing as mp
import threading
import collections
import queue
import time
import sys

# Third-party imports
from PIL import Image
import numpy as np
import tqdm
import pandas as pd

# PyMMDT Library
from .core.collector import Collector
from .core.tools import threaded, clear_queue, to_numpy
from .core.video import VideoDataStream
from .core.tabular import TabularDataStream

# Resource:
# https://stackoverflow.com/questions/8489684/python-subclassing-multiprocessing-process

class BaseProcess(mp.Process):

    def __init__(
            self, 
            message_to_queue:mp.Queue,
            message_from_queue:mp.Queue,
            verbose:bool=False
        ):
        super().__init__()

        # Storing the message queues
        self.message_to_queue = message_to_queue
        self.message_from_queue = message_from_queue
        
        # Saving other parameters
        self.verbose = verbose

        # Create a mapping to messages and their respective functions
        self.message_to_functions = {
            'END': self.end,
            'PAUSE': self.pause,
            'RESUME': self.resume
        }
        self.subclass_message_to_functions = {} # Overriden in subclass

    def setup(self):

        # Pausing information
        self.thread_pause = threading.Event()
        self.thread_pause.clear()

        # Closing information
        self.thread_exit = threading.Event()
        self.thread_exit.clear()

        # Create the thread that checks messages
        self.check_messages_thread = self.check_messages()
        self.check_messages_thread.start()
    
    @threaded 
    def check_messages(self):

        # Set the flag to check if new message
        new_message = False
        message = None
        
        # Constantly check for messages
        while not self.thread_exit.is_set():
            
            # Prevent blocking, as we need to check if the thread_exist
            # is set.
            while not self.thread_exit.is_set():
                try:
                    message = self.message_to_queue.get(timeout=1)
                    new_message = True
                    break
                except queue.Empty:
                    new_message = False
                    time.sleep(0.1)
                    
            # Only process if new message is set and message exists
            if new_message and message:

                # Handle the incoming message
                if self.verbose:
                    print(f"NEW MESSAGE - {message}")

                # META --> General
                if message['header'] == 'META':

                    # Determine the type of message and interpret it
                    func = self.message_to_functions[message['body']['type']]

                # else --> Specific on the process
                else:

                    # Determing the type of message by the subclasses's 
                    # mapping
                    func = self.subclass_message_to_functions[message['body']['type']]

                # After obtaining the function, execute it
                func(**message['body']['content'])

    def pause(self):

        # Setting the thread pause event
        self.thread_pause.set()

    def resume(self):

        # Clearing the thread pause event
        self.thread_pause.clear()

    def end(self):

        # Setting the thread exit event
        self.thread_exit.set()

    def close(self):

        # Waiting for the thread to end
        self.check_messages_thread.join()

        # Closing appropriately
        super().close()
