# Built-in Imports
import multiprocessing as mp
import threading
import queue
import time

# ChimeraPy Imports
from chimerapy.core.tools import threaded

# Resource:
# https://stackoverflow.com/questions/8489684/python-subclassing-multiprocessing-process

class BaseProcess(mp.Process):
    """Class that contains essentials and comms. for subprocesses.

    The purpose of ``BaseProcess`` is to be inherented by more concrete
    classes of subprocesses. This class provides the base for the 
    communication between the main process and the subprocesses. There
    are two message queues for sending and receiving messages. A thread 
    is generated to continuously check for new messages within this 
    ``BaseProcess``.

    """

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
        """Start the thread that checks for messages."""

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
        """Creates the threaded function that can be called to ``start``."""

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
                    print(f"{self.__class__.__name__} - NEW MESSAGE - {message}")

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
        """Pausing the main ``run`` routine of the process.
    
        For this to work, the implementation of the subprocesses' ``run`` \
            needs to incorporate the ``self.thread_pause`` variable.

        """
        # Setting the thread pause event
        self.thread_pause.set()

    def resume(self):
        """Resuming the main ``run`` routine of the process.
    
        For this to work, the implementation of the subprocesses' ``run`` \
            needs to incorporate the ``self.thread_pause`` variable.

        """

        # Clearing the thread pause event
        self.thread_pause.clear()

    def end(self):
        """Signal to stop the messaging thread"""

        # Setting the thread exit event
        self.thread_exit.set()

    def close(self):
        """Closing the subprocesses, with stopping the message thread."""

        # Waiting for the thread to end
        self.check_messages_thread.join()

        # Closing appropriately
        super().close()
