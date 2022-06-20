# Built-in Imports
from typing import Optional, List, Dict, Literal, Any
import multiprocessing as mp
import queue
import time
import logging

# Third-party Imports
import pandas as pd

# ChimeraPy Imports
from chimerapy.utils.tools import threaded, PortableQueue

# Logging
import logging
logger = logging.getLogger(__name__)


"""
bug while starting with following message:
_pickle.PicklingError: Can't pickle <function Process.check_messages at 0x115f70f70>: it's not the same object as chimerapy.core.process.Process.check_messages
/usr/local/Cellar/python@3.9/3.9.13_1/Frameworks/Python.framework/Versions/3.9/lib/python3.9/multiprocessing/reduction.py:60: PicklingError

This is because python multiprocessing uses spawn as default start method starting from 3.8, we change this to fork to make it work
"""
mp.set_start_method("fork")


# Resource:
# https://stackoverflow.com/questions/8489684/python-subclassing-multiprocessing-process
# https://stackoverflow.com/questions/67012978/accessing-a-subclass-method-of-a-multiprocessing-proxy-of-a-class
# https://stackoverflow.com/questions/35717109/python-class-object-sharing-between-processes-created-using-multiprocessing-modu
# https://stackoverflow.com/questions/39687235/share-python-object-between-multiprocess-in-python3
# https://superfastpython.com/share-process-attributes/

class Process(mp.Process):
    
    inputs = []

    def __init__(
            self, 
            name:str,
            inputs:Optional[List],
            verbose:bool=False
        ):
        # mp.Process __init__
        super().__init__()

        # Storing meta data
        self.name = name
        self.inputs = inputs
        self.verbose = verbose

        # Storing the data queues
        self.in_queue = PortableQueue(maxsize=1000)
        self.out_queue = PortableQueue(maxsize=1000)

        # Storing the message queues
        self.message_in_queue = PortableQueue(maxsize=1000)
        self.message_out_queue = PortableQueue(maxsize=1000)

        # Store all the queues in a container to later refer to all queues
        self.queues = [
            self.in_queue,
            self.out_queue,
            self.message_in_queue,
            self.message_out_queue
        ]
       
        # Process state information
        self.running = mp.Value('i', False)
        self.paused = mp.Value('i', False)

        # Create a mapping to messages and their respective functions
        self.message_to_functions = {
            'END': self.shutdown,
            'PAUSE': self.pause,
            'RESUME': self.resume
        }
        self.subclass_message_to_functions = {} # Overriden in subclass
        
        # Start the thread for checking for messages
        self.check_messages_thread = self.check_messages()
        # self.check_messages_thread.start()
 
    def __repr__(self):
        """Representation of ``Process``.
        
        Returns:
            str: The representation of ``Process``.

        """
        return f"{self.__class__.__name__}"

    def __str__(self):
        """Get String form of ``Process``.

        Returns:
            str: The string representation of ``Process``.

        """
        return self.__repr__()

    # Get and Set methods (necessory for shared memory multiprocessing
    def get_running(self):
        return self.running.value

    def shutdown(self):
        # Logging 
        logger.debug(f"{self.__class__.__name__}: teardown")
        
        # Notify other methods that teardown is ongoing
        self.running.value = False
        
        # Then ensure that the thread stops
        # self.check_messages_thread.join()

        # First, clear out all the queues
        for i, queue in enumerate(self.queues):
            logger.debug(f"{self.__class__.__name__}: clearing queue {i}")
            queue.destroy()

    def pause(self):
        """Pausing the main ``run`` routine of the process.
    
        For this to work, the implementation of the subprocesses' ``run`` \
            needs to incorporate the ``self.thread_pause`` variable.

        """
        # Setting the thread pause event
        self.paused.value = True

        # Logging
        logger.debug(f"{self.__class__.__name__}: paused")

    def resume(self):
        """Resuming the main ``run`` routine of the process.
    
        For this to work, the implementation of the subprocesses' ``run`` \
            needs to incorporate the ``self.thread_pause`` variable.

        """

        # Clearing the thread pause event
        self.paused.value = False

    def put(self, data_chunk:Any):

        # Forward the data_chunk to the queue
        self.in_queue.put(data_chunk)
        
        # Logging
        logger.debug(f"{self.__class__.__name__}: put")

    def get(self, timeout:float=None):
        
        # Logging
        logger.debug(f"{self.__class__.__name__}: get -> queue size: {self.out_queue.qsize()}")
        # Obtain the message from the from_queue
        if self.out_queue.qsize() <= 0:
            return None
        return self.out_queue.get(timeout=timeout)

    def put_message(self, message:Dict):

        # Forward the message to the messaging queue
        self.message_in_queue.put(message)
    
    def get_message(self, timeout:float=None):

        # Forward the message to the messaging queue
        return self.message_out_queue.get(timeout=timeout)

    def setup(self):
        """Setup function is inteded to be overwritten for custom setup."""
        ...
 
    def step(self, *args, **kwargs):
        """Apply process onto data sample.

        Raises:
            NotImplementedError: ``step`` method needs to be overwritten.

        """
        raise NotImplementedError("``step`` method needs to be implemented.")

    @threaded 
    def check_messages(self):
        """Creates the threaded function that can be called to ``start``."""

        # Set the flag to check if new message
        new_message = False
        message = None
        
        # Constantly check for messages
        while self.running.value:

            # Prevent blocking, as we need to check if the thread_exist
            # is set.
            while self.running.value:
                try:
                    message = self.message_in_queue.get(timeout=1)
                    new_message = True
                    break
                except queue.Empty:
                    new_message = False
                    time.sleep(0.05)
                    
            # Only process if new message is set and message exists
            if new_message and message:

                # Handle the incoming message
                if self.verbose:
                    logger.debug(f"{self.__class__.__name__} - NEW MESSAGE - {message}")

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

    def run(self):

        # logger.debug("run!")
        logger.debug('RUN!')
       
        # Run the setup
        self.setup()

        # Continously execute the following steps
        while self.running.value:

            logger.debug(f"while loop - {self.running.value}")
            if self.paused.value:
                time.sleep(0.5)
                continue
            
            data_chunk = None
            if self.in_queue: 
                try:
                    data_chunk = self.in_queue.get(timeout=1)
                except queue.Empty:
                    time.sleep(0.1)
                    continue

            output = self.step(data_chunk)
                
            # If we got an output, pass it to the queue
            if type(output) != type(None):
                # Keep trying to put the output into the output queue,
                # but prevent locking if shutdown
                while self.running.value:
                    try:
                        self.out_queue.put(output, timeout=1)
                        break
                    except queue.Full:
                        time.sleep(0.1)

        # Execute teardown
        self.shutdown()

    def join(self):
        # Only join if the process is running
        if self.is_alive():
            super().join()
