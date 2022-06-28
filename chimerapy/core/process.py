# Built-in Imports
from socket import timeout
from typing import Optional, List, Dict, Literal, Any
import multiprocessing as mp
import queue
import time
import logging

# Third-party Imports
import pandas as pd
from chimerapy.core.data_chunk import DataChunk

# ChimeraPy Imports
from chimerapy.utils.tools import threaded
from chimerapy.core.queue import PortableQueue

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
    def __init__(
            self, 
            name:str,
            verbose:bool=False,
        ):
        # mp.Process __init__
        super().__init__()

        # Storing meta data
        self.name = name
        self.verbose = verbose

        # the graph modules populates the in_queues and out_queues
        self.in_queues = []
        self.out_queues = []

        # Storing the message queues
        self.message_in_queue = PortableQueue(maxsize=1000)
        self.message_out_queue = PortableQueue(maxsize=1000)

        # Store all the queues in a container to later refer to all queues
        self.queues = [
            self.message_in_queue,
            self.message_out_queue
        ]
       
        # Process state information
        self.running = mp.Value('i', True)
        self.paused = mp.Value('i', False)
        
        # note: this is not shared amongst other processes and only is used by this process
        self._time_stamp = 0

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
        # # logger.debug(f"{self.__class__.__name__}: teardown")
        
        # Notify other methods that teardown is ongoing
        self.running.value = False

        # First, clear out the in_queues. in_queues because no data will be transmitted from inques any further
        for i, queue in enumerate(self.in_queues):
            # # logger.debug(f"{self.__class__.__name__}: clearing queue {i}")
            queue.destroy()
        
        # Execute teardown
        self.join()

    def pause(self):
        """Pausing the main ``run`` routine of the process.
    
        For this to work, the implementation of the subprocesses' ``run`` \
            needs to incorporate the ``self.thread_pause`` variable.

        """
        # Setting the thread pause event
        self.paused.value = True

        # Logging
        # # logger.debug(f"{self.__class__.__name__}: paused")

    def resume(self):
        """Resuming the main ``run`` routine of the process.
    
        For this to work, the implementation of the subprocesses' ``run`` \
            needs to incorporate the ``self.thread_pause`` variable.

        """

        # Clearing the thread pause event
        self.paused.value = False

    def is_put_ready(self):
        # make sure all out queues are ready
        ready = all(que.is_put_ready() for que in self.out_queues)
        # # logger.debug(f"Process {self.name} is put ready {ready} (y)")
        return ready
    
    def is_get_ready(self):
        # make sure all out queues are ready
        # since all the in_queues can only be accessed by this process exclusively,
        # we won't have to deal with mutual exclusion
        if len(self.in_queues) == 0:
            return True

        ready = all(que.is_get_ready() for que in self.in_queues)
        return ready

    def put(self, data_chunk:DataChunk):
        # Forward the data_chunk to the queue
        # # logger.debug(f"{self.name} is putting {data_chunk.data}")
        for que in self.out_queues:
            que.put(data_chunk, timeout=0.1)

        # Logging
        # # logger.debug(f"{self.__class__.__name__}: put")
        return True

    def get(self, timeout:float=None):
        # Obtain the message from the in_queues
        values = []
        for que in self.in_queues:
            # logger.debug(f"RUNNING!! {self.name}, {que} getting data")
            data = que.get(timeout=timeout)
            # logger.debug(f"RUNNING!! {self.name}, {que} got data, {data}")
            values.append(data)

        return values

    def put_message(self, message:Dict):

        # Forward the message to the messaging queue
        self.message_in_queue.put(message)
    
    def get_message(self, timeout:float=None):

        # Forward the message to the messaging queue
        return self.message_out_queue.get(timeout=timeout)

    def setup(self):
        """Setup function is inteded to be overwritten for custom setup."""
        pass
 
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
                    message = self.message_in_queue.get(timeout=0.1)
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
        # # logger.debug(f"RUNNING!! {self.name}")
        # Run the setup
        self.setup()

        # Continously execute the following steps
        while self.running.value:
            # # logger.debug(f"while loop - {self.running.value}")
            if self.paused.value:
                time.sleep(0.5)
                continue

            data_chunks = [DataChunk(self.name, None)]
            if len(self.in_queues) > 0:
                while not self.is_get_ready():
                    time.sleep(0.01)
                    continue
                data_chunks = self.get(timeout=0.1)

            if all(chunk.data == "END" for chunk in data_chunks):
                output = "END"
            else:
                output = self.step(data_chunks)

            output = DataChunk(self.name, output, self._time_stamp)
            # # logger.debug(f"RUNNING!! {self.name} got output, {output}")
            if len(self.out_queues) > 0:
                while not self.is_put_ready():
                    time.sleep(0.01)
                    continue
                
                # # logger.debug(f"{self.name} is put ready (y)")
                self._time_stamp += 1
                # # logger.debug(f"{self.name} is putting output")
                
                self.put(output)

            # # logger.debug(f"RUNNING!! {self.name} wrote output")
            if output.data == "END":
                self.running.value = False
            

        self.wrapup()

    def wrapup(self):
        """
        Executed after the process has finished running
        """
        pass

    def join(self):
        # Only join if the process is running
        if self.is_alive():
            super().join(timeout=0.1)

