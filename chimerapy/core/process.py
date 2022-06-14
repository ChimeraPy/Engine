# Built-in Imports
from typing import Optional, List, Dict, Literal, Any
import multiprocessing as mp
import threading
import queue
import time
import logging

# Third-party Imports
import pandas as pd

# ChimeraPy Imports
from chimerapy.utils.tools import threaded, PortableQueue, clear_queue

# Logging
logger = logging.getLogger(__name__)

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
            run_type:Literal['proactive', 'reactive']='reactive',
            verbose:bool=False
        ):
        # mp.Process __init__
        super().__init__()

        # Storing meta data
        self.name = name
        self.inputs = inputs
        self.run_type = run_type
        self.verbose = verbose

        # Storing the data queues
        self.data_to_queue = PortableQueue()
        self.data_from_queue = PortableQueue()

        # Storing the message queues
        self.message_to_queue = PortableQueue()
        self.message_from_queue = PortableQueue()

        # Store all the queues in a container to later refer to all queues
        self.queues = [
            self.data_to_queue,
            self.data_from_queue,
            self.message_to_queue,
            self.message_from_queue
        ]
       
        # Process state information
        self.to_shutdown = mp.Value('i', False)
        self.paused = mp.Value('i', False)
        self.is_running = mp.Value('i', False)

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
    def get_shutdown(self):
        return self.to_shutdown.value

    def shutdown(self):
        print(f'{self.__class__.__name__} - shutdown')
        self.to_shutdown.value = True
    
    def pause(self):
        """Pausing the main ``run`` routine of the process.
    
        For this to work, the implementation of the subprocesses' ``run`` \
            needs to incorporate the ``self.thread_pause`` variable.

        """
        # Setting the thread pause event
        self.paused.value = True

        # Logging
        print(f"{self.__class__.__name__}: paused")

    def resume(self):
        """Resuming the main ``run`` routine of the process.
    
        For this to work, the implementation of the subprocesses' ``run`` \
            needs to incorporate the ``self.thread_pause`` variable.

        """

        # Clearing the thread pause event
        self.paused.value = False

    def put(self, data_chunk:Any):

        # Forward the data_chunk to the queue
        self.data_to_queue.put(data_chunk)
        
        # Logging
        print(f"{self.__class__.__name__}: put")

    def get(self, timeout:float=None):
        
        # Logging
        print(f"{self.__class__.__name__}: get")

        # Obtain the message from the from_queue
        return self.data_from_queue.get(timeout=timeout)

    def put_message(self, message:Dict):

        # Forward the message to the messaging queue
        self.message_to_queue.put(message)
    
    def get_message(self, timeout:float=None):

        # Forward the message to the messaging queue
        return self.message_from_queue.get(timeout=timeout)

    def setup(self):
        """Setup function is inteded to be overwritten for custom setup."""
        ...

    def teardown(self):

        # Logging 
        print(f"{self.__class__.__name__}: teardown")
        
        # Notify other methods that teardown is ongoing
        self.to_shutdown.value = True
        
        # Then ensure that the thread stops
        # self.check_messages_thread.join()

        # First, clear out all the queues
        for i, queue in enumerate(self.queues):
            print(f"{self.__class__.__name__}: clearing queue {i}")
            queue.destroy()
 
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
        while not self.to_shutdown.value:

            # Prevent blocking, as we need to check if the thread_exist
            # is set.
            while not self.to_shutdown.value:
                try:
                    message = self.message_to_queue.get(timeout=1)
                    new_message = True
                    break
                except queue.Empty:
                    new_message = False
                    time.sleep(0.05)
                    
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

    def run(self):

        # print("run!")
        print('RUN!')

        # Change the state of the process
        self.is_running.value = True
       
        # Run the setup
        self.setup()

        # Continously execute the following steps
        while not self.to_shutdown.value:

            print(f"while loop - {self.to_shutdown.value}")

            # Check if the loading is halted
            if self.paused.value:
                time.sleep(0.5)
                continue

            if self.run_type == 'reactive': # consumer

                # Execute the step, keep trying every 0.1 second apart
                try:
                    data_chunk = self.data_to_queue.get(timeout=1)
                except queue.Empty:
                    time.sleep(0.1)
                    continue
 
                # If we got a message, step through it
                output = self.step(data_chunk)

                # If we got an output, pass it to the queue
                if type(output) != type(None):
                    
                    # Keep trying to put the output into the output queue,
                    # but prevent locking if shutdown
                    while not self.to_shutdown.value:
                        try:
                            self.data_from_queue.put(output, timeout=1)
                        except queue.Empty:
                            time.sleep(0.1)

            elif self.run_type == 'proactive': # producer
                
                # If we got a message, step through it
                output = self.step()
                
                # If we got an output, pass it to the queue
                if type(output) != type(None):

                    # Keep trying to put the output into the output queue,
                    # but prevent locking if shutdown
                    while not self.to_shutdown.value:
                        try:
                            self.data_from_queue.put(output, timeout=1)
                        except queue.Empty:
                            time.sleep(0.1)

            else:
                raise RuntimeError(f"Invalid ``run_type``: {self.run_type}\
                    - should be either 'proactive' or 'reactive'.") 

        # Execute teardown
        self.teardown()

    def join(self):

        # Only join if the process is running
        if self.is_alive():
            super().join()
