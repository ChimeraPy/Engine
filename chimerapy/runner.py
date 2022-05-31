# Built-in Imports
from typing import Sequence, Dict, Any, Union, List, Optional
import sys
import collections
import os
import curses
import time
import threading
import multiprocessing as mp
import queue
import pathlib
import signal

# Third-Party Imports
import psutil
import pandas as pd

# Internal Imports
from chimerapy.loader import Loader
from chimerapy.logger import Logger
from chimerapy.communication import RunnerCommunication
from chimerapy.core.session import Session
from chimerapy.core.tools import clear_queue, PortableQueue

class Runner:
    """Orchestracting class for running single data pipelines.

    The ``Runner`` class is tasked with managing and coordinating
    the data pipeline between all the classes. The ``Runner`` is 
    the main process for processing data.

    The ``Runner`` provides a TUI to keep track of the data
    pipeline processing and its progress. Additional information 
    includes the memory consumption by the system. Most of the 
    communication between the ``Runner``, ``Loader``, and ``Logger``
    is handled by communication threads in each process.

    The ``Runner`` also provides methods that start with 
    ``message_*`` or ``respond_*`` to are used to send or receive 
    messages from the ``Loader`` and ``Logger``. ``message_*`` are for
    sending messages. ``respond_*`` are for receiving and responding to
    messages.

    """
    
    def __init__(
            self,
            time_window:pd.Timedelta=pd.Timedelta(seconds=5),
            logdir:Optional[Union[str, pathlib.Path]]=None,
            start_time:Optional[pd.Timedelta]=None,
            end_time:Optional[pd.Timedelta]=None,
            memory_limit:float=0.8,
            memory_usage_factor:float=2.25,
            verbose=False,
        ):
        """

        Args:
            name (str): The name of the ``Runner`` that would be \
                used for storing the data generated during the processing.
            time_window (pd.Timedelta): The size of the time window.
            logdir (Optional[Union[str,pathlib.Path]]): The log directory \
                to use for saving and logging data to.
            start_time (Optional[pd.Timedelta]): A timestamp to define \
                the start time of the processing. Any data before the \
                ``start_time`` will be ignored.
            end_time (Optional[pd.Timedelta]): A timestamp to define \
                the end tim of the processing. Any data after the \
                ``end_time`` will be ignored.
            memory_limit (float): The percentage of the available RAM \
                that will be permitted to be used by the ``Runner``. \
                For slower computers and with small RAM, a smaller \
                ``memory_limit`` is highly recommended.
            memory_usage_factor (float): This factor is used to more \
                accurate account for the memory used by the library. Even \
                with tracking all memory loaded, the actual memory is\
                typically higher. The factor is to account for this \
                difference.
            verbose (bool): Debugging printout.

        """
        # Convert the logdir to pathlib
        if isinstance(logdir, str):
            self.logdir = pathlib.Path(logdir)
        else:
            self.logdir = logdir

        # Store the information
        self.verbose = verbose

        # Keep track of the number of processed data chunks
        self.num_processed_data_chunks = 0
        self.total_available_memory = memory_limit * psutil.virtual_memory().available
        self.memory_usage_factor = memory_usage_factor
        self.loading_queue_memory_chunks = {}
        self.logging_queue_memory_chunks = {}
        self.total_memory_used = 0

        # Keeping track of queues and placing the data streams inside 
        # a dictionary
        self.queues = {}
        self.users_data_streams = {f"{self.name}": self.data_streams}
        
        # Setup the loader
        self.init_loader(
            time_window,
            start_time,
            end_time
        )

        # Setup the logger
        self.init_logger()

        # Create session for runner
        self.session = Session(
            name='root',
            logging_queue=self.logging_queue
        )
        self.session.set_runner(self)

    def sigint_handler(self, signal, frame):
        self.thread_exit.set()

    def init_loader(
            self, 
            time_window:pd.Timedelta,
            start_time:Optional[pd.Timedelta],
            end_time:Optional[pd.Timedelta]
        ) -> None:
        """Routine for initializing the ``Loader``.

        Args:
            time_window (pd.Timedelta): Size of the time window.
            max_loading_queue_size (int): Max size of the loading queue.

            start_time (Optional[pd.Timedelta]): The cutoff of the start 
            time of the global timetrack.

            end_time (Optional[pd.Timedelta]): The cutoff of the end 
            time of the global timetrack.

        """
        # Then, create the data queues for loading and logging
        self.loading_queue = PortableQueue()

        # Then create the message queues for the loading subprocess
        self.message_to_loading_queue = PortableQueue()
        self.message_from_loading_queue = PortableQueue()

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
            time_window=time_window,
            start_time=start_time,
            end_time=end_time,
            verbose=self.verbose
        )
        
        # Storing the loading queues
        self.queues.update({
            'loading': self.loading_queue,
            'm_to_loading': self.message_to_loading_queue, 
            'm_from_loading': self.message_from_loading_queue,
        })
        
        # Handling keyboard interrupt
        signal.signal(signal.SIGINT, self.sigint_handler)

    def init_logger(
            self,
        ) -> None:
        """Routine for initializing the ``Logger``."""
        # Create the queue for the logging data
        self.logging_queue = PortableQueue()
       
        # Create the queues for the logger messaging 
        self.message_to_logging_queue = PortableQueue()
        self.message_from_logging_queue = PortableQueue()

        # Creating variables to track the logger's data
        self.num_of_logged_data = 0
        self.logger_finished = False

        # Create the logger
        self.logger = Logger(
            logdir=self.logdir,
            experiment_name=self.name,
            logging_queue=self.logging_queue,
            message_to_queue=self.message_to_logging_queue,
            message_from_queue=self.message_from_logging_queue,
            verbose=self.verbose
        )

        # Storing the logging queues
        self.queues.update({
            'logging': self.logging_queue,
            'm_to_logging': self.message_to_logging_queue,
            'f_to_logging': self.message_from_logging_queue
        })
        
        # Handling keyboard interrupt
        signal.signal(signal.SIGINT, self.sigint_handler)

    def set_session(self, session: Session) -> None:
        """Setting the session for ``Runner``.

        This function is called by the ``GroupRunner`` to provide 
        subsessions to the ``Runner``. Else, the ``Runner``
        creates its own session (when ``run_solo`` is set to True).

        Args:
            session (Session): The session used to log data.
        
        """
        # If there is already a session for the runner, this is not 
        # okay and we need to raise an alarm!
        if hasattr(self, 'session'):
            raise RuntimeError(f"Runner <name={self.name}> has already a Session")
        else:
            self.session = session

    def setup(self) -> None:
        """Setup the ``Runner``'s run.

        The setup includes the messaging threads, the ``Loader``, the
        ``Logger``, and the additional variables to coordinate these
        multiprocessing and multithreaded components.

        """
        # Creating threading Event to indicate stopping processing
        self.thread_exit = threading.Event()
        self.thread_exit.clear()
        
        # Create the communication handler
        self.comm = RunnerCommunication(
            self, 
            self.message_to_loading_queue,
            self.message_to_logging_queue,
            self.message_from_loading_queue,
            self.message_from_logging_queue
        )
        
        # Start the loader and logger
        self.loader.start()
        self.logger.start()

    def step(
            self, 
            data_samples: Dict[str, Dict[str, Union[pd.DataFrame, List[pd.DataFrame]]]]
        ) -> Any:
        """Routine for processing single step of data in the timeline.

        Args:
            data_samples (Dict[str, Dict[str, Union[pd.DataFrame, List[pd.DataFrame]]]]):
            The first level of the dictionary handles the 
            ``Runner``'s name (compatibility with the 
            ``GroupRunner``. The second level of the dictionary then
            uses (data stream name key, data frame data) pairs.

        Returns:
            Any: The output of the ``Pipeline`` can be later used by the 
            ``GroupRunner``. More details can be found in the ``step``
            method of the ``GroupRunner``.

        """
        # Then process the sample
        output = self.pipe.step(data_samples[self.name])

        # Return the pipe's output
        return output
    
    def shutdown(self) -> None:
        """Shutting down the ``Runner``.

        This shutdown also includes the messasing threads, ``Logger`` 
        and ``Loader`` processes, and clearing all the messages from the
        queues. Note: processes cannot be complete ``join`` until all
        connected queues are cleared.

        """
        # Stop the loader and logger
        self.comm.message_end_loading_and_logging() 

        # Wait until the Loader and logger are done, expect if the 
        # thread exit event has been already set.
        while not self.thread_exit.is_set():

            # Waiting 
            time.sleep(0.1)

            # Checking end condition
            # Break Condition
            # print(self.loader_finished, self.logger_finished)
            if self.loader_finished and \
                self.logger_finished:
                break

        # If the thread ended, we should stop the message thread
        self.thread_exit.set()

        # Clear out all mesages
        while self.loader.is_alive() or self.logger.is_alive():

            # Clear the queue if necessary
            for q_name, queue in self.queues.items():
                clear_queue(queue)
            
            # Joining the subprocesses
            self.loader.join(timeout=1)
            self.logger.join(timeout=1)

        # Waiting for the threads to shutdown
        self.comm.message_loader_thread.join()
        self.comm.message_logger_thread.join()

    def process_data(self):
        """Main routine for loading and processing data chunks.

        This routine acts as the base routine for ``get``ting data 
        chunks from the ``loading_queue`` and passing them through the
        ``Pipeline``.

        """
        # Keep track of the number of processed data chunks
        self.num_processed_data_chunks = 0
        
        # Continue iterating
        while not self.thread_exit.is_set():

            # Retrieveing sample from the loading queue
            data_chunk = self.loading_queue.get(block=True)
            
            # Check for end condition
            if data_chunk == 'END':
                break

            # Decompose the data chunk
            all_data_samples = data_chunk['data'] 
 
            # Now that the data has been removed from the queue, remove 
            # this from the memory used
            while data_chunk['uuid'] not in self.loading_queue_memory_chunks:
                time.sleep(0.01)
            del self.loading_queue_memory_chunks[data_chunk['uuid']]
            
            # Then propagate the sample throughout the pipe
            self.step(all_data_samples)

            # Increase the counter
            self.num_processed_data_chunks += 1
        
    def tui_main(self, stdscr):
        """Routine for TUI.

        The routine for the TUI updates the information from each
        process, such as the ``Loader`` and ``Logger``. Additionally,
        the memory usage is reported as well.

        """
        # Enable scrolling and setting timeouts
        stdscr.scrollok(1)
        stdscr.timeout(1)

        # Outside of while loop, generate useful setup
        process_stats = {
            'Runner': psutil.Process(os.getpid()),
            'Loader': psutil.Process(self.loader.pid),
            'Logger': psutil.Process(self.logger.pid)
        }

        # Continue the TUI until the other threads are complete.
        while not self.thread_exit.is_set():

            # Generate the report for the threads of the Loader
            cpu_usage_string = ''
            processes_cpu_info = collections.defaultdict(dict)

            # For each process, generate a string about their process cpu
            # consumption and thread cpu consumption
            for i, (process, stats) in enumerate(process_stats.items()):

                # Check if the process is still alive
                if not stats.is_running():
                    # Add terminal info
                    if i == 0:
                        cpu_usage_string += f"{process}: Terminal\n"
                    else:
                        cpu_usage_string += f"\t\t    {process}: Terminal\n"
                    # Skip the rest of for loop
                    continue

                # Get process data
                try:
                    processes_cpu_info[process]['process'] = process_stats[process].cpu_percent() / psutil.cpu_count()
                except psutil.NoSuchProcess:
                    continue

                # Append process string to cpu_usage_string
                if i == 0:
                    cpu_usage_string += f"{process}: {processes_cpu_info[process]['process']:.2f}\n"
                else:
                    cpu_usage_string += f"\t\t    {process}: {processes_cpu_info[process]['process']:.2f}\n"

            # Create information string
            data_pipeline_info_str = f"""\
            Data Pipeline Information
                Loading:
                    Loaded Data: {self.latest_window_loaded}/{self.num_of_windows}
                    Loading Queue Size: {self.loading_queue.qsize()}
                Processing: 
                    Processed Data: {self.num_processed_data_chunks}/{self.num_of_windows}
                Logging:
                    Logged Data: {self.num_of_logged_data}
                    Logging Queue Size: {self.logging_queue.qsize()}
            System Information:
                Memory Usage: {(self.total_memory_used/self.total_available_memory):.2f}
                CPU Usage: 
                    {cpu_usage_string}
            """

            # Info about data loading
            stdscr.addstr(0,0, data_pipeline_info_str)

            # Refresh the screen
            stdscr.refresh()

            # Sleep
            time.sleep(0.1)

            # Break Condition
            if self.loader_finished and \
                self.num_processed_data_chunks == self.num_of_windows and \
                self.logger_finished:
                break
    
    def run(self, verbose:bool=False) -> None:
        """Run the data pipeline.

        Args:
            verbose (bool): If to include a TUI for tracking the \
                progress of loading, processing, and logging of data \
                throughout the data pipeline process.

        """
        # Performing setup for the subprocesses and threads
        self.setup()
 
        # If verbose, create a simple TUI showing the current state of 
        # the whole process.
        if verbose:
            tui_thread = threading.Thread(target=curses.wrapper, args=(self.tui_main,))
            tui_thread.start()

        # Execute the processing in the main thread
        self.process_data()
        
        # Then fully shutting down the subprocesses and threads
        self.shutdown()
