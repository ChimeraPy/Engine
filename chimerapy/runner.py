# Built-in Imports
from typing import Sequence, Dict, Any, Union, List, Optional
import curses
import time
import threading
import multiprocessing as mp
import queue
import pathlib

# Third-Party Imports
import psutil
import pandas as pd

# Internal Imports
from .loader import Loader
from .logger import Logger
from .core.pipeline import Pipeline
from .core.data_stream import DataStream
from .core.session import Session
from .core.tools import threaded, PortableQueue

class SingleRunner:
    """Orchestracting class for running single data pipelines.

    The ``SingleRunner`` class is tasked with managing and coordinating
    the data pipeline between all the classes. The ``SingleRunner`` is 
    the main process for processing data, defined by the flow and logic
    defined in the ``Pipeline``. 

    The ``SingleRunner`` provides a TUI to keep track of the data
    pipeline processing and its progress. Additional information 
    includes the memory consumption by the system. Most of the 
    communication between the ``SingleRunner``, ``Loader``, and ``Logger``
    is handled by communication threads in each process.

    The ``SingleRunner`` also provides methods that start with 
    ``message_*`` or ``respond_*`` to are used to send or receive 
    messages from the ``Loader`` and ``Logger``. ``message_*`` are for
    sending messages. ``respond_*`` are for receiving and responding to
    messages.

    There are a distinction between ``SingleRunner`` and ``GroupRunner`` 
    for there intended use. The ``SingleRunner` is focused on single 
    participant or object of study data pipelines. ``GroupRunner`` is 
    used when there are participants or tracked elements.

    """
    
    def __init__(
            self,
            name:str,
            pipe:Pipeline,
            data_streams:Sequence[DataStream],
            run_solo:bool=False,
            time_window:pd.Timedelta=pd.Timedelta(seconds=5),
            logdir:Optional[Union[str, pathlib.Path]]=None,
            start_time:Optional[pd.Timedelta]=None,
            end_time:Optional[pd.Timedelta]=None,
            max_loading_queue_size:int=100,
            max_logging_queue_size:int=1000,
            max_message_queue_size:int=100,
            memory_limit:float=0.8,
            memory_usage_factor:float=2.25,
            verbose=False,
        ):
        """

        Args:
            name (str): The name of the ``SingleRunner`` that would be \
                used for storing the data generated during the processing.
            pipe (Pipeline): The pipeline to be used by the \
                ``SingleRunner``.
            data_streams (Sequence[DataStream]): The data streams to be \
                feed into the data pipeline.
            run_solo (bool): Flag variable indicating if the \
                ``SingleRunner`` will be run separately or in a group \
                by the ``GroupRunner``.
            time_window (pd.Timedelta): The size of the time window.
            logdir (Optional[Union[str,pathlib.Path]]): The log directory \
                to use for saving and logging data to.
            start_time (Optional[pd.Timedelta]): A timestamp to define \
                the start time of the processing. Any data before the \
                ``start_time`` will be ignored.
            end_time (Optional[pd.Timedelta]): A timestamp to define \
                the end tim of the processing. Any data after the \
                ``end_time`` will be ignored.
            max_loading_queue_size (int): Max size of the loading queue.
            max_logging_queue_size (int): Max size of the logging queue.
            max_message_queue_size (int): Max size of the message queues.
            memory_limit (float): The percentage of the available RAM \
                that will be permitted to be used by the ``SingleRunner``. \
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
        self.name = name
        self.pipe = pipe
        self.data_streams = data_streams
        self.run_solo = run_solo
        self.max_loading_queue_size = max_loading_queue_size
        self.max_logging_queue_size = max_logging_queue_size
        self.max_message_queue_size = max_message_queue_size
        self.verbose = verbose

        # Keep track of the number of processed data chunks
        self.num_processed_data_chunks = 0
        self.total_available_memory = memory_limit * psutil.virtual_memory().available
        self.memory_usage_factor = memory_usage_factor
        self.loading_queue_memory_chunks = {}
        self.logging_queue_memory_chunks = {}
        self.total_memory_used = 0

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
            'LOGGER':{
                'UPDATE': {
                    'COUNTER': self.respond_logger_message_counter
                },
                'META': {
                    'END': self.respond_logger_message_end
                }
            }
        }

        # If the runner is running by itself, it should be able to have 
        # its own collector.
        if self.run_solo:

            # If running solo, then a logdir should be provided
            assert isinstance(self.logdir, pathlib.Path), f"param ``logdir`` \
                is required when ``run_solo`` is set to ``True``."

            # Keeping track of queues and placing the data streams inside 
            # a dictionary
            self.queues = {}
            self.users_data_streams = {f"{self.name}": self.data_streams}
            
            # Setup the loader
            self.init_loader(
                time_window,
                self.max_loading_queue_size, 
                start_time,
                end_time
            )

            # Setup the logger
            self.init_logger(
                self.max_logging_queue_size,
            )

            # Create session for runner
            self.session = Session(
                name='root',
                logging_queue=self.logging_queue
            )
            self.session.set_runner(self)
            self.pipe.set_session(self.session)

    def init_loader(
            self, 
            time_window:pd.Timedelta,
            max_loading_queue_size:int,
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
        self.loading_queue = PortableQueue(maxsize=max_loading_queue_size)

        # Then create the message queues for the loading subprocess
        self.message_to_loading_queue = PortableQueue(maxsize=self.max_message_queue_size)
        self.message_from_loading_queue = PortableQueue(maxsize=self.max_message_queue_size)

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

    def init_logger(
            self,
            max_logging_queue_size:int,
        ) -> None:
        """Routine for initializing the ``Logger``.

        Args:
            max_logging_queue_size (int): Max size of the logging queue.

        """
        # Create the queue for the logging data
        self.logging_queue = PortableQueue(maxsize=max_logging_queue_size)
       
        # Create the queues for the logger messaging 
        self.message_to_logging_queue = PortableQueue(maxsize=self.max_message_queue_size)
        self.message_from_logging_queue = PortableQueue(maxsize=self.max_message_queue_size)

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

    def message_pause_loader(self):
        """Message to pause the ``Loader``."""

        # Sending the message to loader to pause!
        pause_message = {
            'header': 'META',
            'body': {
                'type': 'PAUSE',
                'content': {},
            }
        }
        self.message_to_loading_queue.put(pause_message)
    
    def message_resume_loader(self):
        """Message to resume the ``Loader``."""

        # Sending the message to loader to pause!
        pause_message = {
            'header': 'META',
            'body': {
                'type': 'RESUME',
                'content': {},
            }
        }
        self.message_to_loading_queue.put(pause_message)

    def message_end_loading_and_logging(self):
        """Message to terminate the ``Loader`` and ``Logging``."""

        # Sending message to loader and logger to stop!
        end_message = {
            'header': 'META',
            'body': {
                'type': 'END',
                'content': {},
            }
        }
        self.message_to_loading_queue.put(end_message)
        self.message_to_logging_queue.put(end_message)

    def respond_loader_message_timetrack(self, timetrack:pd.DataFrame, windows:list):
        """Respond to the retrieved message with initialization info.

        Args:
            timetrack (pd.DataFrame): The global timetrack generated by
            the ``Collector``.

            windows (list): List of the windows with their start and end
            times.

        """
        self.timetrack = timetrack
        self.num_of_windows = len(windows)

    def respond_loader_message_counter(
            self, 
            uuid:str, 
            loading_window:int, 
            data_memory_usage:int
        ):
        """Respond to update in ``Loader`` window counter.

        This message is vital for its memory usage information. We need
        to track this memory consumption to when we need to limit memory
        usage.

        Args:
            uuid (str): The unique id of the loaded data. This id helps
            track the memory of each loaded data chunk and note when
            the memory has been return to the OS.

            loading_window: The id of the loading window.

            data_memory_usage: The number of bytes used by the loaded
            data chunk.

        """
        self.latest_window_loaded = loading_window
        self.loading_queue_memory_chunks[uuid] = data_memory_usage

    def respond_logger_message_counter(
            self, 
            num_of_logged_data:int, 
            uuid:str
        ):
        """Respond to update in ``Logger`` logged data.

        Args:
            num_of_logged_data (int): The number of logged data chunks.
            This is used in the TUI to provide a overview of the system.

            uuid (str): The unique id of the logged data chunk. This id
            helps track the memory deleted by the logger.

        """
        self.num_of_logged_data = num_of_logged_data

        # Sometimes the uuid has not been added yet so we need to wait
        while uuid not in self.logging_queue_memory_chunks:
            time.sleep(0.01)

        del self.logging_queue_memory_chunks[uuid]

    def respond_loader_message_end(self):
        """Respond to the ``Loader`` inform that it has ended."""
        self.loader_finished = True

    def respond_logger_message_end(self):
        """Respond to the ``Logger`` inform that it has ended."""
        self.logger_finished = True
    
    @threaded
    def check_loader_messages(self):
        """Message thread checks for new messages and limit memory usage.
        
        The ``Loader`` message queues are checked frequently and if 
        there is a new message, then the corresponding function is 
        executed. These functions are defined by the protocols layed out 
        in the ``__init__``. 

        Given the memory usage information from the ``Loader`` and 
        ``Logger``, this thread enforces the memory limit by pausing and
        resuming the operation of each process. ``Loader`` consumes
        memory while ``Logger`` frees the RAM memory back to the system 
        to use.

        """
        # Set the flag to check if new message
        loading_message = None
        loading_message_new = False
        
        # Constantly check for messages
        while not self.thread_exit.is_set():

            # Checking the loading message queue
            try:
                loading_message = self.message_from_loading_queue.get(timeout=0.1)
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
            self.total_memory_used = self.memory_usage_factor * (sum(list(self.logging_queue_memory_chunks.values())) + sum(list(self.loading_queue_memory_chunks.values())))

            # Reporting memory usage if debugging
            if self.verbose:
                print(f"TMU: {self.total_memory_used}, AVAILABLE: {self.total_available_memory}, RATIO: {self.total_memory_used / self.total_available_memory}")

            if self.total_memory_used > self.total_available_memory and not self.loader_paused:
                # Pause the loading and wait until memory is cleared!
                self.message_pause_loader()
                self.loader_paused = True
            elif self.total_memory_used < self.total_available_memory and self.loader_paused:
                # resume the loading and wait until memory is cleared!
                self.message_resume_loader()
                self.loader_paused = False

    @threaded
    def check_logger_messages(self):
        """Message thread checks for new messages and limit memory usage.
        
        The ``Logger`` message queues are checked frequently and if 
        there is a new message, then the corresponding function is 
        executed. These functions are defined by the protocols layed out 
        in the ``__init__``. 

        Given the memory usage information from the ``Loader`` and 
        ``Logger``, this thread enforces the memory limit by pausing and
        resuming the operation of each process. ``Loader`` consumes
        memory while ``Logger`` frees the RAM memory back to the system 
        to use.

        """
        # Set the flag to check if new message
        logging_message = None
        logging_message_new = False
        
        # Constantly check for messages
        while not self.thread_exit.is_set():
            
            # Checking the logging message queue
            try:
                logging_message = self.message_from_logging_queue.get(timeout=0.1)
                logging_message_new = True
            except queue.Empty:
                logging_message_new = False

            # Processing new sorting messages
            if logging_message_new:

                # Obtain the function and execute it, by passing the 
                # message
                func = self.respond_message_protocol['LOGGER'][logging_message['header']][logging_message['body']['type']]

                # Execute the function and pass the message
                func(**logging_message['body']['content'])

    def set_session(self, session: Session) -> None:
        """Setting the session for ``SingleRunner``.

        This function is called by the ``GroupRunner`` to provide 
        subsessions to the ``SingleRunner``. Else, the ``SingleRunner``
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
        """Setup the ``SingleRunner``'s run.

        The setup includes the messaging threads, the ``Loader``, the
        ``Logger``, and the additional variables to coordinate these
        multiprocessing and multithreaded components.

        """
        # Creating threading Event to indicate stopping processing
        self.thread_exit = threading.Event()
        self.thread_exit.clear()
        
        # Begin the thread receiving messages
        self.message_loader_thread = self.check_loader_messages()
        self.message_logger_thread = self.check_logger_messages()
        self.message_loader_thread.start()
        self.message_logger_thread.start()
        
        # Start the loader and logger
        self.loader.start()
        self.logger.start()

    def start(self) -> None:
        """Start the data pipeline and its processing.

        This routine is focuses on the pipeline passed to the
        ``SingleRunner``, while the ``setup`` method is more for the 
        other components.

        """
        # set the session to the pipe
        self.pipe.set_session(self.session)

        # First, execute the ``start`` routine of the pipe
        self.pipe.start()

    def step(
            self, 
            data_samples: Dict[str, Dict[str, Union[pd.DataFrame, List[pd.DataFrame]]]]
        ) -> Any:
        """Routine for processing single step of data in the timeline.

        Args:
            data_samples (Dict[str, Dict[str, Union[pd.DataFrame, List[pd.DataFrame]]]]):
            The first level of the dictionary handles the 
            ``SingleRunner``'s name (compatibility with the 
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
    
    def end(self) -> None:
        """Ending the ``SingleRunner``."""
        
        # Closing components
        self.pipe.end()

    def shutdown(self) -> None:
        """Shutting down the ``SingleRunner``.

        This shutdown also includes the messasing threads, ``Logger`` 
        and ``Loader`` processes, and clearing all the messages from the
        queues. Note: processes cannot be complete ``join`` until all
        connected queues are cleared.

        """
        # Stop the loader and logger
        self.message_end_loading_and_logging() 

        # Now we have to wait until the logger is done
        while True:

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
        
        # Joining the subprocesses
        self.loader.join()
        self.logger.join()

        # Waiting for the threads to shutdown
        self.message_loader_thread.join()
        self.message_logger_thread.join()

    def process_data(self):
        """Main routine for loading and processing data chunks.

        This routine acts as the base routine for ``get``ting data 
        chunks from the ``loading_queue`` and passing them through the
        ``Pipeline``.

        """
        # Keep track of the number of processed data chunks
        self.num_processed_data_chunks = 0
        
        # Continue iterating
        while True:

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
        # Continue the TUI until the other threads are complete.
        while True:

            # Create information string
            info_str = f"""\
            Loading:
                Loaded Data: {self.latest_window_loaded}/{self.num_of_windows}
                Loading Queue Size: {self.loading_queue.qsize()}/{self.max_loading_queue_size}
            Processing: 
                Processed Data: {self.num_processed_data_chunks}/{self.num_of_windows}
            Logging:
                Logged Data: {self.num_of_logged_data}
                Logging Queue Size: {self.logging_queue.qsize()}/{self.max_logging_queue_size}
            System Information:
                Memory Usage: {(self.total_memory_used/self.total_available_memory):.2f}
            """

            # Info about data loading
            stdscr.addstr(0,0, info_str)

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
        # Assertions
        assert isinstance(self.session, Session)

        # Performing setup for the subprocesses and threads
        self.setup()
 
        # Start the Runner
        self.start()
 
        # If verbose, create a simple TUI showing the current state of 
        # the whole process.
        if verbose:
            tui_thread = threading.Thread(target=curses.wrapper, args=(self.tui_main,))
            tui_thread.start()

        # Execute the processing in the main thread
        self.process_data()
        
        # End 
        self.end()

        # Then fully shutting down the subprocesses and threads
        self.shutdown()

class GroupRunner(SingleRunner):
    """Orchestracting class for running multiple data pipelines.

    The ``GroupRunner`` inherents and thereby uses plenty of methods 
    from ``SingleRunner``. The main difference is that ``GroupRunner``
    manages the processing of multiple ``SingleRunner`` and then an 
    additional group data pipeline. The output of each ``SingleRunner``
    can then be used as the input to the group data pipeline.

    """

    def __init__(
            self, 
            logdir:Union[str, pathlib.Path],
            name:str,
            pipe:Pipeline,
            runners:Sequence[SingleRunner],
            time_window:pd.Timedelta,
            data_streams:Sequence[DataStream]=[],
            start_time:pd.Timedelta=None,
            end_time:pd.Timedelta=None,
            max_loading_queue_size:int=100,
            max_logging_queue_size:int=1000,
            max_message_queue_size:int=100,
            memory_limit:float=0.8,
            memory_usage_factor:float=2.25,
            verbose:bool=False,
        ) -> None:
        """Construct the ``GroupRunner``.

        Args:
            logdir (Union[str, pathlib.Path]): The logging directory to\
                save the logged data.
            name (str): The name of the ``GroupRunner`` that will be \
                used to create a unique logging directory.
            pipe (Pipeline): The group pipeline that will process all output \
                content of each input runners.
            runners (Sequence[SingleRunner]): List of runners to run \
                concurrently.
            time_window (pd.Timedelta): Size of the time window.
            data_streams (Sequence[DataStream]): The data streams that \
                correlate to the group instead of any ``SingleRunner``.
            start_time (pd.Timedelta): The start time of the entire \
                global timeline.
            end_time (pd.Timedelta): The end time of the entire global \
                timeline.
            max_loading_queue_size (int): Max loading queue size
            max_logging_queue_size (int): Max logging queue size
            max_message_queue_size (int): Max message queue size
            memory_limit (float): The percentage of available memory \
                permitted for the ``GroupRunner`` to use. The limit \
                restricts overloading the system.

            memory_usage_factor (float): The recorded memory usage of \
                the system does not fully capture its memory footprint. To \
                account for this possibly memory overloading and system \
                crash, a factor is used to compensate for this as a safety \
                measure.

            verbose (bool): Debugging printout.

        """
        # Convert the logdir to pathlib
        if isinstance(logdir, str):
            self.logdir = pathlib.Path(logdir)
        else:
            self.logdir = logdir

        # Save hyperparameters
        self.name = name
        self.pipe = pipe
        self.data_streams = data_streams
        self.runners = runners
        self.max_loading_queue_size = max_loading_queue_size
        self.max_logging_queue_size = max_logging_queue_size
        self.max_message_queue_size = max_message_queue_size
        self.verbose = verbose
        
        # Keep track of the number of processed data chunks
        self.num_processed_data_chunks = 0
        self.memory_usage_factor = memory_usage_factor
        self.total_available_memory = memory_limit * psutil.virtual_memory().available
        self.loading_queue_memory_chunks = {}
        self.logging_queue_memory_chunks = {}
        self.total_memory_used = 0

        # Extract all the data streams from each runner and the entire group
        if data_streams:
            self.users_data_streams = {self.name: data_streams}
        else:
            self.users_data_streams = {}

        for runner in self.runners:
            self.users_data_streams[runner.name] = runner.data_streams 
        
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
            'LOGGER':{
                'UPDATE': {
                    'COUNTER': self.respond_logger_message_counter
                },
                'META': {
                    'END': self.respond_logger_message_end
                }
            }
        }
            
        # Keeping track of queues and placing the data streams inside 
        # a dictionary
        self.queues = {}
        
        # Setup the loader
        self.init_loader(
            time_window,
            max_loading_queue_size, 
            start_time,
            end_time
        )

        # Setup the logger
        self.init_logger(
            max_logging_queue_size,
        )

        # Creating sessions for the runners
        self.session = Session(
            name='root',
            logging_queue=self.logging_queue
        )
        
        # Providing each runner with a subsession
        for runner in self.runners:
            runner_session = Session(
                name=runner.name,
                logging_queue=self.logging_queue
            )

            # Connect the session to the group runner to track memory usage
            runner_session.set_runner(self)

            # Connect session to runner to make logging tagged by the user
            runner.set_session(runner_session)

            # Connect the session to the pipe since that's how the session
            # is interfaced.
            runner.pipe.set_session(runner_session)
        
    def start(self) -> None:
        """Start the data pipelines and their processing.

        This routine is focuses on the pipeline passed to the
        ``GroupRunner`` and its ``SingleRunner`` instances, 
        while the ``setup`` method is more for the other components.

        """
        # Execute the runners' ``start`` routine
        for runner in self.runners:
            runner.start()

        # Execute its own start
        super().start()

    def step(self, all_data_samples: Dict[str, Dict[str, Union[pd.DataFrame, List[pd.DataFrame]]]]) -> None:
        """One time step to processing ``get``ing data and processing it.

        Args:
            all_data_samples (Dict[str, Dict[str, Union[pd.DataFrame, List[pd.DataFrame]]]]): 
            These are all the data samples for each runner. The first 
            level of the dictionary is organized by the ``name`` of the
            ``SingleRunner`` and ``GroupRunner``. Then, the second level
            of the dictionary is organized by the ``name`` of the data
            streams. The value in the second level is the data frame
            acquired from the respective data stream.

            An important distinction between ``SingleRunner`` and 
            ``GroupRunner`` is that the output of each ``SingleRunner``
            is then passed to the ``GroupRunner`` to further analyze
            the conjuction of all the ``SingleRunner`` instances.

        """
        # Get samples for all the runners and propagate them
        for runner in self.runners:
            # Get the output of the each runner
            output = runner.step({runner.name: all_data_samples[runner.name]})
            all_data_samples[runner.name]['_output_'] = output

        # Then process the sample in the group pipeline
        self.pipe.step(all_data_samples)

    def end(self) -> None:
        """Ending the data pipelines."""

        # Execute the runners' ``end`` routine
        for runner in self.runners:
            runner.end()

        # Execute its own start
        super().end()
