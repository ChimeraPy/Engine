"""."""
__package__ = "pymmdt"

# Built-in Imports
from typing import Sequence, Optional, Dict, Any, Union, List
import curses
import time
import threading
import queue

# Third-Party Imports
import pandas as pd
import tqdm

# Internal Imports
from .pipe import Pipe
from .data_stream import DataStream
from .session import Session
from .collector import Collector
from .tools import threaded

class SingleRunner:
    
    def __init__(
            self,
            name:str,
            pipe:Pipe,
            data_streams:Sequence[DataStream],
            run_solo:Optional[bool]=False,
            session:Optional[Session]=None,
            time_window_size:Optional[pd.Timedelta]=pd.Timedelta(seconds=5),
            start_at:Optional[pd.Timedelta]=None,
            end_at:Optional[pd.Timedelta]=None,
            max_loading_queue_size:Optional[int]=100,
            max_logging_queue_size:Optional[int]=1000,
        ):

        # Store the information
        self.name = name
        self.pipe = pipe
        self.data_streams = data_streams
        self.session = session
        self.run_solo = run_solo
        
        # Keep track of the number of processed data chunks
        self.num_processed_data_chunks = 0

        # If the runner is running by itself, it should be able to have 
        # its own collector.
        if self.run_solo:
            # The session is required for run solo
            assert isinstance(self.session, Session) and isinstance(time_window_size, pd.Timedelta), \
                "When ``run_solo`` is set, ``session`` and ``time_window_size`` parameters are required."

            self.collector = Collector(
                {self.name: self.data_streams},
                time_window_size,
                start_at=start_at,
                end_at=end_at,
            )
 
            # Setup all threads
            self.setup_threads_and_queues(
                max_loading_queue_size, 
                max_logging_queue_size
            )

    def setup_threads_and_queues(
            self, 
            max_loading_queue_size:int,
            max_logging_queue_size:int
        ) -> None:
        assert isinstance(self.collector, Collector)
        assert isinstance(self.session, Session)
        
        # Create a queue used by the process to stored loaded data
        self.loading_queue = queue.Queue(maxsize=max_loading_queue_size)
        
        # Create a thread for loading data and storing in a Queue
        self.loading_thread = self.collector.load_data_to_queue()
        
        # Create a thread for Runner's ``run``.
        self.processing_thread = self.process_data()

        # Create a queue and thread for I/O logging in separate thread
        self.logging_queues = [queue.Queue(maxsize=max_logging_queue_size) for session in [self.session] + self.session.subsessions]
        self.logging_threads = [session.load_data_to_log() for session in [self.session] + self.session.subsessions]

        # Provide the collector and session their respective queue
        self.collector.set_loading_queue(self.loading_queue)
        self.session.set_logging_queue(self.logging_queues[0])
        for i, session in enumerate(self.session.subsessions):
            session.set_logging_queue(self.logging_queues[i+1])

        # Create a threading.Event to tell all threads to stop
        self._thread_exit = threading.Event()
        self._thread_exit.clear() # Set as False in the beginning

        # Attach the exit event to the collector and the session
        self.session.set_thread_exit(self._thread_exit)
        for session in self.session.subsessions:
            session.set_thread_exit(self._thread_exit)
            
    def set_session(self, session: Session) -> None:
        if isinstance(self.session, Session):
            raise RuntimeError(f"Runner <name={self.name}> has already a Session")
        else:
            self.session = session

    def set_collector(self, collector: Collector) -> None:
        self.collector = collector
    
    def start(self) -> None:
        assert isinstance(self.session, Session)
        assert isinstance(self.collector, Collector)
         
        # Attach the session to the pipe
        self.pipe.attach_session(self.session)
        self.pipe.attach_collector(self.collector)

        # First, execute the ``start`` routine of the pipe
        self.pipe.start()

    def step(self, data_samples: Dict[str, Dict[str, Union[pd.DataFrame, List[pd.DataFrame]]]]) -> Any:
        assert isinstance(self.session, Session)

        # Then process the sample
        output = self.pipe.step(data_samples)

        # Return the pipe's output
        return output
    
    def end(self) -> None:
        assert isinstance(self.session, Session)

        # Close the data streams
        for ds in self.data_streams:
            ds.close()

        # Closing components
        self.pipe.end()
        self.session.close()

    @threaded
    def process_data(self):

        # Keep track of the number of processed data chunks
        self.num_processed_data_chunks = 0

        # Continue iterating
        while True:

            # Retrieveing sample from the loading queue
            all_data_samples = self.loading_queue.get(block=True)
           
            # Check for end condition
            if all_data_samples == 'END':
                break

            # Then propagate the sample throughout the pipe
            self.step(all_data_samples)

            # Increase the counter
            self.num_processed_data_chunks += 1

        # If the thread ended, we should stop some processes
        self._thread_exit.set()

    def tui_main(self, stdscr):
        
        # Assertions
        assert isinstance(self.collector, Collector)
        assert isinstance(self.session, Session)

        # Continue the TUI until the other threads are complete.
        while True:

            # Create information string
            info_str = f"""\
            Loaded Data: {self.collector.windows_loaded}/{len(self.collector.windows)}
            Loading Queue Size: {self.loading_queue.qsize()}/{self.loading_queue.maxsize}
            Processed Data: {self.num_processed_data_chunks}/{len(self.collector.windows)}
            Session:
                {self.session.experiment_name}
                    Logging Queue: {self.logging_queues[0].qsize()}/{self.logging_queues[0].maxsize}
                    Logged Data: {self.session.num_of_logged_data}
            """

            # Info about data loading
            stdscr.addstr(0,0, info_str)

            # Refresh the screen
            stdscr.refresh()

            # Sleep
            time.sleep(0.1)

            # Break Condition
            if self.collector.windows_loaded == len(self.collector.windows) and \
                self.num_processed_data_chunks == len(self.collector.windows) and \
                self.logging_queues[0].qsize() == 0:
                break

    def run(self, verbose:bool=False) -> None:
        """Run the data pipeline.

        Args:
            verbose (bool): If to include logging and loading bar to
            help visualize the wait time until completion.

        """
        # Assertions
        assert isinstance(self.collector, Collector)
        assert isinstance(self.session, Session)
        
        # Start 
        self.start()

        # And start the threads
        self.loading_thread.start()
        self.processing_thread.start()
        for thread in self.logging_threads:
            thread.start()
        
        # If verbose, create a simple TUI showing the current state of 
        # the whole process.
        if verbose:
            curses.wrapper(self.tui_main)
        
        # Then wait until the threads is complete!
        self.processing_thread.join()
        self.loading_thread.join()

        # Closing the logging threads after the end, just in case
        # logging occurs in the end.
        for thread in self.logging_threads:
            thread.join()
        
        # End 
        self.end()

class GroupRunner(SingleRunner):
    """Multimodal Data Processing Group Director.

    """

    def __init__(
            self, 
            name:str,
            pipe:Pipe,
            session:Session,
            runners:Sequence[SingleRunner],
            time_window_size:pd.Timedelta,
            data_streams:Optional[Sequence[DataStream]]=[],
            start_at:Optional[pd.Timedelta]=None,
            end_at:Optional[pd.Timedelta]=None,
            max_loading_queue_size:Optional[int]=100,
            max_logging_queue_size:Optional[int]=1000,
        ) -> None:
        """Construct the analyzer. 

        Args:
            data_streams (Sequence[DataStream]): A list of data streams to process forward.
            pipe (Pipe): The pipeline to send forward the data samples from the data streams toward.

        """
        # Save hyperparameters
        self.name = name
        self.runners = runners
        self.pipe = pipe
        self.session = session
        self.data_streams = data_streams
        
        # Keep track of the number of processed data chunks
        self.num_processed_data_chunks = 0

        # Extract all the data streams from each runner and the entire group
        if data_streams:
            all_data_streams = {self.name: data_streams}
        else:
            all_data_streams = {}

        for runner in self.runners:
            all_data_streams[runner.name] = runner.data_streams 
        
        # Construct a collector based on the runner's datastreams
        self.collector = Collector(
            all_data_streams,
            time_window_size,
            start_at=start_at,
            end_at=end_at,
        )
        
        # Providing each runner with a subsession
        for runner in self.runners:
            runner.set_session(self.session.create_subsession(runner.name))
            runner.set_collector(self.collector)
        
        # Setup all threads
        self.setup_threads_and_queues(
            max_loading_queue_size, 
            max_logging_queue_size
        )
  
    def start(self) -> None:
        
        # Execute the runners' ``start`` routine
        for runner in self.runners:
            runner.start()

        # Execute its own start
        super().start()

    def step(self, all_data_samples: Dict[str, Dict[str, Union[pd.DataFrame, List[pd.DataFrame]]]]) -> None:

        # Get samples for all the runners and propagate them
        for runner in self.runners:
            # Get the output of the each runner
            output = runner.step({runner.name: all_data_samples[runner.name]})
            all_data_samples[runner.name]['_output_'] = output

        # Then process the sample
        self.pipe.step(all_data_samples)

    def end(self) -> None:

        # Execute the runners' ``end`` routine
        for runner in self.runners:
            runner.end()

        # Execute its own start
        super().end()

    def tui_main(self, stdscr):
        
        # Assertions
        assert isinstance(self.collector, Collector)
        assert isinstance(self.session, Session)

        # Continue the TUI until the other threads are complete.
        while True:

            # Create information string
            info_str = f"""\
            Loaded Data: {self.collector.windows_loaded}/{len(self.collector.windows)}
            Loading Queue Size: {self.loading_queue.qsize()}/{self.loading_queue.maxsize}
            Processed Data: {self.num_processed_data_chunks}/{len(self.collector.windows)}
            Session:
            """

            # Create each session's information and appending to it
            for id, session in enumerate([self.session] + self.session.subsessions):
                session_str = f"""
                {session.experiment_name}
                    Logging Queue: {self.logging_queues[id].qsize()}/{self.logging_queues[id].maxsize}
                    Logged Data: {session.num_of_logged_data}
                """
                info_str += session_str

            # Info about data loading
            stdscr.addstr(0,0, info_str)

            # Refresh the screen
            stdscr.refresh()

            # Sleep
            time.sleep(0.1)

            # Break Condition
            if self.collector.windows_loaded == len(self.collector.windows) and \
                self.num_processed_data_chunks == len(self.collector.windows):

                # Check all the logging queues
                ready_queues = [x.qsize() == 0 for x in self.logging_queues]

                # If all ready, then end!
                if all(ready_queues):
                    break

