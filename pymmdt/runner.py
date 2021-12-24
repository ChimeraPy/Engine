"""."""
__package__ = "pymmdt"

# Built-in Imports
from typing import Sequence, Optional, Dict, Any
import time
import threading

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
            time_window_size:Optional[pd.Timedelta]=None,
            start_at:Optional[pd.Timedelta]=None,
            end_at:Optional[pd.Timedelta]=None,
            verbose:Optional[bool]=False
        ):

        # Store the information
        self.name = name
        self.pipe = pipe
        self.data_streams = data_streams
        self.session = session
        self.run_solo = run_solo
        self.verbose = verbose

        # If the worker is running by itself, it should be able to have 
        # its own collector.
        if self.run_solo:
            # The session is required for run solo
            assert isinstance(self.session, Session) and \
                isinstance(time_window_size, pd.Timedelta), \
                "When ``run_solo`` is set, ``session`` and ``time_window_size`` parameters are required."

            self.collector = Collector(
                {self.name: self.data_streams},
                time_window_size,
                verbose=verbose
            )
            
            # Apply triming if start_at or end_at has been selected
            if type(start_at) != type(None) and isinstance(start_at, pd.Timedelta):
                self.collector.set_start_time(start_at)
            if type(end_at) != type(None) and isinstance(end_at, pd.Timedelta):
                self.collector.set_end_time(end_at)

            # Setup the collector thread
            self.setup_collector_thread()
 
    def setup_collector_thread(self):
        
        # Tell the Collector to start creating its loading data thread
        self.collector.start()

        # Create a thread for ``run``.
        self.processing_thread = self.process_data()
        self._thread_exit = threading.Event()
        self._thread_exit.clear() # Set as False in the beginning
            
    def set_session(self, session: Session) -> None:
        if isinstance(self.session, Session):
            raise RuntimeError(f"Runner <name={self.name}> has already as session")
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

    def step(self, data_samples: Dict[str, Dict[str, pd.DataFrame]]) -> Any:
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

    def get_and_step(self):

        # Get data from the loading data queue from the Collector
        # Forward propagate the window's data
        while True:
            # If the thread is supposed to exit, we should end it
            if self._thread_exit.is_set():
                all_data_samples = 'END'
                break
           
            # Check the queue before getting the item
            elif self.collector.loading_queue.qsize() != 0:
                # Get the data from the loading queue
                all_data_samples = self.collector.loading_queue.get(block=True)
                break
           
            # else, there is no safe sample, wait 
            else:
                time.sleep(0.1)
                continue

        # Check for end condition
        if all_data_samples == 'END':
            return True

        # Then propagate the sample throughout the pipe
        self.step(all_data_samples)

        # Continue
        return False

    @threaded
    def process_data(self):

        # Continue iterating
        while True:
            # Process and sample - determine if it should be ended
            should_end = self.get_and_step()
            # If so, break and end the thread
            if should_end:
                break
        
    def run(self) -> None:
        """Run the data pipeline.

        Args:
            verbose (bool): If to include logging and loading bar to
            help visualize the wait time until completion.

        """
        # Assertions
        assert isinstance(self.collector, Collector)

        # Start 
        self.start()

        # Start the thread and wait until its complete
        self.processing_thread.start()
        self.processing_thread.join()

        # End 
        self.end()

class GroupRunner(SingleRunner):
    """Multimodal Data Processing Group Director.

    """

    def __init__(
            self, 
            name: str,
            pipe: Pipe,
            session: Session,
            workers: Sequence[SingleRunner],
            time_window_size: pd.Timedelta,
            data_streams: Optional[Sequence[DataStream]] = None,
            start_at: Optional[pd.Timedelta] = None,
            end_at: Optional[pd.Timedelta] = None,
            verbose:Optional[bool]=False
        ) -> None:
        """Construct the analyzer. 

        Args:
            data_streams (Sequence[DataStream]): A list of data streams to process forward.
            pipe (Pipe): The pipeline to send forward the data samples from the data streams toward.

        """
        # Save hyperparameters
        self.name = name
        self.workers = workers
        self.pipe = pipe
        self.session = session
        self.data_streams = data_streams
        self.verbose = verbose

        # Extract all the data streams from each worker and the entire group
        if data_streams:
            all_data_streams = {self.name: data_streams}
        else:
            all_data_streams = {}

        for worker in self.workers:
            all_data_streams[worker.name] = worker.data_streams 
        
        # Construct a collector based on the worker's datastreams
        self.collector = Collector(all_data_streams, time_window_size, verbose=verbose)
        
        # Apply triming if start_at or end_at has been selected
        if type(start_at) != type(None) and isinstance(start_at, pd.Timedelta):
            self.collector.set_start_time(start_at)
        if type(end_at) != type(None) and isinstance(end_at, pd.Timedelta):
            self.collector.set_end_time(end_at)

        # Setup the collector thread
        self.setup_collector_thread()

        # Providing each worker with a subsession
        for worker in self.workers:
            worker.set_session(self.session.create_subsession(worker.name))
            worker.set_collector(self.collector)

    def start(self) -> None:
        
        # Execute the workers' ``start`` routine
        for worker in self.workers:
            worker.start()

        # Execute its own start
        super().start()

    def step(self, all_data_samples: Dict[str, Dict[str, pd.DataFrame]]) -> None:

        # Get samples for all the workers and propgate them
        for worker in self.workers:
            output = worker.step({worker.name: all_data_samples[worker.name]})
            all_data_samples[worker.name]['_output_'] = output

        # Then process the sample
        self.pipe.step(all_data_samples)

    def end(self) -> None:

        # Execute the workers' ``end`` routine
        for worker in self.workers:
            worker.end()

        # Execute its own start
        super().end()
