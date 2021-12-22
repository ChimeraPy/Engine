"""."""
__package__ = "pymmdt"

# Built-in Imports
from typing import Sequence, Optional, Dict, Any
import math
import collections

# Third-Party Imports
import pandas as pd
import tqdm

# Internal Imports
from .pipe import Pipe
from .data_stream import DataStream
from .session import Session
from .collector import Collector

class SingleWorker:
    
    def __init__(
            self,
            name:str,
            pipe:Pipe,
            data_streams:Sequence[DataStream],
            run_solo:Optional[bool]=False,
            session:Optional[Session]=None,
            time_window_size:Optional[pd.Timedelta]=None,
        ):

        # Store the information
        self.name = name
        self.pipe = pipe
        self.data_streams = data_streams
        self.session = session
        self.time_window_size = time_window_size
        self.run_solo = run_solo

        # If the worker is running by itself, it should be able to have 
        # its own collector.
        if self.run_solo:
            self.collector = Collector({self.name: self.data_streams})
            
            # The session is required for run solo
            assert isinstance(self.session, Session) and \
                isinstance(self.time_window_size, pd.Timedelta), \
                "When ``run_solo`` is set, ``session`` and ``time_window_size`` parameters are required."

    def set_session(self, session: Session) -> None:
        if isinstance(self.session, Session):
            raise RuntimeError(f"Worker <name={self.name}> has already as session")
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

        # After the data has propagated the entire pipe, flush the 
        # session logging.
        self.session.flush()

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
        
    def run(self, verbose=False) -> None:
        """Run the data pipeline.

        Args:
            verbose (bool): If to include logging and loading bar to
            help visualize the wait time until completion.

        """
        # Assertions
        assert isinstance(self.collector, Collector)

        # Start 
        self.start()

        # Determine how many time windows given the total time and 
        # size
        total_time = (self.collector.end - self.collector.start)
        num_of_windows = math.ceil(total_time / self.time_window_size)

        # Create unique namedtuple and storage for the Windows
        Window = collections.namedtuple("Window", ['start', 'end'])
        windows = []

        # For all the possible windows, calculate their start and end
        for x in range(num_of_windows):
            start = self.collector.start + x * self.time_window_size 
            end = self.collector.start + (x+1) * self.time_window_size
            capped_end = min(self.collector.end, end)
            window = Window(start, capped_end)
            windows.append(window)

        # Now that we have a list of window start and end times, iterate
        # over these values
        for window in tqdm.tqdm(windows, disable=not verbose):
            # Get the samples from the collector within the window
            all_data_samples = self.collector.get(window.start, window.end)

            # Forward propagate the window's data
            self.step(all_data_samples)

        # End 
        self.end()

class GroupWorker(SingleWorker):
    """Multimodal Data Processing Group Director.

    """

    def __init__(
            self, 
            name: str,
            pipe: Pipe,
            session: Session,
            workers: Sequence[SingleWorker],
            time_window_size: pd.Timedelta,
            data_streams: Optional[Sequence[DataStream]] = None,
            start_at: Optional[pd.Timedelta] = None,
            end_at: Optional[pd.Timedelta] = None,
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
        self.time_window_size = time_window_size
        self.data_streams = data_streams

        # Extract all the data streams from each worker and the entire group
        if data_streams:
            all_data_streams = {self.name: data_streams}
        else:
            all_data_streams = {}

        for worker in self.workers:
            all_data_streams[worker.name] = worker.data_streams 
        
        # Construct a collector based on the worker's datastreams
        self.collector = Collector(all_data_streams)
        
        # Providing each worker with a subsession
        for worker in self.workers:
            worker.set_session(self.session.create_subsession(worker.name))
            worker.set_collector(self.collector)

        # Apply triming if start_at or end_at has been selected
        self.start_time = pd.Timedelta(0)
        self.end_time = self.collector.end
        if type(start_at) != type(None) and isinstance(start_at, pd.Timedelta):
            self.start_time = start_at
        if type(end_at) != type(None) and isinstance(end_at, pd.Timedelta):
            self.end_time = end_at

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

        # After the data has propagated the entire pipe, flush the 
        # session logging.
        self.session.flush()

    def end(self) -> None:

        # Execute the workers' ``end`` routine
        for worker in self.workers:
            worker.end()

        # Execute its own start
        super().end()
