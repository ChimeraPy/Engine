"""."""
__package__ = "pymmdt"

# Built-in Imports
from typing import Sequence, Optional, Dict, Any

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
            name: str,
            pipe: Pipe,
            data_streams: Sequence[DataStream],
            session: Optional[Session] = None,
            run_solo: Optional[bool] = False
        ):

        # Store the information
        self.name = name
        self.pipe = pipe
        self.data_streams = data_streams
        self.session = session
        self.run_solo = run_solo

        # If the worker is running by itself, it should be able to have 
        # its own collector.
        if self.run_solo:
            self.collector = Collector({self.name: self.data_streams})

    def set_session(self, session: Session):
        if isinstance(self.session, Session):
            raise RuntimeError(f"Worker <name={self.name}> has already as session")
        else:
            self.session = session

    def set_collector(self, collector: Collector):
        self.collector = collector
    
    def start(self):
        assert isinstance(self.session, Session)
        
        # Attach the session to the pipe
        self.pipe.attach_session(self.session)
        self.pipe.attach_collector(self.collector)

        # First, execute the ``start`` routine of the pipe
        self.pipe.start()

    def step(self, data_samples: Dict[str, pd.DataFrame]):
        assert isinstance(self.session, Session)

        # Then process the sample
        self.pipe.step(data_samples)

        # After the data has propagated the entire pipe, flush the 
        # session logging.
        self.session.flush()
    
    def end(self):
        assert isinstance(self.session, Session)

        # Close the data streams
        for ds in self.data_streams:
            ds.close()

        # Closing components
        self.pipe.end()
        self.session.close()
        
    def run(self):
        assert self.run_solo == True, f"Worker <name={self.name}> needs to \
            have input parameter ``run_solo`` set to True to call ``run``."

class GroupWorker(SingleWorker):
    """Multimodal Data Processing and Analysis Coordinator. 

    Attributes:
        collector (pymmdt.Collector): The collector used to match the 
        timetracks of each individual data stream.

        pipe (pymmdt.Pipe): The pipeline used to propagate the samples
        from the data streams found in the collector.
        
    Todo:
        * Make the session have the option to save intermediate 
        data sample during the analysis, if the user request this feature.
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
        if type(start_at) != type(None) and isinstance(start_at, pd.Timedelta):
            self.collector.trim_before(start_at)
        if type(end_at) != type(None) and isinstance(end_at, pd.Timedelta):
            self.collector.trim_after(end_at)

    def start(self) -> None:
        
        # Execute the workers' ``start`` routine
        for worker in self.workers:
            worker.start()

        # Execute its own start
        super().start()

    def step(self, all_data_samples: Dict[str, Any]) -> None:

        # Get samples for all the workers and propgate them

        # Then execute its own while taking the output of the worker's
        # pipelines
        super().step()

    def end(self) -> None:

        # Execute the workers' ``end`` routine
        for worker in self.workers:
            worker.end()

        # Execute its own start
        super().end()

    def run(self, verbose=False) -> None:
        """Run the data pipeline.

        Args:
            verbose (bool): If to include logging and loading bar to
            help visualize the wait time until completion.

        """
        # Start 
        self.start()

        # Iterate through the collector
        for data_samples in tqdm.tqdm(self.collector, disable=not verbose):
            self.step(data_samples)

        # End 
        self.end()

