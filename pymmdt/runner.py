"""Module focused on the ``Runner`` implementation.

Contains the following classes:
    ``Runner``

"""

# Package Management
__package__ = 'pymmdt'

# Built-in Imports
from typing import Sequence, Optional, Dict, Any
import collections

# Third Party Imports
import tqdm
import pandas as pd

# Internal Imports
from .data_stream import DataStream
from .pipe import Pipe
from .process import Process
from .collector import Collector 
from .process import Process
from .session import Session

class Runner:
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
            data_streams: Sequence[DataStream],
            pipe: Pipe,
            session: Session,
            time_window_size: pd.Timedelta,
            start_at: Optional[pd.Timedelta] = None,
            end_at: Optional[pd.Timedelta] = None,
        ) -> None:
        """Construct the analyzer. 

        Args:
            data_streams (Sequence[DataStream]): A list of data streams to process forward.
            pipe (Pipe): The pipeline to send forward the data samples from the data streams toward.

        """
        # Create a collector that organizes the chronological of data from 
        # the data streams
        self.name = name
        self.collector = Collector(data_streams, time_window_size)
        self.pipe = pipe
        self.session = session
        self.all_dtypes = [ds.name for ds in data_streams]

        # Apply triming if start_at or end_at has been selected
        if type(start_at) != type(None) and isinstance(start_at, pd.Timedelta):
            self.collector.trim_before(start_at)
        if type(end_at) != type(None) and isinstance(end_at, pd.Timedelta):
            self.collector.trim_after(end_at)

    def start(self) -> None:
        
        # Create data sample slots in the session
        self.all_samples = {}
        for dtype in self.all_dtypes:
            self.all_samples[dtype] = None, None # sample, time

        # Attach the session to the pipe
        self.pipe.attach_session(self.session)
        self.pipe.attach_collector(self.collector)

        # First, execute the ``start`` routine of the pipe
        self.pipe.start()

    def step(self, data_samples: Dict[str, Any]) -> None:

        # Then process the sample
        self.pipe.step(data_samples)

        # After the data has propagated the entire pipe, flush the 
        # session logging.
        self.session.flush()

    def end(self) -> None:

        # Then execute the ``end`` routine of the pipe
        self.pipe.end()

        # Closing the session
        self.session.close()

        # Then close all the streams in the collector
        self.collector.close()

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

