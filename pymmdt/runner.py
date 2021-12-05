"""Module focused on the ``Runner`` implementation.

Contains the following classes:
    ``Runner``

"""

# Package Management
__package__ = 'pymmdt'

# Built-in Imports
from typing import Sequence, Union, Iterable
import collections

# Third Party Imports
import tqdm

# Internal Imports
from .data_stream import DataStream
from .pipe import Pipe
from .process import Process
from .collector import Collector 
from .process import Process
from .session import Session

class Runner:
    """Multimodal Data Processing and Analysis Coordinator. 

    The Runner is the coordinator between the other modules. First,
    the analyzer determines the flow of data from the data streams
    stored in the collector and the given processes. Once the analyzer
    constructs the data flow graph, the pipelines for the input 
    data stream are determined and stored.

    Then, when feeding new data samples, the analyzer send them to
    the right pipeline and its processes. The original data sample and
    its subsequent generated data sample in the pipeline are stored
    in the Session as a means to keep track of the latest sample.

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
            data_streams: Sequence[DataStream],
            pipe: Pipe,
            session: Session
        ) -> None:
        """Construct the analyzer. 

        Args:
            data_streams (Sequence[DataStream]): A list of data streams to process forward.
            pipe (Pipe): The pipeline to send forward the data samples from the data streams toward.

        """

        # Create a collector that organizes the chronological of data from 
        # the data streams
        self.collector = Collector(data_streams)
        self.pipe = pipe
        self.session = session
        self.all_dtypes = [ds.name for ds in data_streams]

    def run(self, verbose=False) -> None:
        """Run the data pipeline.

        Once the analyzer has been initialized with the collector, 
        processes, and the session, it can run the entire data pipeline.

        Args:
            verbose (bool): If to include logging and loading bar to
            help visualize the wait time until completion.

        """
        # Create data sample slots in the session
        all_samples = {}
        for dtype in self.all_dtypes:
            all_samples[dtype] = None, None # sample, time

        # Attach the session to the pipe
        self.pipe.attach_session(self.session)
        self.pipe.attach_collector(self.collector)

        # First, execute the ``start`` routine of the pipe
        self.pipe.start()

        # Iterate through the collector
        for time, dtype, sample_with_time in tqdm.tqdm(self.collector, disable=not verbose):

            # Store the sample to the session
            all_samples[dtype] = sample_with_time

            # Set the time for the current pipe
            self.pipe.set_time(time)

            # Then process the sample
            self.pipe.step(all_samples, dtype)

            # After the data has propagated the entire pipe, flush the 
            # session logging.
            self.session.flush()

        # Then execute the ``end`` routine of the pipe
        self.pipe.end()

        # Closing the session
        self.session.close()

        # Then close all the streams in the collector
        self.collector.close()
