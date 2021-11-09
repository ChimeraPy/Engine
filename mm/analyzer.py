"""Module focused on the ``Analyzer`` implementation.

Contains the following classes:
    ``Analyzer``

"""

# Package Management
__package__ = 'mm'

# Built-in Imports
from typing import Sequence, Optional, Union, Tuple, Iterator, Iterable
import collections

# Third Party Imports
import networkx as nx
from networkx.drawing.nx_agraph import graphviz_layout

# Internal Imports
from .data_sample import DataSample
from .process import Process
from .collector import Collector, OfflineCollector
from .process import Process
from .session import Session

class Analyzer:
    """Multimodal Data Processing and Analysis Coordinator. 

    The Analyzer is the coordinator between the other modules. First,
    the analyzer determines the flow of data from the data streams
    stored in the collector and the given processes. Once the analyzer
    constructs the data flow graph, the pipelines for the input 
    data stream are determined and stored.

    Then, when feeding new data samples, the analyzer send them to
    the right pipeline and its processes. The original data sample and
    its subsequent generated data sample in the pipeline are stored
    in the Session as a means to keep track of the latest sample.

    Attributes:
        collector (mm.Collector): The collector used to match the 
        timetracks of each individual data stream.
        
        processes (List[mm.Process]): A list of processes to be executed
        depending on their inputs and triggers.
        
        session (mm.Session): The session that stores all of the latest
        data samples from original and generated data streams.
        
        data_flow_graph (nx.DiGraph): The data flow constructed from
        the data streams and the processes.
        
        pipeline_lookup (dict): A dictionary that stores the pipelines
        for each type of input data stream.

    Todo:
        * Make the session have the option to save intermediate 
        data sample during the analysis, if the user request this feature.
    """

    def __init__(
            self, 
            collector: Union[Collector, OfflineCollector],
            processes: Sequence[Process],
            session: Session
        ) -> None:
        """Construct the analyzer. 

        Args:
            collector (mm.Collector): The collector used to match the 
            timetracks of each individual data stream.
            
            processes (List[mm.Process]): A list of processes to be executed
            depending on their inputs and triggers.
            
            session (mm.Session): The session that stores all of the latest
            data samples from original and generated data streams.

        """
        # Store inputs parameters
        self.collector = collector
        self.processes = processes
        self.session = session

        # Construct dependency graph
        self.data_flow_graph = nx.DiGraph()

        # Add the collector and processes in the process graph
        self.data_flow_graph.add_nodes_from(processes)
        self.data_flow_graph.add_nodes_from(list(collector.data_streams.keys())) # names of the streams

        # Table from generated ds -> processes that generates it
        self.ds_generated_from_process = {x.output: x for x in processes if x.output not in x.inputs}

        # Table of trigger for processes (not all processes use this)
        self.trigger_lookup = collections.defaultdict(list)

        # Accounting for each process
        for process in processes:

            # If the process has a trigger, use that instead of the input
            if process.trigger:

                # Add that to a lookup table 
                self.trigger_lookup[process.trigger].append(process)

            # Else, the process gets trigger when new input is available
            else:

                # Connect dependencies to the process
                for input in process.inputs:

                    # If the dependency is the output of another process
                    if input in self.ds_generated_from_process.keys():
                        self.data_flow_graph.add_edge(self.ds_generated_from_process[input], process)
                    else:
                        # else, the dependecy is a collector's stream
                        self.data_flow_graph.add_edge(input, process)

        # Then creating a look-up table of the process dependent on
        # which samples
        self.pipeline_lookup = {}
        for data_stream_type in collector.data_streams.keys():
            # Determine the processes that are dependent on the data_type
            all_dependencies: Iterable[Union[Process, str]] = sum(nx.dfs_successors(self.data_flow_graph, data_stream_type).values(), [])
            # The pipeline only consist of subsequent processes
            self.pipeline_lookup[data_stream_type] = [x for x in all_dependencies if isinstance(x, Process)]

    def get_sample_pipeline(self, sample: DataSample) -> Sequence[Process]:
        """Get the processes that are dependent to this type of data sample.

        Args:
            sample (mm.DataSample): The data sample that contains the 
            data type used to select the data pipeline.
        
        """
        return self.pipeline_lookup[sample.dtype] + self.trigger_lookup[sample.dtype]

    def step(self, sample: DataSample) -> None:
        """Routine executed to process a data sample.

        Given a ``DataSample`` from one of the ``DataStream``s in the 
        ``Collector``, the ``Analyzer`` propagates the data sample
        through its respective data pipeline and saves intermediate 
        and final data samples into its ``Session`` attribute.

        Args:
            sample (mm.DataSample): The new input data sample to will be
            propagated though its corresponding pipeline and stored.
        
        """
        # Add the sample to the analyzer's session data
        self.session.update(sample)

        # Obtain the processes that are dependent on the sample
        s_p = self.get_sample_pipeline(sample)

        # Forward the sample throughout all the processes
        for process in s_p:

            # Obtain the needed inputs for the process 
            output = self.session.apply(process)

        return None

    def close(self) -> None:
        """Close routine that executes all other's closing routines.

        The ``Analyzer``'s closing routine closes all the ``Process``,
        ``Collector``, and ``DataStream`` by executing ``.close()`` 
        methods in each respective component.

        """
        # Close all the processes
        for process in self.processes:
            process.close()

        # Close all the data streams in the collector
        for data_stream in self.collector.data_streams.values():
            data_stream.close()

        # Closing the session
        self.session.close()
