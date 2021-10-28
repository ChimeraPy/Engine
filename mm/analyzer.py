# Package Management
__package__ = 'mm'

# Built-in Imports
from typing import List, Optional, Union, Tuple
import collections

# Third Party Imports
import networkx as nx
from networkx.drawing.nx_agraph import graphviz_layout

# Internal Imports
from .data_sample import DataSample
from .process import Process
from .collector import Collector
from .process import Process

class Analyzer:

    def __init__(
            self, 
            collector: Collector,
            processes: List[Process], 
            # visualizations: Optional[List[Visualization]]=None
        ):
        """
        Confirm that processes and visualizations inputs are possible.
        """

        # Construct dependency graph
        self.data_flow_graph = nx.DiGraph()

        # Add the collector and processes in the process graph
        self.data_flow_graph.add_nodes_from(processes)
        self.data_flow_graph.add_nodes_from(list(collector.data_streams.keys())) # names of the streams

        # Constructing process by name look up table
        self.secondary_stream_table = {x.name: x for x in processes}

        # Accounting for each process
        for process in processes:

            # Connect dependencies to the process
            for dependency in process.inputs:

                # If the dependency is the output of another process
                if dependency in self.secondary_stream_table.keys():
                    self.data_flow_graph.add_edge(self.secondary_stream_table[dependency], process)
                else:
                    # else, the dependecy is a collector's stream
                    self.data_flow_graph.add_edge(dependency, process)

        # Then creating a look-up table of the process dependent on
        # which samples
        self.pipeline_lookup = {}
        for data_stream_type in collector.data_streams.keys():
            self.pipeline_lookup[data_stream_type] = self.data_flow_graph.successors(data_stream_type)

    def get_sample_pipeline(self, sample: DataSample) -> List[Process]:
        return self.pipeline_lookup[sample.dtype]
