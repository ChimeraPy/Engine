# Package Management
__package__ = 'mm'

# Built-in Imports
from typing import List, Optional, Union, Tuple, Iterator
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
            processes: List[Process]
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
        self.process_lookup_table = {x.output: x for x in processes if x.output not in x.inputs}

        # Accounting for each process
        for process in processes:

            # Connect dependencies to the process
            for input in process.inputs:

                # If the dependency is the output of another process
                if input in self.process_lookup_table.keys():
                    self.data_flow_graph.add_edge(self.process_lookup_table[input], process)
                else:
                    # else, the dependecy is a collector's stream
                    self.data_flow_graph.add_edge(input, process)

        # Then creating a look-up table of the process dependent on
        # which samples
        self.pipeline_lookup = {}
        for data_stream_type in collector.data_streams.keys():
            all_dependencies = sum(nx.dfs_successors(self.data_flow_graph, data_stream_type).values(), [])
            self.pipeline_lookup[data_stream_type] = [x for x in all_dependencies if isinstance(x, Process)]

    def get_sample_pipeline(self, sample: DataSample) -> List[Process]:
        return self.pipeline_lookup[sample.dtype]
