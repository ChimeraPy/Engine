# Package Management
__package__ = 'mm'

# Built-in Imports
from typing import List, Optional

# Third Party Imports
import networkx as nx

# Internal Imports
from .data_sample import DataSample
from .process import Process
from .collector import Collector
from .process import Process
from .visualization import Visualization

class Analyzer:

    def __init__(self, collector: Collector, processes: List[Process], visualizations:Optional[List[Visualization]]=None):
        self.collector = collector
        self.processes = {process.name: process for process in processes}
        
        if type(visualizations) != type(None):
            self.visualizations = {visual.name: visual for visual in visualizations}

        """
        Confirm that processes and visualizations inputs are possible.
        """

        # Construct dependency graph
        self.data_flow_graph = nx.DiGraph()

        # Add the collector and processes in the process graph
        self.data_flow_graph.add_node(list(self.collector.data_streams.keys()))
        self.data_flow_graph.add_node(list(self.processes.keys()))
 
        # Now after all the components have been added to the graph,
        # add the edges and their labels. The labels specific which 
        # data stream to pull from the src to the destination of the 
        # edge.
        
        for process in self.processes.values():

            # Connect dependencies to the process
            for x in process.inputs:
                self.data_flow_graph.add_edge(x, process.name)

        # Calculating the depth of each node
        # nx.all_pairs_shortest_path_length(G)

    # def forward(self, x: DataSample):

    #     # Determining where the data sample needs to go
    #     successors = self.data_flow_graph[x.data_stream_name]

    #     # Feed into those processes and storing their results for each
    #     for successor for successors:
    #         successor.__call__(x)

    #     # Then 

    #     return None        
