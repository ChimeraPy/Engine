# Package Management
__package__ = 'mm'

# Built-in Imports
from typing import List, Optional, Union
import collections

# Third Party Imports
import networkx as nx

# Internal Imports
from .data_sample import DataSample
from .process import Process
from .collector import Collector
from .process import Process
from .visualization import Visualization

class Analyzer:

    def __init__(
            self, 
            collector: Collector,
            processes: List[Process], 
            visualizations: Optional[List[Visualization]]=None
        ):
        self.processes = {process.name: process for process in processes}
        
        if type(visualizations) != type(None):
            self.visualizations = {visual.name: visual for visual in visualizations}

        """
        Confirm that processes and visualizations inputs are possible.
        """

        # Construct dependency graph
        self.data_flow_graph = nx.DiGraph()

        # Add the collector and processes in the process graph
        self.data_flow_graph.add_node(list(self.processes.keys()))
 
        for process in self.processes.values():

            # Connect dependencies to the process
            for x in process.inputs:
                self.data_flow_graph.add_edge(x, process.name)

    def update_session(self, in_sample: DataSample) -> None:
        ...

    def get_sample_processes(self, in_sample: DataSample) -> List[Process]:
        ...

    def get_sample_visuals(self, in_sample: DataSample) -> List[Visualization]:
        ...

    def get_all_inputs(self, element: Union[Process, Visualization]) -> dict:
        ...

