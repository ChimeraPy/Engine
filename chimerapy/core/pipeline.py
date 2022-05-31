# Package Management
__package__ = 'chimerapy'

# Built-in imports
from typing import Dict, Optional, Union, List
import copy
import pathlib

# Third-party imports
import pandas as pd
import networkx as nx

# Local Imports
from chimerapy.core.data_stream import DataStream
from chimerapy.core.session import Session
from chimerapy.core.process import Process
from chimerapy.core.tools import threaded, clear_queue, PortableQueue,  get_threads_cpu_percent

# define a new metaclass which overrides the "__call__" function
class NewInitCaller(type):
    # Reference: https://stackoverflow.com/questions/16017397/injecting-function-call-after-init-with-decorator

    def __call__(cls, *args, **kwargs):
        """Called when you call MyNewClass() """
        obj = type.__call__(cls, *args, **kwargs)
        obj._post_init_call()
        return obj

class Pipeline(metaclass=NewInitCaller):

    # All pipelines have the following attribute
    session = None
    graph = nx.DiGraph()

    def __init__(
            self, 
            name:str, 
            inputs:List[Union[DataStream, 'Pipeline']],
        ) -> None:
        """Construct a ``Pipeline``.

        Within this construct, define the chain between your Process and 
        keep them stored as variables directly connected to your class. 
        That way we can identify the pipeline structure and create a 
        graph representation.

        """
        # Verify inputs
        assert isinstance(inputs, list), "``inputs`` must be a list."

        # Store input parameter
        self.name = name
        self.inputs = inputs
        self.pipelines = []

        # Create the directed graph and add the data streams as nodes
        for ins in self.inputs:
            # If data stream, this becomes the source
            if isinstance(ins, DataStream):
                self.graph.add_node(ins.name, item=ins)

            # If it is another pipeline, we have to add it to the current
            # pipeline's graph and know all of its the previous content
            elif type(ins) == 'Pipeline':

                # Add the pipeline to this pipeline, with an suffix
                self.safe_merge(ins)

                # Store the pipeline to avoid in the future adding it again
                self.pipelines.append(ins)

        # Then create the pipeline runner
        # self.runner = Runner()

        # Create the session to log data to
        # self.session = Session(
        #     name=name,
        #     logging_queue=self.logging_queue
        # )

    def _post_init_call(self) -> None:

        # Check for all the set attributes in the Pipeline
        for attr_name, attr in self.__dict__.items():

            # If it is a process, add it to the graph and its edge
            if type(attr) == Process:

                # Add the process as a node
                self.graph.add_node(attr.name, item=attr)

                # Add the edges from the process
                for ins in attr.inputs:
                    self.graph.add_edge(ins.name, attr.name)

            # Else if it is a pipeline, add its subgraph
            if type(attr) == 'Pipeline':

                # Check if this is a previously added pipeline
                if attr not in self.pipelines:
                    self.safe_merge(attr)

        return None

    def __str__(self) -> str:
        """Get string representation of ``Pipeline``.

        Returns:
            str: string representation.

        """
        return self.__repr__()

    def __eq__(self, other:'Pipeline') -> bool:
        return nx.is_isomorphic(self.graph, other.graph)

    def safe_merge(self, other:'Pipeline'):

        # Get the previous graph's nodes
        nodes = list(other.graph.nodes(data=False))

        # Create a rename mapping
        new_node_names = [f"{other.name}_{n}" for n in nodes]
        rename_mapping = {k:v for k,v in zip(new_node_names, nodes)}

        # Store the previous graph into the new graph
        self.graph = nx.compose(nx.relabel_nodes(other.graph, rename_mapping), self.graph)

    def set_session(self, session: Session) -> None:
        """Set the session to the ``Pipeline`` instance.

        Args:
            session (Session): The session that acts as the interface \
                between the ``Pipeline`` and the ``Logger``.

        """
        # First make the session an attribute
        self.session = session

    def run(
        self,
        time_window:pd.Timedelta=pd.Timedelta(seconds=5),
        logdir:Optional[Union[str, pathlib.Path]]=None,
        memory_limit:float=0.8,
        verbose=True
        ):
        ...

