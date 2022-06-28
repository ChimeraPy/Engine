
from multiprocessing.managers import BaseManager
import networkx as nx


from chimerapy.core.process import Process
from chimerapy.core.graph.edge import ProcessEdge
from chimerapy.utils.memory_manager import MPManager
from chimerapy.utils.tools import Proxy
import logging

# Logging
logger = logging.getLogger(__name__)


# Perform global information
MPManager.register('ProcessEdge', ProcessEdge)

class Graph:
    def __init__(
        self,
        name:str,
        maxsize:int=10,
    ):
        self.name = name
        self.maxsize = maxsize
        self.graph = nx.DiGraph()
        self.manager = MPManager()
        self.manager.start()

        self.memory_manager = self.manager.MemoryManager(memory_limit=maxsize)

    def add_node(self, node: Process):
        self.graph.add_node(node)

    def remove_node(self, node: Process):
        self.graph.remove_node(node)

    def add_edge(
        self,
        from_node: Process,
        to_node: Process,
        edge_name: str = "",
        queue_limit: int = 0
    ):
        # add graph edge here
        self.graph.add_edge(from_node, to_node)

        # add queue to processes here
        maxsize = queue_limit or self.maxsize
        que = self.manager.ProcessEdge(
            maxsize,
            memory_manager=self.memory_manager,
            name=edge_name
        )
        from_node.out_queues.append(que)
        to_node.in_queues.append(que)

    def start(self) -> None:
        for node in self.graph.nodes:
            node.start()
        

    def shutdown(self) -> None:
        for node in self.graph.nodes:
            node.shutdown()
