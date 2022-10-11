from typing import Sequence
import copy

from .node import Node

import networkx as nx


class Graph:
    def __init__(self, g: nx.DiGraph = nx.DiGraph()):
        self.G = copy.deepcopy(g)

    def has_node_by_name(self, node_name: str):
        return self.G.has_node(node_name)

    def add_node(self, node: Node):
        self.G.add_node(node.name, object=node)

    def add_nodes_from(self, nodes: Sequence[Node]):
        self.G.add_nodes_from([(n.name, {"object": n}) for n in nodes])

    def add_edge(self, src: Node, dst: Node):
        self.G.add_edge(src.name, dst.name)

    def add_edges_from(self, src: Sequence[Node], dst: Sequence[Node]):
        self.G.add_edges_from([[n.name for n in src], [n.name for n in dst]])

    def is_valid(self):
        return nx.is_directed_acyclic_graph(self.G)

    def to_manager(self, src: Node):
        ...
