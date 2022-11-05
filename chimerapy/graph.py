from typing import Sequence, Tuple
import copy
import logging
import pdb

from .node import Node

import numpy as np
import networkx as nx
import matplotlib.pyplot as plt

logger = logging.getLogger("chimerapy")


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

    def add_edges_from(self, list_of_edges: Sequence[Sequence[Node]]):
        # Reconstruct the list as node names
        list_of_edges_with_names = []
        for edge in list_of_edges:
            list_of_edges_with_names.append([edge[0].name, edge[1].name])

        self.G.add_edges_from(list_of_edges_with_names)

    def is_valid(self):
        """Checks if ``Graph`` is a true DAG."""
        return nx.is_directed_acyclic_graph(self.G)

    def get_layers_and_pos(self):

        # First obtain the layers
        # https://networkx.org/nx-guides/content/algorithms/dag/index.html
        layers = list(nx.topological_generations(self.G))

        pos = {}
        for i, layer in enumerate(layers):

            if len(layer) >= 2:
                layer_points = np.linspace(0, 1, len(layer))
            else:
                layer_points = [0.5]

            for j, node_name in enumerate(layer):
                x = i / (len(layers) - 1)
                y = layer_points[j]
                pos[node_name] = np.array([x, y])

        return layers, pos

    def plot(self, font_size: int = 30, node_size: int = 5000):
        """Plotting the ``Graph`` to visualize data pipeline.

        This visualization tool uses ``matplotlib`` and ``networkx`` to
        show the ``Nodes`` and their edges.

        Args:
            font_size (int): Font size
            node_size (int): Node size
        """

        # Then get the position of the nodes
        layers, pos = self.get_layers_and_pos()

        # Draw the networkx
        fig = plt.figure(figsize=(20, 10))
        nx.draw_networkx(
            self.G,
            pos,
            node_color="red",
            with_labels=True,
            font_size=font_size,
            node_size=node_size,
            arrowsize=50,
        )
        plt.show()

        # return fig
