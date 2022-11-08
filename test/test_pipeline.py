import logging

import pytest
from pytest_lazyfixture import lazy_fixture
import numpy as np

import chimerapy as cp

logger = logging.getLogger("chimerapy")

# How to test with matplotlib
# https://stackoverflow.com/questions/63541241/networkx-drawing-in-layered-manner


@pytest.fixture
def simple_graph():

    a = cp.Node(name="a")
    b = cp.Node(name="b")

    graph = cp.Graph()
    graph.add_nodes_from([a, b])
    graph.add_edge(a, b)

    return graph


@pytest.fixture
def slightly_more_complex_graph():

    a = cp.Node(name="a")
    b = cp.Node(name="b")
    c = cp.Node(name="c")
    d = cp.Node(name="d")

    graph = cp.Graph()
    graph.add_nodes_from([a, b, c, d])
    graph.add_edges_from([[a, b], [a, c], [b, c], [c, d]])
    return graph


@pytest.fixture
def complex_graph():

    a = cp.Node(name="a")
    b = cp.Node(name="b")
    c = cp.Node(name="c")
    d = cp.Node(name="d")
    e = cp.Node(name="e")
    f = cp.Node(name="f")

    graph = cp.Graph()
    graph.add_nodes_from([a, b, c, d, e, f])
    graph.add_edges_from([[a, b], [c, d], [c, e], [b, e], [d, f], [e, f]])
    return graph


@pytest.mark.parametrize(
    "graph, expected_layers, expected_pos",
    [
        (lazy_fixture("simple_graph"), [["a"], ["b"]], {"a": [0, 0.5], "b": [1, 0.5]}),
        (
            lazy_fixture("slightly_more_complex_graph"),
            [["a"], ["b"], ["c"], ["d"]],
            {
                "a": [0, 0.5],
                "b": [0.3333333, 0.5],
                "c": [0.666666, 0.5],
                "d": [0.9999999, 0.5],
            },
        ),
        (
            lazy_fixture("complex_graph"),
            [["a", "c"], ["b", "d"], ["e"], ["f"]],
            {
                "a": [0, 0],
                "c": [0, 1],
                "b": [0.333333, 0],
                "d": [0.333333, 1],
                "e": [0.666666, 0.5],
                "f": [0.999999, 0.5],
            },
        ),
    ],
)
def test_graph_pos_simple(graph, expected_layers, expected_pos):
    layers, pos = graph.get_layers_and_pos()
    assert layers == expected_layers

    for node_name in pos:
        assert (
            np.isclose(expected_pos[node_name], pos[node_name])
        ).all(), f"Node {node_name} is incorrect"

@pytest.mark.skip(reason="need to automate matplotlib test")
@pytest.mark.parametrize(
    "graph",
    [
        (lazy_fixture("simple_graph")),
        (lazy_fixture("slightly_more_complex_graph")),
        (lazy_fixture("complex_graph")),
    ],
)
def test_graph_simple_visualization(graph):

    # Visualize the graph
    graph.plot()
