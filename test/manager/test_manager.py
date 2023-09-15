import time
import os
import pathlib

import pytest

import chimerapy.engine as cpe
from ..conftest import GenNode, ConsumeNode, TEST_DATA_DIR

logger = cpe._logger.getLogger("chimerapy-engine")

# Constant
TEST_DIR = pathlib.Path(os.path.abspath(__file__)).parent.parent
TEST_PACKAGE_DIR = TEST_DIR / "MOCK"


@pytest.fixture
def local_node_graph(gen_node):
    graph = cpe.Graph()
    graph.add_node(gen_node)
    return graph


@pytest.fixture
def packaged_node_graph():
    # Installing test package (in case this test package hasn't been done before)
    # Reference: https://stackoverflow.com/a/55188705/13231446
    try:
        import test_package as tp
    except ImportError:
        import pip._internal as pip

        pip.main(["install", str(TEST_PACKAGE_DIR)])
    finally:
        import test_package as tp

    node = tp.TestNode(name="test")
    graph = cpe.Graph()
    graph.add_node(node)
    return graph


@pytest.fixture(scope="module")
def manager_with_worker():
    # Create the actors
    manager = cpe.Manager(logdir=TEST_DATA_DIR, port=0)
    worker = cpe.Worker(name="local", port=0)

    worker.connect(host=manager.host, port=manager.port)

    return (manager, worker)


class TestLifeCycle:
    @pytest.mark.skip(reason="Frozen test")
    def test_sending_package(self, manager, _worker, config_graph):
        _worker.connect(host=manager.host, port=manager.port)

        assert manager.commit_graph(
            graph=config_graph,
            mapping={_worker.id: list(config_graph.G.nodes())},
            send_packages=[
                {"name": "test_package", "path": TEST_PACKAGE_DIR / "test_package"}
            ],
        ).result(timeout=30)

        for node_id in config_graph.G.nodes():
            assert manager.workers[_worker.id].nodes[node_id].fsm != "NULL"

    @pytest.mark.parametrize("context", ["multiprocessing", "threading"])
    # @pytest.mark.parametrize("context", ["multiprocessing"])
    def test_manager_lifecycle(self, manager_with_worker, context):
        manager, worker = manager_with_worker

        # Define graph
        gen_node = GenNode(name="Gen1")
        con_node = ConsumeNode(name="Con1")
        graph = cpe.Graph()
        graph.add_nodes_from([gen_node, con_node])
        graph.add_edge(src=gen_node, dst=con_node)

        # Configure the worker and obtain the mapping
        mapping = {worker.id: [gen_node.id, con_node.id]}
        manager.commit_graph(graph, mapping, context=context).result(timeout=30)

        assert manager.start().result()
        assert manager.record().result()

        time.sleep(5)

        assert manager.stop().result()
        assert manager.collect().result()

        manager.reset()

    def test_manager_reset(self, manager_with_worker):
        manager, worker = manager_with_worker

        # Define graph
        gen_node = GenNode(name="Gen1")
        con_node = ConsumeNode(name="Con1")
        simple_graph = cpe.Graph()
        simple_graph.add_nodes_from([gen_node, con_node])
        simple_graph.add_edge(src=gen_node, dst=con_node)

        # Configure the worker and obtain the mapping
        mapping = {worker.id: [gen_node.id, con_node.id]}

        manager.reset()

        manager.commit_graph(graph=simple_graph, mapping=mapping).result(timeout=30)
        assert manager.start().result()
        assert manager.record().result()

        time.sleep(3)

        assert manager.stop().result()
        assert manager.collect().result()

        manager.reset()

    # @pytest.mark.skip(reason="Flaky")
    def test_manager_recommit_graph(self, manager_with_worker):
        manager, worker = manager_with_worker

        # Define graph
        gen_node = GenNode(name="Gen1")
        con_node = ConsumeNode(name="Con1")
        simple_graph = cpe.Graph()
        simple_graph.add_nodes_from([gen_node, con_node])
        simple_graph.add_edge(src=gen_node, dst=con_node)

        # Configure the worker and obtain the mapping
        mapping = {worker.id: [gen_node.id, con_node.id]}

        graph_info = {"graph": simple_graph, "mapping": mapping}

        logger.debug("STARTING COMMIT 1st ROUND")
        tic = time.time()
        assert manager.commit_graph(**graph_info).result(timeout=30)
        toc = time.time()
        delta = toc - tic
        logger.debug("FINISHED COMMIT 1st ROUND")

        logger.debug("STARTING RESET")
        assert manager.reset()
        logger.debug("FINISHED RESET")

        logger.debug("STARTING COMMIT 2st ROUND")
        tic2 = time.time()
        assert manager.commit_graph(**graph_info).result(timeout=30)
        toc2 = time.time()
        delta2 = toc2 - tic2
        logger.debug("FINISHED COMMIT 2st ROUND")

        assert ((delta2 - delta) / (delta)) < 1

        manager.reset()
