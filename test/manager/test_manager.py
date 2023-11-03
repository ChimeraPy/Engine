import asyncio
import os
import pathlib
import time

import pytest

import chimerapy.engine as cpe

from ..conftest import TEST_DATA_DIR, ConsumeNode, GenNode

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


@pytest.fixture
async def manager_with_worker():
    manager = cpe.Manager(logdir=TEST_DATA_DIR, port=0)
    worker = cpe.Worker(name="local", port=0)
    await manager.aserve()
    await worker.aserve()
    await worker.async_connect(host=manager.host, port=manager.port)
    yield manager, worker
    await worker.async_shutdown()
    await manager.async_shutdown()


class TestLifeCycle:

    # @pytest.mark.skip(reason="Frozen test")
    # def test_sending_package(self, manager, _worker, config_graph):
    #     _worker.connect(host=manager.host, port=manager.port)

    #     assert manager.commit_graph(
    #         graph=config_graph,
    #         mapping={_worker.id: list(config_graph.G.nodes())},
    #         send_packages=[
    #             {"name": "test_package", "path": TEST_PACKAGE_DIR / "test_package"}
    #         ],
    #     ).result(timeout=30)

    #     for node_id in config_graph.G.nodes():
    #         assert manager.workers[_worker.id].nodes[node_id].fsm != "NULL"

    # @pytest.mark.parametrize("context", ["multiprocessing", "threading"])
    @pytest.mark.parametrize("context", ["multiprocessing"])
    async def test_manager_lifecycle(self, manager_with_worker, context):
        manager, worker = manager_with_worker

        # Define graph
        gen_node = GenNode(name="Gen1")
        con_node = ConsumeNode(name="Con1")
        graph = cpe.Graph()
        graph.add_nodes_from([gen_node, con_node])
        graph.add_edge(src=gen_node, dst=con_node)

        # Configure the worker and obtain the mapping
        mapping = {worker.id: [gen_node.id, con_node.id]}
        await manager.async_commit(graph, mapping, context=context)

        assert await manager.async_start()
        assert await manager.async_record()

        await asyncio.sleep(3)

        assert await manager.async_stop()
        assert await manager.async_collect()

        await manager.async_reset()

    async def test_manager_reset(self, manager_with_worker):
        manager, worker = manager_with_worker

        # Define graph
        gen_node = GenNode(name="Gen1")
        con_node = ConsumeNode(name="Con1")
        simple_graph = cpe.Graph()
        simple_graph.add_nodes_from([gen_node, con_node])
        simple_graph.add_edge(src=gen_node, dst=con_node)

        # Configure the worker and obtain the mapping
        mapping = {worker.id: [gen_node.id, con_node.id]}

        await manager.async_reset()

        await manager.async_commit(graph=simple_graph, mapping=mapping)
        assert await manager.async_start()
        assert await manager.async_record()

        await asyncio.sleep(3)

        assert await manager.async_stop()
        assert await manager.async_collect()

        await manager.async_reset()

    # @pytest.mark.skip(reason="Flaky")
    async def test_manager_recommit_graph(self, manager_with_worker):
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
        assert await manager.async_commit(**graph_info)
        toc = time.time()
        delta = toc - tic
        logger.debug("FINISHED COMMIT 1st ROUND")

        logger.debug("STARTING RESET")
        assert await manager.async_reset()
        logger.debug("FINISHED RESET")

        logger.debug("STARTING COMMIT 2st ROUND")
        tic2 = time.time()
        assert await manager.async_commit(**graph_info)
        toc2 = time.time()
        delta2 = toc2 - tic2
        logger.debug("FINISHED COMMIT 2st ROUND")

        assert ((delta2 - delta) / (delta)) < 1

        await manager.async_reset()
