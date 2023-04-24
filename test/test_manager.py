from .conftest import TEST_DATA_DIR, GenNode, ConsumeNode
from .streams import VideoNode, TabularNode

import time

from aiohttp import web
import pytest

import chimerapy as cp
from chimerapy.manager import Manager


@pytest.fixture(scope="module")
def testbed_setup():

    # Create Manager
    manager = cp.Manager(logdir=TEST_DATA_DIR, port=0)
    worker = cp.Worker(name="local", port=0)
    gen_node = GenNode(name="Gen1")
    con_node = ConsumeNode(name="Con1")

    # Define graph
    simple_graph = cp.Graph()
    simple_graph.add_nodes_from([gen_node, con_node])
    simple_graph.add_edge(src=gen_node, dst=con_node)

    # Connect to the manager
    worker.connect(host=manager.host, port=manager.port)

    yield (manager, worker, simple_graph, gen_node, con_node)

    manager.shutdown()
    worker.shutdown()


def test_manager_logdir_string():
    manager = Manager(logdir=str(TEST_DATA_DIR), port=0)
    assert manager.logdir.parent == TEST_DATA_DIR
    manager.shutdown()


def test_manager_logdir_path():
    manager = Manager(logdir=TEST_DATA_DIR, port=0)
    assert manager.logdir.parent == TEST_DATA_DIR
    manager.shutdown()


def test_manager_creating_node(testbed_setup):

    manager, worker, graph, gen_node, con_node = testbed_setup
    manager._register_graph(graph)

    # Create Node
    future = manager._request_node_creation(worker_id=worker.id, node_id=gen_node.id)
    assert future.result(timeout=30)
    assert gen_node.id in manager.state.workers[worker.id].nodes

    # Destroy Node
    future = manager.reset()
    assert future.result(timeout=30)


def test_manager_create_connections(testbed_setup):

    manager, worker, graph, gen_node, con_node = testbed_setup
    manager._register_graph(graph)

    # Create Nodes
    gen_future = manager._request_node_creation(
        worker_id=worker.id, node_id=gen_node.id
    )
    con_future = manager._request_node_creation(
        worker_id=worker.id, node_id=con_node.id
    )
    assert gen_future.result(timeout=30) and con_future.result(timeout=30)

    # Get the node information
    get_future = manager._request_node_server_data(worker_id=worker.id)
    assert get_future.result(timeout=10)
    assert manager.nodes_server_table != {}

    # Create connections
    connection_future = manager._request_connection_creation(worker_id=worker.id)
    assert connection_future.result(timeout=30)

    # Teardown
    future = manager.reset()
    assert future.result(timeout=30)


def test_manager_lifecycle_graph(testbed_setup):

    manager, worker, graph, gen_node, con_node = testbed_setup

    future = manager.commit_graph(
        graph=graph, mapping={worker.id: [gen_node.id, con_node.id]}
    )
    assert future.result(timeout=30)
    assert manager.start().result()

    time.sleep(5)

    assert manager.stop().result()
    assert manager.collect().result()

    future = manager.reset(keep_workers=True)
    assert future.result(timeout=30)


class RemoteTransferWorker(cp.Worker):
    async def _async_send_archive(self, request: web.Request):
        await request.json()

        # Collect data from the Nodes
        success = await self.async_collect()

        # If located in the same computer, just move the data
        if success:
            await self._async_send_archive_remotely(
                self.manager_host, self.manager_port
            )

        # After completion, let the Manager know
        self.logger.debug(f"{self}: Responded to Manager collect request!")
        return web.json_response({"id": self.id, "success": success})


@pytest.mark.parametrize(
    "node_cls",
    [VideoNode, TabularNode],
)
def test_manager_remote_transfer(node_cls):

    manager = cp.Manager(logdir=TEST_DATA_DIR, port=0)
    remote_worker = RemoteTransferWorker(name="remote", id="remote")
    remote_worker.connect(manager.host, manager.port)

    node = node_cls(name="node")

    # Define graph
    graph = cp.Graph()
    graph.add_nodes_from([node])
    manager._register_graph(graph)

    future = manager.commit_graph(graph=graph, mapping={remote_worker.id: [node.id]})
    assert future.result(timeout=30)
    assert manager.start().result()

    time.sleep(0.5)

    assert manager.stop().result()
    assert manager.collect().result()

    future = manager.reset(keep_workers=True)
    assert future.result(timeout=30)

    manager.shutdown()

    # The files from the remote worker should exists!
    assert (manager.logdir / "remote" / "node").exists()
