import asyncio

import pytest

import chimerapy_engine as cpe
from chimerapy_engine.networking.async_loop_thread import AsyncLoopThread

from ..conftest import TEST_DATA_DIR, GenNode, ConsumeNode
from .dev_manager_services_group import DevManagerServicesGroup

logger = cpe._logger.getLogger("chimerapy")
cpe.debug()

# TODO: Fix this entire file


@pytest.mark.asyncio
@pytest.fixture
async def testbed_setup(worker):

    thread = AsyncLoopThread()
    thread.start()

    # Create the service
    manager_services_group = DevManagerServicesGroup(
        logdir=TEST_DATA_DIR, publisher_port=0, thread=thread
    )

    assert await worker.async_connect(
        method="ip",
        host=manager_services_group.http_server.ip,
        port=manager_services_group.http_server.port,
    )

    # Define graph
    gen_node = GenNode(name="Gen1", id="Gen1")
    con_node = ConsumeNode(name="Con1", id="Con1")
    simple_graph = cpe.Graph()
    simple_graph.add_nodes_from([gen_node, con_node])
    simple_graph.add_edge(src=gen_node, dst=con_node)

    # Register graph
    manager_services_group.worker_handler._register_graph(simple_graph)

    yield (manager_services_group.worker_handler, worker)

    await manager_services_group.async_apply("shutdown")


@pytest.mark.asyncio
async def test_worker_handler_create_node(testbed_setup):

    item = await testbed_setup.__anext__()
    worker_handler, worker = item

    assert await worker_handler._request_node_creation(
        worker_id=worker.id, node_id="Gen1"
    )
    assert "Gen1" in worker_handler.state.workers[worker.id].nodes

    assert await worker_handler._request_node_destruction(
        worker_id=worker.id, node_id="Gen1"
    )
    assert "Gen1" not in worker_handler.state.workers[worker.id].nodes


@pytest.mark.asyncio
async def test_worker_handler_create_connections(testbed_setup):

    item = await testbed_setup.__anext__()
    worker_handler, worker = item

    # Create Nodes
    assert await worker_handler._request_node_creation(
        worker_id=worker.id, node_id="Gen1"
    )
    assert await worker_handler._request_node_creation(
        worker_id=worker.id, node_id="Con1"
    )

    # Get the node information
    await worker_handler._request_node_server_data(worker_id=worker.id)
    assert worker_handler.nodes_server_table != {}

    # Create connections
    assert await worker_handler._request_connection_creation(worker_id=worker.id)

    # Teardown
    assert await worker_handler.reset()


@pytest.mark.asyncio
async def test_worker_handler_lifecycle_graph(testbed_setup):

    item = await testbed_setup.__anext__()
    worker_handler, worker = item

    assert await worker_handler.commit(
        graph=worker_handler.graph, mapping={worker.id: ["Gen1", "Con1"]}
    )
    assert await worker_handler.start_workers()

    await asyncio.sleep(5)

    assert await worker_handler.stop()
    assert await worker_handler.collect()

    # Teardown
    assert await worker_handler.reset()
