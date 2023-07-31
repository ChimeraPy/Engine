import asyncio
import pathlib
import tempfile

import pytest

import chimerapy.engine as cpe
from chimerapy.engine.manager.worker_handler_service import WorkerHandlerService
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread
from chimerapy.engine.eventbus import EventBus, make_evented
from chimerapy.engine.states import ManagerState

from ..conftest import GenNode, ConsumeNode

logger = cpe._logger.getLogger("chimerapy-engine")
cpe.debug()


@pytest.fixture
def testbed_setup(worker):

    thread = AsyncLoopThread()
    thread.start()
    eventbus = EventBus(thread=thread)

    state = make_evented(
        ManagerState(logdir=pathlib.Path(tempfile.mkdtemp())), event_bus=eventbus
    )

    # Define graph
    gen_node = GenNode(name="Gen1", id="Gen1")
    con_node = ConsumeNode(name="Con1", id="Con1")
    simple_graph = cpe.Graph()
    simple_graph.add_nodes_from([gen_node, con_node])
    simple_graph.add_edge(src=gen_node, dst=con_node)

    worker_handler = WorkerHandlerService(
        name="worker_handler", eventbus=eventbus, state=state
    )

    # Register graph
    worker_handler._register_graph(simple_graph)

    # Register worker
    thread.exec(worker_handler._register_worker(worker.state)).result(timeout=10)

    return (worker_handler, worker)


def test_instanticate(testbed_setup):
    ...


@pytest.mark.asyncio
async def test_worker_handler_create_node(testbed_setup):

    worker_handler, worker = testbed_setup

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

    worker_handler, worker = testbed_setup

    # Create Nodes
    assert await worker_handler._request_node_creation(
        worker_id=worker.id, node_id="Gen1"
    )
    assert await worker_handler._request_node_creation(
        worker_id=worker.id, node_id="Con1"
    )

    # Get the node information
    await worker_handler._request_node_pub_table(worker_id=worker.id)
    assert worker_handler.node_pub_table.table != {}

    # Create connections
    assert await worker_handler._request_connection_creation(worker_id=worker.id)

    # Teardown
    assert await worker_handler.reset()


@pytest.mark.asyncio
async def test_worker_handler_lifecycle_graph(testbed_setup):

    worker_handler, worker = testbed_setup

    assert await worker_handler.commit(
        graph=worker_handler.graph, mapping={worker.id: ["Gen1", "Con1"]}
    )
    assert await worker_handler.start_workers()

    await asyncio.sleep(2)

    assert await worker_handler.stop()
    assert await worker_handler.collect()

    # Teardown
    assert await worker_handler.reset()
