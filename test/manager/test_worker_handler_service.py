import asyncio
import pathlib
import tempfile

import pytest

import chimerapy.engine as cpe
from chimerapy.engine import config
from chimerapy.engine.manager.worker_handler_service import WorkerHandlerService
from chimerapy.engine.manager.http_server_service import HttpServerService
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread
from chimerapy.engine.eventbus import EventBus, make_evented, Event
from chimerapy.engine.states import ManagerState

from ..conftest import GenNode, ConsumeNode

logger = cpe._logger.getLogger("chimerapy-engine")
cpe.debug()


@pytest.fixture(scope="module")
def testbed_setup():

    # Creating worker to communicate
    worker = cpe.Worker(name="local", id="local", port=0)

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

    # Create services
    http_server = HttpServerService(
        name="http_server",
        port=0,
        enable_api=True,
        thread=thread,
        eventbus=eventbus,
        state=state,
    )
    worker_handler = WorkerHandlerService(
        name="worker_handler", eventbus=eventbus, state=state
    )

    eventbus.send(Event("start")).result()

    # Register worker
    worker.connect(host=http_server.ip, port=http_server.port)

    yield (worker_handler, worker, simple_graph)

    eventbus.send(Event("shutdown")).result()
    worker.shutdown()


def test_instanticate(testbed_setup):
    ...


async def test_worker_handler_create_node(testbed_setup):
    worker_handler, worker, simple_graph = testbed_setup

    # Register graph
    worker_handler._register_graph(simple_graph)

    assert await worker_handler._request_node_creation(
        worker_id=worker.id, node_id="Gen1"
    )
    assert "Gen1" in worker.state.nodes
    assert "Gen1" in worker_handler.state.workers[worker.id].nodes

    assert await worker_handler._request_node_destruction(
        worker_id=worker.id, node_id="Gen1"
    )
    await asyncio.sleep(1)

    assert "Gen1" not in worker.state.nodes
    assert "Gen1" not in worker_handler.state.workers[worker.id].nodes


async def test_worker_handler_create_connections(testbed_setup):
    worker_handler, worker, simple_graph = testbed_setup

    # Register graph
    worker_handler._register_graph(simple_graph)

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


async def test_worker_handler_lifecycle_graph(testbed_setup):
    worker_handler, worker, simple_graph = testbed_setup

    # Register graph
    worker_handler._register_graph(simple_graph)

    assert await worker_handler.commit(
        graph=worker_handler.graph, mapping={worker.id: ["Gen1", "Con1"]}
    )
    assert await worker_handler.start_workers()

    await asyncio.sleep(2)

    assert await worker_handler.stop()
    assert await worker_handler.collect()

    # Teardown
    assert await worker_handler.reset()


async def test_worker_handler_enable_diagnostics(testbed_setup):
    worker_handler, worker, simple_graph = testbed_setup

    config.set("diagnostics.interval", 2)
    config.set("diagnostics.logging-enabled", True)

    # Register graph
    worker_handler._register_graph(simple_graph)

    assert await worker_handler.commit(
        graph=worker_handler.graph, mapping={worker.id: ["Gen1", "Con1"]}
    )
    assert await worker_handler.start_workers()
    await worker_handler.diagnostics(enable=True)

    await asyncio.sleep(4)
    await worker_handler.diagnostics(enable=False)

    assert await worker_handler.stop()
    assert await worker_handler.collect()

    # Teardown
    assert await worker_handler.reset()

    session_folder = list(worker_handler.state.logdir.iterdir())[0]
    assert (session_folder / "Con1" / "diagnostics.csv").exists()
