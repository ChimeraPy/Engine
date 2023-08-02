import time

import pytest

import chimerapy.engine as cpe
from chimerapy.engine.node.worker_comms_service import WorkerCommsService
from chimerapy.engine.node.node_config import NodeConfig
from chimerapy.engine.states import NodeState
from chimerapy.engine.eventbus import EventBus
from chimerapy.engine.networking.data_chunk import DataChunk
from chimerapy.engine.networking.server import Server
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread
from chimerapy.engine.networking.publisher import Publisher
from chimerapy.engine.data_protocols import NodePubTable, NodePubEntry


logger = cpe._logger.getLogger("chimerapy-engine")


@pytest.fixture
def server():
    server = Server(
        id="test_server",
        port=0,
    )
    server.serve()
    yield server
    server.shutdown()


@pytest.fixture
def worker_comms_setup(server):

    # Event Loop
    thread = AsyncLoopThread()
    thread.start()
    eventbus = EventBus(thread=thread)

    # Create sample state
    state = NodeState()
    node_config = NodeConfig(None)

    # Create the service
    worker_comms = WorkerCommsService(
        "worker_comms",
        host=server.host,
        port=server.port,
        node_config=node_config,
        state=state,
        eventbus=eventbus
    )

    yield worker_comms

    thread.exec(worker_comms.teardown()).result(timeout=30)


def test_instanticate(worker_comms_setup):
    ...
