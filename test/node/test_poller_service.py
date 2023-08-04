import time

import pytest

import chimerapy.engine as cpe
from chimerapy.engine.node.poller_service import PollerService
from chimerapy.engine.states import NodeState
from chimerapy.engine.eventbus import EventBus
from chimerapy.engine.networking.data_chunk import DataChunk
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread
from chimerapy.engine.networking.publisher import Publisher
from chimerapy.engine.data_protocols import NodePubTable, NodePubEntry


logger = cpe._logger.getLogger("chimerapy-engine")


@pytest.fixture
def poller_setup():

    # Event Loop
    thread = AsyncLoopThread()
    thread.start()
    eventbus = EventBus(thread=thread)

    # Create sample state
    state = NodeState()

    # Create the service
    poller = PollerService(
        "poller",
        in_bound=["pub_mock"],
        in_bound_by_name=["pub_mock"],
        follow="pub_mock",
        state=state,
        eventbus=eventbus,
    )

    pub = Publisher()
    pub.start()

    yield (poller, pub)

    thread.exec(poller.teardown()).result(timeout=10)
    pub.shutdown()
    logger.debug("poller_setup fixture: shutdown complete")


def test_instanticate(poller_setup):
    ...


def test_setting_connections(poller_setup):

    poller, pub = poller_setup

    node_pub_table = NodePubTable(
        {"pub_mock": NodePubEntry(ip=pub.host, port=pub.port)}
    )
    poller.setup_connections(node_pub_table)


def test_poll_message(poller_setup):

    poller, pub = poller_setup

    # Setup
    node_pub_table = NodePubTable(
        {"pub_mock": NodePubEntry(ip=pub.host, port=pub.port)}
    )
    poller.setup_connections(node_pub_table)

    # Send a message
    time.sleep(1)
    data_chunk = DataChunk()
    pub.publish(data_chunk)

    # Sleep
    time.sleep(1)
    assert poller.eventbus._event_counts > 0
