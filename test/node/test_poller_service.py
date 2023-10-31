import asyncio

import pytest
from aiodistbus import EventBus

import chimerapy.engine as cpe
from chimerapy.engine.data_protocols import NodePubEntry, NodePubTable
from chimerapy.engine.networking.data_chunk import DataChunk
from chimerapy.engine.networking.publisher import Publisher
from chimerapy.engine.node.poller_service import PollerService
from chimerapy.engine.states import NodeState

logger = cpe._logger.getLogger("chimerapy-engine")


@pytest.fixture
async def poller_setup(bus):

    # Create sample state
    state = NodeState()

    # Create the service
    poller = PollerService(
        "poller",
        in_bound=["pub_mock"],
        in_bound_by_name=["pub_mock"],
        follow="pub_mock",
        state=state,
    )
    await poller.attach(bus)

    pub = Publisher()
    pub.start()

    yield (poller, pub)

    await poller.teardown()
    pub.shutdown()


async def test_instanticate(poller_setup):
    ...


async def test_setting_connections(poller_setup):

    poller, pub = poller_setup

    node_pub_table = NodePubTable(
        {"pub_mock": NodePubEntry(ip=pub.host, port=pub.port)}
    )
    await poller.setup_connections(node_pub_table)


async def test_poll_message(poller_setup):

    poller, pub = poller_setup

    # Setup
    node_pub_table = NodePubTable(
        {"pub_mock": NodePubEntry(ip=pub.host, port=pub.port)}
    )
    await poller.setup_connections(node_pub_table)

    # Send a message
    await asyncio.sleep(1)
    data_chunk = DataChunk()
    await pub.publish(data_chunk.to_bytes())

    # Sleep
    await asyncio.sleep(1)
    assert poller.emit_counter > 0
