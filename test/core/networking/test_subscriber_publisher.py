# Built-in Imports
import asyncio
from typing import Dict

import numpy as np

# Third-party Imports
import pytest
from pytest_lazyfixture import lazy_fixture

# Internal Imports
import chimerapy.engine as cpe
from chimerapy.engine.networking.publisher import Publisher
from chimerapy.engine.networking.subscriber import Subscriber

logger = cpe._logger.getLogger("chimerapy-engine")
cpe.debug()


@pytest.fixture
async def publisher():
    pub = Publisher()
    pub.start()
    yield pub
    pub.shutdown()


@pytest.fixture
async def subscriber(publisher):
    sub = Subscriber()
    sub.subscribe(host=publisher.host, port=publisher.port, id="test")
    yield sub
    await sub.shutdown()


@pytest.fixture
def text_data_chunk():
    # Create the data
    data = cpe.DataChunk()
    data.add(name="msg", value="HELLO")
    return data


@pytest.fixture
def image_data_chunk():
    # Create the data
    data = cpe.DataChunk()
    test_image = (np.random.rand(100, 100, 3) * 255).astype(np.uint8)
    data.add(name="test_image", value=test_image, content_type="image")
    return data


async def test_pub_instance(publisher):
    ...


async def test_sub_instance(subscriber):
    ...


@pytest.mark.parametrize(
    "data_chunk",
    [(lazy_fixture("text_data_chunk")), (lazy_fixture("image_data_chunk"))],
)
async def test_sending_data_chunk_between_pub_and_sub(
    publisher, subscriber, data_chunk
):

    flag = asyncio.Event()
    expected_data_chunk = None

    def update(datas: Dict[str, bytes]):
        nonlocal expected_data_chunk
        expected_data_chunk = cpe.DataChunk.from_bytes(datas["test"])
        flag.set()

    subscriber.on_receive(update)
    await subscriber.start()
    await publisher.publish(data_chunk.to_bytes())

    await asyncio.wait_for(flag.wait(), timeout=5)
    assert expected_data_chunk == data_chunk
