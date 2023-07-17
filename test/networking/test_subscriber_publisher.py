# Built-in Imports
import time

# Third-party Imports
import pytest
import numpy as np
from pytest_lazyfixture import lazy_fixture

# Internal Imports
import chimerapy.engine as cpe
from chimerapy.engine.networking.publisher import Publisher
from chimerapy.engine.networking.subscriber import Subscriber

logger = cpe._logger.getLogger("chimerapy-engine")
cpe.debug()


@pytest.fixture
def publisher():
    pub = Publisher()
    pub.start()
    yield pub
    pub.shutdown()


@pytest.fixture
def subscriber(publisher):
    sub = Subscriber(host=publisher.host, port=publisher.port)
    sub.start()
    yield sub
    sub.shutdown()


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


def test_pub_instance(publisher):
    ...


def test_sub_instance(subscriber):
    ...


@pytest.mark.parametrize(
    "data_chunk",
    [(lazy_fixture("text_data_chunk")), (lazy_fixture("image_data_chunk"))],
)
def test_sending_data_chunk_between_pub_and_sub(publisher, subscriber, data_chunk):

    time.sleep(5)
    publisher.publish(data_chunk)
    logger.debug(f"{publisher}: published {data_chunk}")

    new_data = subscriber.receive(timeout=10)
    logger.debug(f"{subscriber}: received {new_data}")
    assert new_data == data_chunk
