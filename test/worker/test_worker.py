import chimerapy.engine as cpe

from ..networking.test_client_server import server
from ..streams.data_nodes import AudioNode, ImageNode, TabularNode, VideoNode

logger = cpe._logger.getLogger("chimerapy-engine")
cpe.debug()


# Constants
assert server
NAME_CLASS_MAP = {
    "vn": VideoNode,
    "img_n": ImageNode,
    "tn": TabularNode,
    "an": AudioNode,
}


async def test_worker_instance(worker):
    ...


async def test_worker_instance_shutdown_twice(worker):
    await worker.async_shutdown()


async def test_worker_instance_async():
    worker = cpe.Worker(name="local", id="local", port=0)
    await worker.aserve()
    await worker.async_shutdown()
