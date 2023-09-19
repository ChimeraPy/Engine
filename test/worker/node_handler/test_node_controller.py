import asyncio


import chimerapy.engine as cpe
from chimerapy.engine.worker.node_handler_service.context_session import (
    MPSession,
    ThreadSession,
)
from chimerapy.engine.worker.node_handler_service.node_controller import (
    MPNodeController,
    ThreadNodeController,
)

from ...conftest import GenNode

logger = cpe._logger.getLogger("chimerapy-engine")

OUTPUT = 1

# class TestNode:
#     def run(
#         self,
#         blocking: bool = True,
#         running: Optional[mp.Value] = None,  # type: ignore
#         eventbus=None,
#     ):
#         while running:
#             time.sleep(0.1)
#         return OUTPUT


async def test_mp_node_controller():
    session = MPSession()
    # node = TestNode()
    node = GenNode(name="Gen1")
    node_controller = MPNodeController(node, logger)  # type: ignore
    node_controller.run(session)
    await asyncio.sleep(0.25)
    await node_controller.shutdown()
    assert node_controller.future.result() == OUTPUT


async def test_thread_node_controller():
    session = ThreadSession()
    # node = TestNode()
    node = GenNode(name="Gen1")
    node_controller = ThreadNodeController(node, logger)  # type: ignore
    node_controller.run(session)
    await asyncio.sleep(0.25)
    await node_controller.shutdown()
    assert node_controller.future.result() == OUTPUT
