from typing import Optional

import multiprocess as mp

import chimerapy.engine as cpe
from chimerapy.engine.worker.node_handler_service.context_session import (
    MPSession,
    ThreadSession,
)
from chimerapy.engine.worker.node_handler_service.node_controller import (
    MPNodeController,
    ThreadNodeController,
)

logger = cpe._logger.getLogger("chimerapy-engine")


class TestNode:
    def run(
        self,
        blocking: bool = True,
        running: Optional[mp.Value] = None,  # type: ignore
        eventbus=None,
    ):
        return 2


async def test_mp_node_controller():
    session = MPSession()
    node = TestNode()
    node_controller = MPNodeController(node, logger)  # type: ignore
    node_controller.run(session)
    await session.wait_for_all()
    assert node_controller.future.result() == 2


async def test_thread_node_controller():
    session = ThreadSession()
    node = TestNode()
    node_controller = ThreadNodeController(node, logger)  # type: ignore
    node_controller.run(session)
    await session.wait_for_all()
    assert node_controller.future.result() == 2
