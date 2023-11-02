import asyncio
from typing import List

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

from ...conftest import GenNode

logger = cpe._logger.getLogger("chimerapy-engine")

OUTPUT = 1


async def test_mp_node_controller():
    mp_manager = mp.Manager()
    session = MPSession()
    node = GenNode(name="Gen1")

    node_controller = MPNodeController(node, logger)  # type: ignore
    node_controller.set_mp_manager(mp_manager)
    node_controller.run(session)
    await asyncio.sleep(0.25)

    output = await node_controller.shutdown()
    assert output == OUTPUT


async def test_mp_node_controller_multiple():
    mp_manager = mp.Manager()
    session = MPSession()

    controllers: List[MPNodeController] = []
    for i in range(10):
        node = GenNode(name=f"Gen{i}")

        node_controller = MPNodeController(node, logger)
        controllers.append(node_controller)
        node_controller.set_mp_manager(mp_manager)
        node_controller.run(session)

    await asyncio.sleep(0.25)

    for c in controllers:
        output = await c.shutdown()
        assert output == OUTPUT


async def test_thread_node_controller():
    session = ThreadSession()
    node = GenNode(name="Gen1")

    node_controller = ThreadNodeController(node, logger)  # type: ignore
    node_controller.run(session)
    await asyncio.sleep(0.25)

    output = await node_controller.shutdown()
    assert output == OUTPUT
