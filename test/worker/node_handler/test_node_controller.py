import asyncio
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


async def test_thread_node_controller():
    session = ThreadSession()
    node = GenNode(name="Gen1")

    node_controller = ThreadNodeController(node, logger)  # type: ignore
    node_controller.run(session)
    await asyncio.sleep(0.25)

    output = await node_controller.shutdown()
    assert output == OUTPUT
