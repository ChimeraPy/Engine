import asyncio
import time

import multiprocess as mp

import chimerapy.engine as cpe
from chimerapy.engine.worker.node_handler_service.context_session import (
    MPSession,
    ThreadSession,
)

OUTPUT = 1

logger = cpe._logger.getLogger("chimerapy-engine")


def target():
    return OUTPUT


def looping_target(running):

    while running.value:
        time.sleep(1)
        logger.debug("Looping target")

    return OUTPUT


async def test_mp_context():
    session = MPSession()
    future = session.add(target)
    await session.wait_for_all()
    assert future.result() == OUTPUT
    logger.debug(future.result())
    session.shutdown()


async def test_thread_context():
    session = ThreadSession()
    future = session.add(target)
    await session.wait_for_all()
    assert future.result() == OUTPUT
    logger.debug(future.result())
    session.shutdown()


async def test_mp_shared_variable():
    manager = mp.Manager()
    running = manager.Value("i", 1)

    session = MPSession()
    future = session.add(looping_target, running)
    logger.debug("Started")

    await asyncio.sleep(2)

    running.value = 0
    logger.debug("Stop")

    # await session.wait_for_all()
    output = await future
    assert output == OUTPUT
    session.shutdown()
