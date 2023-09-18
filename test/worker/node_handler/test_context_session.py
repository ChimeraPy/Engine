import chimerapy.engine as cpe
from chimerapy.engine.worker.node_handler_service.context_session import (
    MPSession,
    ThreadSession,
)


logger = cpe._logger.getLogger("chimerapy-engine")


def target():
    return 9999


async def test_mp_context():
    session = MPSession()
    future = session.add(target)
    await session.wait_for_all()
    assert future.result() == 9999
    logger.debug(future.result())


async def test_thread_context():
    session = ThreadSession()
    future = session.add(target)
    await session.wait_for_all()
    assert future.result() == 9999
    logger.debug(future.result())
