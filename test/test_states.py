import asyncio

from aiodistbus import make_evented

from chimerapy.engine import _logger
from chimerapy.engine.states import WorkerState

logger = _logger.getLogger("chimerapy-engine")


async def handler(*args, **kwargs):
    logger.debug("Received data")
    logger.debug(f"{args}, {kwargs}")


async def test_make_evented(bus, entrypoint):
    state = WorkerState()
    state = make_evented(state, bus=bus)
    await entrypoint.on("WorkerState.changed", handler)
    state.name = "test"
    await asyncio.sleep(1)
    assert len(entrypoint._received) >= 1
