import asyncio

import pytest

import chimerapy.engine as cpe
from chimerapy.engine.async_timer import AsyncTimer

logger = cpe._logger.getLogger("chimerapy-engine")

# Constants
A_LIST = []


async def callback():
    A_LIST.append(1)


@pytest.fixture
def timer():
    timer = AsyncTimer(callback, 0.25)
    return timer


async def test_async_timer(timer):
    await timer.start()
    await asyncio.sleep(2)
    await timer.stop()
    assert len(A_LIST) >= 7
