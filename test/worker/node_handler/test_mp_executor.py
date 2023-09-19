import time
import multiprocess as mp
import asyncio

import chimerapy.engine as cpe
from chimerapy.engine.worker.node_handler_service.context_session import (
    MultiprocessExecutor,
)

logger = cpe._logger.getLogger("chimerapy-engine")
manager = mp.Manager()


def worker(running, counter):
    while running.value:
        counter.value += 1
        logger.debug(f"Counter: {counter.value}")
        time.sleep(1)
    return counter.value


async def test_mp_executor():
    running = manager.Value("i", 1)
    counter = manager.Value("i", 0)

    loop = asyncio.get_running_loop()
    pool = mp.Pool(processes=1)
    executor = MultiprocessExecutor(pool)

    future = loop.run_in_executor(executor, worker, running, counter)
    await asyncio.sleep(2)

    running.value = 0
    final_counter = await future
    assert final_counter != 0
