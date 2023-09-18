import abc
import asyncio
from functools import partial
from typing import List, Awaitable, Callable, Union
from concurrent.futures import ThreadPoolExecutor, Future

import multiprocess as mp


class MultiprocessExecutor:
    def __init__(self, processes=None):
        self.pool = mp.Pool(processes)

    def submit(self, fn, *args, **kwargs):
        future = Future()
        result = self.pool.apply_async(
            fn,
            args,
            kwargs,
            callback=future.set_result,
            error_callback=future.set_exception,
        )
        future._result = result  # Store this to prevent it from being garbage-collected
        return future

    def shutdown(self, wait=True):
        self.pool.close()
        if wait:
            self.pool.join()


class ContextSession(abc.ABC):

    futures: List[Awaitable]
    loop: asyncio.AbstractEventLoop
    pool: Union[MultiprocessExecutor, ThreadPoolExecutor]

    async def wait_for_all(self):
        await asyncio.wait(self.futures)

    def add(self, f: Callable, *args, **kwargs) -> Awaitable:
        ret = self.loop.run_in_executor(self.pool, partial(f, *args, **kwargs))
        self.futures.append(ret)
        return ret


class MPSession(ContextSession):
    def __init__(self):
        self.loop = asyncio.get_running_loop()
        self.pool = MultiprocessExecutor()
        self.futures = []


class ThreadSession(ContextSession):
    def __init__(self):
        self.loop = asyncio.get_running_loop()
        self.pool = ThreadPoolExecutor()
        self.futures = []
