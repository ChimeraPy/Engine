# Built-in
from typing import Coroutine, Callable, Tuple, List, Optional, Any
import threading
import asyncio
from functools import partial

# Internal Imports
from .. import _logger

logger = _logger.getLogger("chimerapy")

# Reference
# https://stackoverflow.com/a/66055205/13231446


def waitable_callback(
    callback: Callable, args: List[Any]
) -> Tuple[threading.Event, Callable]:

    finished = threading.Event()
    finished.clear()

    # Create wrapper that signals when the callback finished
    def _wrapper(func: Callable, *args):
        output = func(*args)
        finished.set()
        return output

    wrapper = _wrapper(callback, *args)

    return finished, wrapper


# first, we need a loop running in a parallel Thread
class AsyncLoopThread(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self._loop = asyncio.new_event_loop()

    def callback(self, coro: Callable[[], Coroutine]):
        self._loop.create_task(coro())

    def exec(self, coro: Callable[[], Coroutine]):
        # future = asyncio.run_coroutine_threadsafe(coro(), self._loop)
        # self._loop.create_task(coro())
        self._loop.call_soon_threadsafe(partial(self.callback, coro))

    def exec_noncoro(
        self, callback: Callable, args: List[Any], waitable: bool = False
    ) -> Optional[threading.Event]:

        if waitable:
            finished, wrapper = waitable_callback(callback, args)
            self._loop.call_soon_threadsafe(wrapper, *args)
            return finished

        else:
            self._loop.call_soon_threadsafe(callback, *args)

    def run(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def stop(self):

        # Cancel all tasks
        for task in asyncio.all_tasks(self._loop):
            task.cancel()

        # Then stop the loop
        self._loop.stop()
