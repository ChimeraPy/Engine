import queue
import multiprocessing as mp

from multiprocessing.queues import Queue
from chimerapy.core.data_chunk import DataChunk

from chimerapy.utils.tools import SharedCounter
from chimerapy.utils.memory_manager import MemoryManager

import logging

# Logging
logger = logging.getLogger(__name__)


class PortableQueue(Queue):
    """ A portable implementation of multiprocessing.Queue.
    Because of multithreading / multiprocessing semantics, Queue.qsize() may
    raise the NotImplementedError exception on Unix platforms like Mac OS X
    where sem_getvalue() is not implemented. This subclass addresses this
    problem by using a synchronized shared counter (initialized to zero) and
    increasing / decreasing its value every time the put() and get() methods
    are called, respectively. This not only prevents NotImplementedError from
    being raised, but also allows us to implement a reliable version of both
    qsize() and empty().

    Code acquired from: 
    https://github.com/vterron/lemon/blob/d60576bec2ad5d1d5043bcb3111dff1fcb58a8d6/methods.py#L536-L573

    According to the StackOver post here:
    https://stackoverflow.com/questions/65609529/python-multiprocessing-queue-notimplementederror-macos
    
    Fixing the `size` not an attribute of Queue can be found here:
    https://stackoverflow.com/questions/69897765/cannot-access-property-of-subclass-of-multiprocessing-queues-queue-in-multiproce
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, ctx=mp.get_context())
        self.size = SharedCounter(0)
        self.alive = mp.Value('i', True)
    
    def __getstate__(self):
        return super().__getstate__() + (self.size,)

    def __setstate__(self, state):
        super().__setstate__(state[:-1])
        self.size = state[-1]

    def is_put_ready(self):
        # # logger.debug(f"{self.name} is put ready {self.size.value}, {self._maxsize}, {not self.alive.value}")
        if not self.alive.value or self.size.value >= self._maxsize:
            return False

        # # logger.debug(f"{self.name} is put ready (y)")
        return True
    
    def is_get_ready(self):
        if self.size.value == 0:
            return False

        return True

    def put(self, data: DataChunk, *args, **kwargs):
        if not self.is_put_ready():
            return False

        # # logger.debug(f"{self.name} is putting {data.data}")
        super().put(data, *args, **kwargs)
        self.size.increment(1)
        return True

    def get(self, *args, **kwargs) -> DataChunk:
        # Have to account for timeout to make this implementation
        # faithful to the complete mp.Queue implementation.
        if not self.is_get_ready():
            return False

        data = super().get(*args, **kwargs)
        self.size.increment(-1)

        return data

    def qsize(self):
        """ Reliable implementation of multiprocessing.Queue.qsize() """
        return self.size.value

    def empty(self):
        """ Reliable implementation of multiprocessing.Queue.empty() """
        return not self.qsize()

    def clear(self):
        """ Remove all elements from the Queue. """
        while not self.empty():
            try:
                # # logger.debug(f'{self.__class__.__name__}:clear, size={self.qsize()}')
                self.get(timeout=0.1)
            except queue.Empty:
                # # logger.debug(f'{self.__class__.__name__}:clear, empty!')
                return
            except EOFError:
                logger.warning("Queue EOFError --- data corruption")
                return 

    def destroy(self):
        self.clear()
        self.alive.value = False
