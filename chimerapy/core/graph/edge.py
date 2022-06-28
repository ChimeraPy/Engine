import multiprocessing as mp
from multiprocessing.managers import BaseProxy

from chimerapy.core.data_chunk import DataChunk
from chimerapy.core.queue import PortableQueue
from chimerapy.utils.tools import SharedCounter

import logging
logger = logging.getLogger(__name__)


class ProcessEdge(PortableQueue):
    def __init__(self, *args, **kwargs):
        self.memory_manager = kwargs.pop("memory_manager")
        self.name = kwargs.pop("name")
        # the window index keeps track of the next data chunk that will be available when doing a get
        # this is important because when fetching dapta from multiple queue
        # for synchronization, we would want the processes to fetch datachunks that belong to the same indexes
        super().__init__(*args, **kwargs)
        
    def is_put_ready(self):
        ready = super().is_put_ready()
        # ready = ready and (
        #     self.memory_manager.total_memory_used > self.memory_manager.total_available_memory
        # )
        return ready
    
    def is_get_ready(self):
        ready = super().is_get_ready()
        return ready

    def put(self, data: DataChunk, *args, **kwargs):
        # if we are using too much of memory, we can just not add anything
        result = super().put(data, *args, **kwargs)
        # if we successfully put the item in the queue, we add that to the mem manager
        if result and self.memory_manager:
            self.memory_manager.add(data, self.name)
        
        return result

    def get(self, *args, **kwargs):
        data = super().get(*args, **kwargs)
        if data and self.memory_manager:
            self.memory_manager.remove(data)
        return data
