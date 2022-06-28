# Built-in Imports
from typing import Dict, Any, List, Literal, Union
import time
import uuid
import pickle
from multiprocessing.managers import BaseManager, NamespaceProxy
import logging

# Third-party imports
import psutil

# Setup the logger
logger = logging.getLogger(__name__)

def get_memory_data_size(data:Any) -> int:
    """Calculate the memory usage of a Python object.

    This was a solution to a memory leak issue. Here is the SO link:
    https://stackoverflow.com/q/71447286/13231446

    The main issue is that multiprocessing.Queue pickles an input and 
    unpickles when using ``get``. Numpy arrays do not handle this well,
    for their memory meta data is corrupted when this happends. This 
    caused memory to not be accurately computed. The solution was to 
    repickle the data and measure the len of the pickle string. This is
    a temporary solution, as I would like NumPy to solve this issue.

    Args:
        data (Any): The python object in question.

    Returns:
        int: Size of the Python object in bytes.
    """
    return len(pickle.dumps(data))

class MemoryManager:

    def __init__(
            self,
            memory_limit:float,
            verbose=bool
        ) -> None:

        # Keep track of the number of processed data chunks
        self.num_processed_data_chunks = 0
        self._total_available_memory = memory_limit * psutil.virtual_memory().available
        self.memory_usage_factor = 2.25
        self.memory_chunks = {}
        self.verbose = verbose
    
    def add_uuid(self, uuid:str, data_memory_usage:int, which:Literal['reader', 'pipeline', 'writer']) -> None:

        # logger.debug(f"MemoryManager: added uuid {uuid}")

        # Keep track if it is in the reader or the logging queues
        self.memory_chunks[uuid] = {
            'location': which,
            'data_memory_usage': data_memory_usage
        }

    def add(self, data_chunk:Dict, which:Literal['reader', 'pipeline', 'writer']) -> None:

        # Extract the important information
        uuid = data_chunk.id
        data_memory_usage = get_memory_data_size(data_chunk)
        self.add_uuid(uuid, data_memory_usage, which)
   
    def remove_uuid(self, uuid:str) -> bool:
        # Remove the data
        if uuid in self.memory_chunks.keys():
            del self.memory_chunks[uuid]
            return True
        else:
            return False

    def remove(self, data_chunk:Dict) -> bool:
        
        # Extract the important information
        uuid = data_chunk.id

        # Remove
        return self.remove_uuid(uuid)

    def get_uuids(self) -> List:
        return list(self.memory_chunks.keys())

    def total_memory_used(self, percentage:bool=True) -> Union[int, float]:
        memory_used = sum([x['data_memory_usage'] for x in self.memory_chunks.values()])

        if percentage:
            return memory_used / self._total_available_memory
        else:
            return memory_used
        
    def total_available_memory(self) -> int:
        return self._total_available_memory 

class MemoryManagerProxy(NamespaceProxy):
    _exposed_ = (
        '__getattribute__', 
        '__setattr__', 
        '__delattr__', 
        'add', 
        'add_uuid',
        'remove',
        'remove_uuid',
        'get_uuids',
        'total_memory_used',
        'total_available_memory'
    )

    def add(self, data_chunk:Dict, which:Literal['reader', 'pipeline', 'writer']) -> None:
        return self._callmethod(self.add.__name__, (data_chunk,which,))

    def add_uuid(self, uuid:str, data_memory_usage:int, which:Literal['reader', 'pipeline', 'writer']) -> None:
        return self._callmethod(self.add_uuid.__name__, (uuid, data_memory_usage, which,))
     
    def remove(self, data_chunk:Dict) -> None:
        return self._callmethod(self.remove.__name__, (data_chunk,))
    
    def remove_uuid(self, uuid:str) -> None:
        return self._callmethod(self.remove_uuid.__name__, (uuid,))

    def get_uuids(self) -> List:
        return self._callmethod(self.get_uuids.__name__)

    def total_memory_used(self, percentage:bool=True) -> Union[int, float]:
        return self._callmethod(self.total_memory_used.__name__, (percentage,))

    def total_available_memory(self) -> int:
        return self._callmethod(self.total_available_memory.__name__)

class MPManager(BaseManager): pass

# Perform global information
MPManager.register('MemoryManager', MemoryManager, MemoryManagerProxy)
