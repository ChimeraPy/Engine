
# Built-in Imports
import pathlib
import shutil
import os
import uuid
import multiprocessing as mp

# Third-party Imports
import pytest
import pandas as pd

# Internal Imports
from chimerapy.utils.memory_manager import MemoryManager, MPManager, get_memory_data_size
from chimerapy.core.loader import Loader
from chimerapy.core.logger import Logger
import chimerapy as cp

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
TEST_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = TEST_DIR / 'data' 
OUTPUT_DIR = TEST_DIR / 'test_output' 
    
@pytest.fixture
def memory_manager():
    
    # Create memory manager that is shared between processes
    base_manager = MPManager()
    base_manager.start()
    memory_manager = base_manager.MemoryManager(
        memory_limit=0.5
    )

    return memory_manager

def add_memory(mem_manager, data_chunk):

    # Use the data_chunk protocol
    mem_manager.add(data_chunk)

def remove_memory(mem_manager, data_chunk):

    # Use the data_chunk protocol
    deleted = mem_manager.remove(data_chunk)
    assert deleted == True

def test_shared_memory_between_processes(memory_manager):

    # Set number of processes
    N_OF_P = 4

    # Create a pool
    pool = mp.Pool(N_OF_P)

    # Add uuids to the memory manager
    data_chunks = []
    for i in range(N_OF_P):
        
        # Create a data chunk and save it
        data_chunk = {'uuid': uuid.uuid4(), 'data': [1,2,3,4,5]}
        data_chunks.append(data_chunk)

        pool.apply(func=add_memory, args=(memory_manager, data_chunk))
    
    # Get all of the uuids
    uuids = memory_manager.get_uuids()
    assert len(uuids) == N_OF_P

    # Check that the uuids are the same
    input_uuids = [x['uuid'] for x in data_chunks]
    input_uuids.sort()
    uuids.sort()
    assert input_uuids == uuids

    # Check that the memory is valid
    input_uuids_memory = {x['uuid']: get_memory_data_size(x) for x in data_chunks}
    for uuid_v in input_uuids_memory.keys():
        assert abs(input_uuids_memory[uuid_v] - memory_manager.loading_queue_memory_chunks[uuid_v]) < input_uuids_memory[uuid_v] * 0.10

    # Remove the uuids from memory manager
    for i in range(N_OF_P):
        data_chunk = data_chunks[i]
        pool.apply(func=remove_memory, args=(memory_manager, data_chunk))

    assert len(memory_manager.get_uuids()) == 0

    pool.close()
    pool.join()
