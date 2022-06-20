# Built-in Imports
import pathlib
import os
import time
import queue
import multiprocessing as mp
from multiprocessing.managers import BaseManager
import psutil
import logging
logger = logging.getLogger(__name__)

# Third-Party Imports
import pytest
import numpy as np
import pandas as pd

# Testing Library
import chimerapy as cp

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
TEST_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = TEST_DIR / 'data' 
OUTPUT_DIR = TEST_DIR / 'test_output' 

@pytest.fixture
def memory_manager():
    
    # Create memory manager that is shared between processes
    base_manager = cp.utils.MPManager()
    base_manager.start()
    memory_manager = cp.utils.MemoryManager(
        memory_limit=0.5
    )

    return memory_manager

@pytest.fixture
def dss():

    # Storing the data
    csv_data = pd.read_csv(RAW_DATA_DIR/"example_use_case"/"test.csv")
    csv_data['_time_'] = pd.to_timedelta(csv_data['time'], unit="s")

    # Create each type of data stream
    tabular_ds = cp.TabularDataStream(
        name="test_tabular",
        data=csv_data,
        time_column="_time_"
    )
    video_ds = cp.VideoDataStream(
        name="test_video",
        start_time=pd.Timedelta(seconds=0),
        video_path=RAW_DATA_DIR/"example_use_case"/"test_video1.mp4",
    )
    dss = {'P01': [tabular_ds, video_ds]}

    return dss

@pytest.fixture
def reader(dss, memory_manager):
    
    # Create data reader
    reader_i = cp.Reader(
        memory_manager=memory_manager,
        users_data_streams=dss,
        time_window=pd.Timedelta(seconds=0.1),
    )

    yield reader_i
    reader_i.shutdown()
    reader_i.join()

def test_start_and_shutting_down(reader):

    # Start 
    reader.start()
    time.sleep(0.5)
    # Stop
    reader.shutdown()
    reader.join()
    assert reader.get_running() == True

def test_memory_tracking(reader):

    NUM_OF_STEPS = 5

    # Start the reader and get data
    reader.setup()
    assert reader.memory_manager.total_memory_used() == 0

    for i in range(NUM_OF_STEPS):
        data_chunk = reader.step()
        reader.out_queue.put(data_chunk)

    # Check the memory
    assert len(reader.memory_manager.get_uuids()) == NUM_OF_STEPS
    assert reader.memory_manager.total_memory_used() != 0

    # Now get all the content from the reader
    while True:
        try:
            data_chunk = reader.get(timeout=0.1)
            del data_chunk
        except queue.Empty:
            break

    # Now the memory should be empty
    assert len(reader.memory_manager.get_uuids()) == 0
    assert reader.memory_manager.total_memory_used() == 0

def test_memory_limitting(reader):

    # Constants
    SAFE_NUM_OF_STEPS = 1000

    # Variables
    previous_memory_usage = 0
    memory_usage = 0
    memory_maxed = False

    # Start the reader and get data
    reader.setup()

    for i in range(SAFE_NUM_OF_STEPS):

        # Get data
        data_chunk = reader.step(index=0)

        # Check how much memory is being used
        memory_usage = reader.memory_manager.total_memory_used()

        # When reader is taking all memory, the memory manager will prevent
        # reading more data
        if type(data_chunk) == None:
            assert reader.memory_manager.total_memory_used() >= 1
            memory_maxed = True
            break
        else:
            assert memory_usage >= previous_memory_usage
            # To avoid crashing, let's just delete the memory for now
            del data_chunk
            previous_memory_usage = memory_usage

    # We need to prove that the system will prevent overloading data
    assert memory_usage != 0
    assert memory_maxed == False, f"The memory max event didn't occured, used {memory_usage:.2f}"

def test_creating_and_simple_running_reader(reader):

    # Start the reader and get data
    reader.start()
    logger.info("TEST: reader - start")

    time.sleep(1)

    # Pause the reader
    reader.pause()
    logger.info("TEST: reader - pause")

    # Wait until the reader fully stops
    time.sleep(1)
    # assert False

    assert reader.out_queue.qsize() != 0
    # Then get the data
    # datas = []
    # while True:
    #     try:
    #         data = reader.get(timeout=0.1)
    #         datas.append(data)
    #     except queue.Empty:
    #         break

    # assert len(datas) != 0
    
def test_changing_the_index_of_reader(reader):

    # Start the reader and get data
    reader.start()
    reader.pause()
       
    # Message to set window index
    window_index_message = {
        'header': 'UPDATE',
        'body': {
            'type': 'WINDOW_INDEX',
            'content': {
                'window_index': 100
            }
        }
    }
    reader.put_message(window_index_message)

    # Let the reader run to get windows from that range
    reader.resume()

    # Wait until the reader fully stops
    time.sleep(1)
    reader.pause()

    # Then get the data
    datas = []
    while True:
        try:
            data = reader.get(timeout=0.1)
            datas.append(data)
        except queue.Empty:
            break

    assert len(datas) != 0 and datas[0]['window_index'] >= 100    
