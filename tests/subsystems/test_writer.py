# Built-in Imports
import json
import time
import unittest
import pathlib
import shutil
import os
import multiprocessing as mp
from multiprocessing.managers import BaseManager
import queue

# Third-Party Imports
import pytest
import pprint 
import numpy as np
import pandas as pd

# Testing Library
import chimerapy as cp
from chimerapy.core.writer import Writer
from chimerapy.core.session import Session
from chimerapy.utils import MemoryManager

# Perform global information
BaseManager.register('MemoryManager', MemoryManager)

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
TEST_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = TEST_DIR / 'data' 
OUTPUT_DIR = TEST_DIR / 'test_output' 

@pytest.fixture
def memory_manager():
    
    # Create memory manager that is shared between processes
    base_manager = BaseManager()
    base_manager.start()
    memory_manager = MemoryManager(
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
        startup_now=True
    )
    dss = {'P01': [tabular_ds, video_ds]}

    return dss

@pytest.fixture
def writer(memory_manager):
    
    # Creating the parameters to the writer
    logdir = OUTPUT_DIR
    experiment_name = 'testing_writer'
    
    # Clear out the previous run
    experiment_dir = logdir / experiment_name
    if experiment_dir.exists():
        shutil.rmtree(experiment_dir)
        os.mkdir(experiment_dir)

  
    # Create the writer
    writer = Writer(
        logdir=logdir,
        memory_manager=memory_manager,
        experiment_name='testing_writer',
    )

    yield writer
    writer.shutdown()
    writer.join()

def adding_test_data(dss, session):
    
    # Timing
    start_time = pd.Timedelta(seconds=0)
    tab_end_time = pd.Timedelta(seconds=10)
    video_end_time = pd.Timedelta(seconds=0.1)
    
    test_tabular_data = dss['P01'][0].get(start_time, tab_end_time)
    test_video_data = dss['P01'][1].get(start_time, video_end_time)

    # Test all types of logging
    session.add_tabular(
        name='test_tabular',
        data=test_tabular_data,
        time_column='_time_'
    )
    session.add_image(
        name='test_image_with_timestamp',
        data=test_video_data['frames'].iloc[0],
        timestamp=test_video_data['_time_'].iloc[0]
    )
    session.add_images(
        name='test_images',
        df=test_video_data,
        data_column='frames',
        time_column='_time_'
    )
    session.add_video(
        name='test_video',
        df=test_video_data,
    )

def test_running_and_shutting_down_writer(writer):

    writer.start()

def test_creating_and_simple_running_writer(
        dss,
        memory_manager, 
        writer
    ):

    # Create root and other sessions that interface to the writer
    root_session = Session(
        name='root',
        memory_manager=memory_manager,
        logging_queue=writer.data_to_queue
    )
    subsession = Session(
        name='P01',
        memory_manager=memory_manager,
        logging_queue=writer.data_to_queue
    )

    # Add data
    adding_test_data(dss, root_session)
    adding_test_data(dss, subsession)

    assert writer.data_to_queue.qsize() != 0

    # Check how much data chunks were passed to the writer
    total_logged_data = writer.data_to_queue.qsize()
    print(f'Total logged data: {total_logged_data}')
    
    # Start the writer
    writer.start()

    print("Started writer")

    # Wait until the writer is done
    while writer.data_to_queue.qsize() != 0:
        time.sleep(0.1)

    assert writer.data_to_queue.qsize() == 0

    # Now we have to check that the writer did the right behavior
    # assert writer.data_to_queue.qsize() == 0, f"logging queue should be empty, instead it is {writer.data_to_queue.qsize()}"
    # assert len(messages) == total_logged_data + 1, f"For each logged data, we should receive logging confirmation from the writer (exp: {total_logged_data+1}, actual: {len(messages)})."

def test_writer_memory_stress(
        dss,
        memory_manager, 
        writer
    ):

    # Create root and other sessions that interface to the writer
    root_session = Session(
        name='root',
        memory_manager=memory_manager,
        logging_queue=writer.data_to_queue
    )
    subsession = Session(
        name='P01',
        memory_manager=memory_manager,
        logging_queue=writer.data_to_queue
    )

    print("Finished session construction")

    # Add data
    for i in range(3):
        adding_test_data(dss, root_session)
        adding_test_data(dss, subsession)

    time.sleep(3)
    
    # Start the writer
    writer.start()

def test_single_session_threading_saving_and_closing(
        dss,
        memory_manager, 
        writer
    ):

    # Create root and other sessions that interface to the writer
    root_session = Session(
        name='root',
        memory_manager=memory_manager,
        logging_queue=writer.data_to_queue
    )

    print("Finished session construction")

    # Add data
    for i in range(3):
        adding_test_data(dss, root_session)

    # Start the writer
    writer.start()
    writer.shutdown()
    writer.join()

    # Check about the generated meta file
    expected_meta = {
        'id': 'root',
        'records': {
            'root': {
                'test_tabular': {
                    'dtype': 'tabular',
                    'start_time': str(pd.Timedelta(0)),
                    'end_time': str(pd.Timedelta(seconds=9))
                },
                'test_image_with_timestamp': {
                    'dtype': 'image',
                    'start_time': str(pd.Timedelta(0)),
                    'end_time': str(pd.Timedelta(0)),
                },
                'test_images': {
                    'dtype': 'image',
                    'start_time': str(pd.Timedelta(0)),
                    'end_time': str(pd.Timedelta(seconds=0.482758612)),
                },
                'test_video': {
                    'dtype': 'video',
                    'start_time': str(pd.Timedelta(0)),
                    'end_time': str(pd.Timedelta(seconds=0.482758612)),
                }
            }
        },
        'subsessions': []
    }
    with open(writer.experiment_dir / "meta.json", "r") as json_file:
        actual_meta = json.load(json_file)

    print("EXPECTED META")
    pprint.pprint(expected_meta)
    print(2*"\n")

    print("ACTUAL META")
    pprint.pprint(actual_meta)
