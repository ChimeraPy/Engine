# Built-in Imports
from typing import Dict
import time
import signal
import unittest
import pathlib
import shutil
import os
import sys
import threading

# Third-Party Imports
import pytest
import pandas as pd
import tqdm
import networkx as nx
import matplotlib.pyplot as plt

# ChimeraPy Library
from chimerapy.core.process import Process
from chimerapy.core.pipeline import Pipeline
from chimerapy.core.tabular import TabularDataStream
from chimerapy.core.video import VideoDataStream

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
TEST_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = TEST_DIR / 'data' 
OUTPUT_DIR = TEST_DIR / 'test_output' 

class SimpleProcess(Process):

    def step(self, data_samples: Dict[str, pd.DataFrame]):
        return data_samples[list(data_samples.keys())[0]]

class PipelineTest(Pipeline):

    def __init__(self, data_streams:dict):
        super().__init__(name='test1', inputs=list(data_streams.values()), logdir=OUTPUT_DIR)

        self.p1 = SimpleProcess(name='p1', inputs=[data_streams['tab']])
        self.p2 = SimpleProcess(name='p2', inputs=[data_streams['video'], self.p1])
        self.p3 = SimpleProcess(name='p3', inputs=[data_streams['tab'], self.p2])

def save_graph(G, name):
    pos = nx.spring_layout(G)
    nx.draw(G, pos, with_labels=True, node_size=100, font_size=10)

    plt.tight_layout()
    # plt.show()
    plt.savefig(f"{name}.png", format="PNG")
    plt.clf()

@pytest.fixture
def dss():
 
    # Storing the data
    csv_data = pd.read_csv(RAW_DATA_DIR/"example_use_case"/"test.csv")
    csv_data['_time_'] = pd.to_timedelta(csv_data['time'], unit="s")

    # Create each type of data stream
    tabular_ds = TabularDataStream(
        name="test_tabular",
        data=csv_data,
        time_column="_time_"
    )
    video_ds = VideoDataStream(
        name="test_video",
        start_time=pd.Timedelta(0),
        video_path=RAW_DATA_DIR/"example_use_case"/"test_video1.mp4",
    )

    data_streams = {'tab': tabular_ds, 'video': video_ds}

    return data_streams

@pytest.fixture
def pipeline(dss):
    pipeline = PipelineTest(dss)
    yield pipeline
    pipeline.shutdown()
    pipeline.join()

def check_if_equal_layer(a, b):

    for aa in a:
        if aa not in b:
            return False

    return True

def test_pipeline_nonrun_shutdown(dss):
    
    # Create the pipeline
    pipeline = PipelineTest(dss)
    pipeline.shutdown()
    pipeline.join()

def test_pipeline_graph_construction(dss, pipeline):

    # Now create the expected directed graph
    expected_graph = nx.DiGraph()
    expected_graph.add_nodes_from([
        dss['tab'].name, 
        dss['video'].name, 
        pipeline.p1.name, 
        pipeline.p2.name, 
        pipeline.p3.name
    ])
    expected_graph.add_edges_from([
        (dss['tab'].name, pipeline.p1.name),
        (dss['video'].name, pipeline.p2.name),
        (pipeline.p1.name, pipeline.p2.name),
        (pipeline.p2.name, pipeline.p3.name),
        (dss['tab'].name, pipeline.p3.name)
    ])

    # Visualize testing --- if debugging
    # save_graph(pipeline.graph, 'pipeline')
    # save_graph(expected_graph, 'expected')

    # Checking if they are equal
    assert nx.is_isomorphic(pipeline.graph, expected_graph)

    # Check that the layers are correct
    print(pipeline.layers)
    assert len(pipeline.layers) == 4
    assert check_if_equal_layer(pipeline.layers[0], [(dss['tab'].name, {'item': dss['tab']}), (dss['video'].name, {'item': dss['video']})])
    assert check_if_equal_layer(pipeline.layers[1], [(pipeline.p1.name, {'item': pipeline.p1})])
    assert check_if_equal_layer(pipeline.layers[2], [(pipeline.p2.name, {'item': pipeline.p2})])
    assert check_if_equal_layer(pipeline.layers[3], [(pipeline.p3.name, {'item': pipeline.p3})])

def test_merging_two_pipelines(dss, pipeline):

    class SecondPipelineTest(Pipeline):

        def __init__(self, input_pipeline):
            super().__init__(name='test2', inputs=[input_pipeline], logdir=OUTPUT_DIR)

            self.q1 = Process(name='q1', inputs=[input_pipeline.p3])

    # Construct the pipeline
    second_pipeline = SecondPipelineTest(pipeline)
    
    # Now create the expected directed graph
    expected_graph = nx.DiGraph()
    expected_graph.add_nodes_from([
        dss['tab'].name, 
        dss['video'].name, 
        pipeline.p1.name, 
        pipeline.p2.name, 
        pipeline.p3.name,
        second_pipeline.q1.name
    ])
    expected_graph.add_edges_from([
        (dss['tab'].name, pipeline.p1.name),
        (dss['video'].name, pipeline.p2.name),
        (pipeline.p1.name, pipeline.p2.name),
        (pipeline.p2.name, pipeline.p3.name),
        (dss['tab'].name, pipeline.p3.name),
        (pipeline.p3.name, second_pipeline.q1.name)
    ])
    
    # Visualize testing --- if debugging
    # save_graph(second_pipeline.graph, 'pipeline')

    # Checking if they are equal
    assert nx.is_isomorphic(second_pipeline.graph, expected_graph)

    second_pipeline.shutdown()
    second_pipeline.join()

def test_forward_propagate(pipeline):

    # Run the pipeline
    pipeline.step()

def test_run(pipeline):
    pipeline.run()
 
# # def test_runner_handling_keyboard_interrupt():

# #     def create_keyboard_interrupt():
# #         time.sleep(1)
# #         signal.raise_signal(signal.SIGINT)
# #         print("KEYBOARD INTERRUPT!")
    
# #     # Load construct the first runner
# #     self.runner = SingleRunner(
# #         name='P01',
# #         logdir=OUTPUT_DIR,
# #         data_streams=self.dss,
# #         pipe=self.individual_pipeline,
# #         time_window=pd.Timedelta(seconds=0.5),
# #         start_time=pd.Timedelta(seconds=0),
# #         end_time=pd.Timedelta(seconds=20),
# #         run_solo=True,
# #         verbose=True
# #     )

# #     # Create thread that later calls the keyboard interrupt signal
# #     signal_thread = threading.Thread(target=create_keyboard_interrupt, args=())
# #     signal_thread.start()

# #     # Run!
# #     self.runner.run()

# #     # Then stoping the thread
# #     signal_thread.join()


# # def test_group_runner_run(self):
    
# #     # Pass all the runners to the Director
# #     group_runner = GroupRunner(
# #         logdir=OUTPUT_DIR,
# #         name="Nurse Teamwork Example #1",
# #         pipe=self.overall_pipeline,
# #         runners=self.runners, 
# #         time_window=pd.Timedelta(seconds=0.5),
# #     )

# #     # Run the director
# #     group_runner.run()

# #     return None

# # def test_group_runner_with_shorter_run(self):
    
# #     # Pass all the runners to the Director
# #     group_runner = GroupRunner(
# #         logdir=OUTPUT_DIR,
# #         name="Nurse Teamwork Example #1",
# #         pipe=self.overall_pipeline,
# #         runners=self.runners, 
# #         time_window=pd.Timedelta(seconds=0.5),
# #         start_time=pd.Timedelta(seconds=5),
# #         end_time=pd.Timedelta(seconds=15),
# #     )

# #     # Run the director
# #     group_runner.run()

# #     return None

# # def test_group_runner_run_with_keyboard_interrupt_no_tui(self):
    
# #     def create_keyboard_interrupt():
# #         time.sleep(1)
# #         signal.raise_signal(signal.SIGINT)
    
# #     # Pass all the runners to the Director
# #     group_runner = GroupRunner(
# #         logdir=OUTPUT_DIR,
# #         name="Nurse Teamwork Example #1",
# #         pipe=self.overall_pipeline,
# #         runners=self.runners, 
# #         time_window=pd.Timedelta(seconds=0.5),
# #         verbose=True
# #     )
    
# #     # Create thread that later calls the keyboard interrupt signal
# #     signal_thread = threading.Thread(target=create_keyboard_interrupt, args=())
# #     signal_thread.start()

# #     # Run the director
# #     group_runner.run()
    
# #     # Then stoping the thread
# #     signal_thread.join()

# #     return None
