# Built-in Imports
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
import chimerapy as cp

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
TEST_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = TEST_DIR / 'data' 
OUTPUT_DIR = TEST_DIR / 'test_output' 
    
class TestPipeline(cp.Pipeline):

    def __init__(self, data_streams:dict):
        super().__init__(name='test1', inputs=list(data_streams.values()))

        self.p1 = cp.Process(name='p1', inputs=[data_streams['tab']])
        self.p2 = cp.Process(name='p2', inputs=[data_streams['video'], self.p1])
        self.p3 = cp.Process(name='p3', inputs=[data_streams['tab'], self.p2])

def save_graph(G, name):
    pos = nx.spring_layout(G)
    nx.draw(G, pos, with_labels=True, node_size=100, font_size=8)

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
    tabular_ds = cp.TabularDataStream(
        name="test_tabular",
        data=csv_data,
        time_column="_time_"
    )
    video_ds = cp.VideoDataStream(
        name="test_video",
        start_time=pd.Timedelta(0),
        video_path=RAW_DATA_DIR/"example_use_case"/"test_video1.mp4",
    )

    data_streams = {'tab': tabular_ds, 'video': video_ds}

    return data_streams

@pytest.fixture
def pipeline(dss):

    # Create the pipeline
    pipeline = TestPipeline(dss)

    return pipeline

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

def test_merging_two_pipelines(dss, pipeline):

    class SecondTestPipeline(cp.Pipeline):

        def __init__(self, input_pipeline):
            super().__init__(name='test2', inputs=[input_pipeline])

            self.q1 = cp.Process(name='q1', inputs=[input_pipeline.p3])

    # Construct the pipeline
    second_pipeline = SecondTestPipeline(pipeline)
    
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

def test_forward_propagate(pipeline):

    # Run the pipeline
    pipeline.run()
