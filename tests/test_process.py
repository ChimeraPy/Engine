import unittest
import pathlib

import matplotlib.pyplot as plt
import pandas as pd
import networkx as nx
from networkx.drawing.nx_agraph import write_dot, graphviz_layout 

import mm
import mm.video
import mm.logs

TEST_DIR = pathlib.Path.cwd() / 'tests'
DATA_DIR = TEST_DIR  / 'data'
TEST_OUTPUT = TEST_DIR / 'test_output'

class TestProcess(unittest.TestCase):

    def setUp(self):

        # Load data 
        a1_data = pd.read_csv(DATA_DIR / 'test.csv')

        # Converting the UNIX timestamps to pd.DatetimeIndex to improve consistency and reability
        a1_data['time'] = pd.to_datetime(a1_data['time'], unit='ms')

        # Get start time
        start_time = a1_data['time'][0]
     
        # Construct data streams
        ds1 = mm.logs.OfflineCSVDataStream(
            name='csv',
            data=a1_data,
            time_column='time',
            data_columns=['data']
        )
        ds2 = mm.video.OfflineVideoDataStream(
            name='video', 
            video_path=DATA_DIR / 'test_video1.mp4', 
            start_time=start_time
        )

        # Create processes
        draw_text = mm.video.ProcessDrawText(
            name='draw_video',
            inputs=['csv', 'video']
        )
        show_video = mm.video.ProcessShowVideo(
            name='show_video',
            inputs=['draw_video']
        )

        # Initiate collector and analyzer
        self.processes = [draw_text, show_video]
        self.collector = mm.OfflineCollector([ds1, ds2])
        self.analyzer = mm.Analyzer(self.collector, self.processes)
        self.session = mm.Session()

    def test_process_output_data_sample(self):

        # Get a test sample
        csv_sample = self.collector.data_streams['csv'][0]
        video_sample = self.collector.data_streams['video'][0] 

        # Storing the samples in the session
        self.session.update(csv_sample)
        self.session.update(video_sample)

        # Test that the output is indeed a DataSample
        output = self.session.apply(self.processes[0])

        self.assertTrue(isinstance(output, mm.DataSample))

if __name__ == "__main__":
    # unittest.main()

    # Have to manually write this to allow the debugger access
    test = TestProcess()
    test.setUp()
    test.test_process_output_data_sample()
