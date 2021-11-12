import unittest
import pathlib

import pandas as pd
import networkx as nx

import pymmdt
import pymmdt.video
import pymmdt.tabular

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
        ds1 = pymmdt.tabular.OfflineTabularDataStream(
            name='csv',
            data=a1_data,
            time_column='time',
            data_columns=['data']
        )
        ds2 = pymmdt.video.OfflineVideoDataStream(
            name='video', 
            video_path=DATA_DIR / 'test_video1.mp4', 
            start_time=start_time
        )

        # Create processes
        show_video = pymmdt.video.ShowVideo(
            inputs=['drawn_video'],
        )

        # Initiate collector and analyzer
        self.processes = [show_video]
        self.collector = pymmdt.OfflineCollector([ds1, ds2])
        self.session = pymmdt.Session()
        self.analyzer = pymmdt.Analyzer(
            self.collector, 
            self.processes,
            self.session
        )

    def test_process_output_data_sample(self):

        # Get a test sample
        csv_sample = self.collector.data_streams['csv'][0]
        video_sample = self.collector.data_streams['video'][0] 

        # Storing the samples in the session
        self.session.update(csv_sample)
        self.session.update(video_sample)

        # Test that the output is indeed a DataSample
        output = self.session.apply(self.processes[0])
        # output2 = self.session.apply(self.processes[1])

        # self.assertTrue(isinstance(output, pymmdt.DataSample))
        # self.assertTrue(isinstance(output2, pymmdt.DataSample))

if __name__ == "__main__":
    unittest.main()

    # Have to manually write this to allow the debugger access
    # test = TestProcess()
    # test.setUp()
    # test.test_process_output_data_sample()
