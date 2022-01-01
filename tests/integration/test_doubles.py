# Built-in Imports
from typing import Dict
import time
import unittest
import pathlib
import shutil
import os
import sys

# Third-Party Imports
import pandas as pd
import tqdm

# Testing Library
import pymmdt as mm
import pymmdt.utils.tobii

# Creating test pipe class and instance
class TestExamplePipe(mm.Pipe):
    def step(self, data_samples: Dict[str, Dict[str, pd.DataFrame]]):
        data_streams_samples = list(data_samples.values())[0]
        self.session.add_tabular('test_tabular', data_streams_samples['test_tabular'])
        self.session.add_video('test_video', data_streams_samples['test_video'])
        # self.session.add_images('test_images', data_streams_samples['test_video'])
        # self.session.add_image('test_image', data_streams_samples['test_video'].iloc[0].to_frame())
