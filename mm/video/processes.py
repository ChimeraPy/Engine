# Subpackage management
__package__ = 'video'

from typing import List

import cv2

from mm.process import Process
from mm.data_sample import DataSample

class ProcessShowVideo(Process):

    def __init__(self, inputs: List[str], ms_delay: int=1):
        super().__init__(inputs, None)
        self.ms_delay = ms_delay

    def forward(self, frame_sample: DataSample):
        # Extract the frame
        frame = frame_sample.data

        cv2.imshow("Video", frame)
        cv2.waitKey(self.ms_delay)

class ProcessDrawText(Process):
    """Currently a dummy process to test"""

    def forward(self, csv_sample, video_sample):
        return video_sample.data

