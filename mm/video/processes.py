# Subpackage management
__package__ = 'video'

import cv2

from mm.process import Process

class ProcessShowVideo(Process):

    def forward(self, frame):
        cv2.imshow(self.name, frame)
        cv2.waitKey(0)

class ProcessDrawText(Process):
    """Currently a dummy process to test"""

    def forward(self, csv_sample, video_sample):
        return video_sample.data

