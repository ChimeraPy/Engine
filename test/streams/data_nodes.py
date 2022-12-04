# Build-in Imports
import time

# Third-party Imports
import pyaudio
import numpy as np
import pandas as pd

# Internal Imports
import chimerapy as cp


class AudioNode(cp.Node):
    def __init__(
        self,
        name: str,
        chunk: int = 1024,
        channels: int = 2,
        format: int = pyaudio.paInt16,
        rate: int = 44100,
        **kwargs
    ):
        super().__init__(name, **kwargs)
        self.chunk = chunk
        self.channels = channels
        self.format = format
        self.rate = rate

    def step(self):

        time.sleep(1 / 20)
        data = np.random.rand(self.chunk) * 2 - 1
        self.save_audio(
            name="test",
            data=data,
            channels=self.channels,
            format=self.format,
            rate=self.rate,
        )


class ImageNode(cp.Node):
    def step(self):
        time.sleep(1 / 10)
        rand_frame = np.random.rand(20, 30, 3) * 255
        self.save_image(name="test", data=rand_frame)


class TabularNode(cp.Node):
    def step(self):

        time.sleep(1 / 10)

        # Testing different types
        data = {"time": time.time(), "content": "HELLO"}
        self.save_tabular(name="test", data=data)

        data = pd.Series({"time": time.time(), "content": "GOODBYE"})
        self.save_tabular(name="test", data=data)

        data = pd.DataFrame({"time": [time.time()], "content": ["WAIT!"]})
        self.save_tabular(name="test", data=data)


class VideoNode(cp.Node):
    def step(self):
        time.sleep(1 / 10)
        rand_frame = np.random.rand(20, 30, 3) * 255
        self.save_video(name="test", data=rand_frame, fps=30)
