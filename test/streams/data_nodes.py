# Build-in Imports
import time
import random

# Third-party Imports
import pyaudio
import numpy as np
import pandas as pd

# Internal Imports
import chimerapy.engine as cpe


class AudioNode(cpe.Node):
    def __init__(
        self,
        name: str,
        chunk: int = 1024,
        channels: int = 2,
        format: int = pyaudio.paInt16,
        rate: int = 44100,
        **kwargs,
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


class ImageNode(cpe.Node):
    def step(self):
        time.sleep(1 / 10)
        rand_frame = np.random.rand(20, 30, 3) * 255
        self.save_image(name="test", data=rand_frame)


class TabularNode(cpe.Node):
    def step(self):

        time.sleep(1 / 10)
        self.logger.debug(f"{self}: step: {self.state.fsm}")

        # Testing different types
        data = {"time": time.time(), "content": "HELLO"}
        self.save_tabular(name="test", data=data)

        data = pd.Series({"time": time.time(), "content": "GOODBYE"})
        self.save_tabular(name="test", data=data)

        data = pd.DataFrame({"time": [time.time()], "content": ["WAIT!"]})
        self.save_tabular(name="test", data=data)


class VideoNode(cpe.Node):
    def step(self):
        time.sleep(1 / 15)
        rand_frame = np.random.rand(720, 1280, 3) * 255
        self.save_video(name="test", data=rand_frame, fps=15)


class JSONNode(cpe.Node):
    def step(self):
        time.sleep(1 / 10)
        data = {"time": time.time(), "content": "HELLO"}
        self.save_json(name="test", data=data)


class TextNode(cpe.Node):
    def setup(self):
        self.step_count = 0

    def step(self):
        time.sleep(1 / 10)
        num_lines = random.randint(1, 5)
        self.step_count += 1
        lines = []
        for j in range(num_lines):
            lines.append(f"This is a test - Step Count - {self.step_count + 1}\n")

        self.save_text(name="test", data="".join(lines), suffix="text")
