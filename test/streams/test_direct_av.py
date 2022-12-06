# Built-in Imports
import logging
import pathlib
import os
import time
import pdb

# Third-party Import
import pytest
import wave
import pyaudio
import numpy as np
import ffmpeg
import cv2
import chimerapy as cp

logger = cp._logger.getLogger("chimerapy")

# Interal Testing imports
from ..conftest import not_github_actions

# Constants
CWD = pathlib.Path(os.path.abspath(__file__)).parent.parent
TEST_DATA_DIR = CWD / "data"
RECORD_SECONDS = 5

V_OUTPUT_FILE = TEST_DATA_DIR / "v.mp4"
A_OUTPUT_FILE = TEST_DATA_DIR / "a.wav"
AV_OUTPUT_FILE = TEST_DATA_DIR / "va.mp4"


class RandomGenerator:
    def read(self):
        return True, np.uint8(np.random.rand(200, 300, 3) * 255)

    def release(self):
        ...


@not_github_actions
def test_write_audio():
    # References:
    # https://python-ffmpegio.github.io/python-ffmpegio/quick.html#stream-read-write
    # https://people.csail.mit.edu/hubert/pyaudio/

    # Constants
    CHUNK = 1024
    FORMAT = pyaudio.paInt16
    CHANNELS = 2
    RATE = 44100

    # Audio
    p = pyaudio.PyAudio()

    stream = p.open(
        format=FORMAT, channels=CHANNELS, rate=RATE, input=True, frames_per_buffer=CHUNK
    )

    logger.info("* recording")

    frames = []

    for i in range(0, int(RATE / CHUNK * RECORD_SECONDS)):
        data = stream.read(CHUNK)
        frames.append(data)

    print("* done recording")

    stream.stop_stream()
    stream.close()
    p.terminate()

    wf = wave.open(str(A_OUTPUT_FILE), "wb")
    wf.setnchannels(CHANNELS)
    wf.setsampwidth(p.get_sample_size(FORMAT))
    wf.setframerate(RATE)
    wf.writeframes(b"".join(frames))
    wf.close()


@pytest.mark.order(after="test_write_audio")
@not_github_actions
def test_write_video():

    # Constants
    FPS = 15

    # Video
    try:
        cap = cv2.VideoCapture(0)
        ret, frame = cap.read()
        assert isinstance(frame, np.ndarray)
    except:
        cap = RandomGenerator()
        ret, frame = cap.read()

    h, w = frame.shape[:2]
    writer = cv2.VideoWriter(
        str(V_OUTPUT_FILE), cv2.VideoWriter_fourcc(*"MP4V"), FPS, (w, h)
    )

    tic = time.time()
    # prev_step = tic
    while True:
        ret, frame = cap.read()

        if ret:
            writer.write(frame)

        # Break when session complete
        toc = time.time()
        total_d = toc - tic
        if total_d >= RECORD_SECONDS:
            break

    cap.release()
    writer.release()


@pytest.mark.order(after="test_write_video")
@not_github_actions
def test_combine_video_and_audio():

    assert V_OUTPUT_FILE.exists()
    assert A_OUTPUT_FILE.exists()

    # Delete VA file
    if AV_OUTPUT_FILE.exists():
        os.remove(AV_OUTPUT_FILE)

    video = ffmpeg.input(str(V_OUTPUT_FILE))
    audio = ffmpeg.input(str(A_OUTPUT_FILE))
    ffmpeg.concat(video, audio, v=1, a=1).output(str(AV_OUTPUT_FILE)).run()
