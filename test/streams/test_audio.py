# Built-in Imports
import asyncio
import datetime
import glob
import os
import pathlib
import tempfile
import uuid
import wave

# Third-party
import numpy as np
import pyaudio
import pytest

# Internal Imports
import chimerapy.engine as cpe
from chimerapy.engine.eventbus import Event, EventBus
from chimerapy.engine.records.audio_record import AudioRecord

from .data_nodes import AudioNode

logger = cpe._logger.getLogger("chimerapy-engine")

# Constants
CWD = pathlib.Path(os.path.abspath(__file__)).parent.parent
TEST_DATA_DIR = CWD / "data"
CHUNK = 1024
FORMAT = pyaudio.paInt16
CHANNELS = 2
RATE = 44100
RECORD_SECONDS = 2


@pytest.fixture
def audio_node():
    # Create a node
    an = AudioNode("an", CHUNK, CHANNELS, FORMAT, RATE, logdir=TEST_DATA_DIR)
    return an


def get_wav_files():
    data_dir = TEST_DATA_DIR / "audio"
    return glob.glob(str(data_dir / "*.wav"))


@pytest.mark.skip(reason="Test taking way to long?")
@pytest.mark.parametrize("input_file", get_wav_files())
def test_audio_writer(input_file):
    save_dir = pathlib.Path(tempfile.mkdtemp())
    with wave.open(input_file, "rb") as f:
        audio_writer = AudioRecord(
            dir=save_dir,
            name=pathlib.Path(input_file).stem,
        )
        for j in range(f.getnframes()):
            frame = f.readframes(1)
            datachunk = {
                "uuid": uuid.uuid4(),
                "name": "pvrecorder-test",
                "data": frame,
                "dtype": "audio",
                "channels": f.getnchannels(),
                "sampwidth": f.getsampwidth(),
                "framerate": f.getframerate(),
                "nframes": f.getnframes(),
                "recorder_version": 2,
                "timestamp": datetime.datetime.now(),
            }

            audio_writer.write(datachunk)
        audio_writer.close()

    assert (save_dir / f"{pathlib.Path(input_file).stem}.wav").exists()

    with wave.open(input_file, "rb") as inp, wave.open(
        str(save_dir / f"{pathlib.Path(input_file).stem}.wav"), "rb"
    ) as out:
        assert inp.getparams() == out.getparams()
        assert inp.readframes(inp.getnframes()) == out.readframes(out.getnframes())


def test_audio_record():

    # Check that the audio was created
    expected_audio_path = TEST_DATA_DIR / "test.wav"
    try:
        os.remove(expected_audio_path)
    except FileNotFoundError:
        ...

    # Create the record
    ar = AudioRecord(dir=TEST_DATA_DIR, name="test")

    # Write to audio file
    for i in range(0, int(RATE / CHUNK * RECORD_SECONDS)):
        data = (np.random.rand(CHUNK) * 2 - 1) * (i * 0.1)
        audio_chunk = {
            "uuid": uuid.uuid4(),
            "name": "test",
            "data": data,
            "dtype": "audio",
            "channels": CHANNELS,
            "format": FORMAT,
            "rate": RATE,
            "recorder_version": 1,
            "timestamp": datetime.datetime.now(),
        }
        ar.write(audio_chunk)

    assert expected_audio_path.exists()


async def test_node_save_audio_stream(audio_node):

    # Event Loop
    eventbus = EventBus()

    # Check that the audio was created
    expected_audio_path = pathlib.Path(audio_node.state.logdir) / "test.wav"
    # try:
    #     os.remove(expected_audio_path)
    # except FileNotFoundError:
    #     ...

    # Stream
    await audio_node.arun(eventbus=eventbus)

    # Wait to generate files
    await eventbus.asend(Event("start"))
    logger.debug("Finish start")
    await eventbus.asend(Event("record"))
    logger.debug("Finish record")
    await asyncio.sleep(3)
    await eventbus.asend(Event("stop"))
    logger.debug("Finish stop")

    await audio_node.ashutdown()

    # Check that the audio was created
    assert expected_audio_path.exists()
