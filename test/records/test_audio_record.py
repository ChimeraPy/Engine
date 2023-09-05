import pytest
import glob
import uuid
import wave
import tempfile
from pathlib import Path
from datetime import datetime

from chimerapy.engine.records import AudioRecord


def get_wav_files():
    data_dir = Path(__file__).parent / "data"
    return glob.glob(str(data_dir / "*.wav"))


@pytest.mark.parametrize("input_file", get_wav_files())
def test_audio_writer(input_file):
    save_dir = Path(tempfile.mkdtemp())
    with wave.open(input_file, "rb") as f:
        audio_writer = AudioRecord(
            dir=save_dir,
            name=Path(input_file).stem,
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
                "timestamp": datetime.now(),
            }

            audio_writer.write(datachunk)
        audio_writer.close()

    assert (save_dir / f"{Path(input_file).stem}.wav").exists()

    with wave.open(input_file, "rb") as inp, wave.open(
        str(save_dir / f"{Path(input_file).stem}.wav"), "rb"
    ) as out:
        assert inp.getparams() == out.getparams()
        assert inp.readframes(inp.getnframes()) == out.readframes(out.getnframes())
