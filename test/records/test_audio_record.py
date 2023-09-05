import pytest
import glob
import wave
from pathlib import Path


def get_wav_files():
    data_dir = Path(__file__).parent / "data"
    return glob.glob(str(data_dir / "*.wav"))


@pytest.mark.parametrize("input_file", get_wav_files())
def test_audio_writer(input_file):
    print(input_file)
    # assert False
    with wave.open(input_file, "rb") as f:
        print(f.getparams())
        print(f.getnframes())
        print(f.getsampwidth())
        # while True:
        #     data = f.readframes(f.getnframes())
        #     if not data:
        #         break
