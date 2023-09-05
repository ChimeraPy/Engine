# Built-in Imports
from typing import Dict, Any
import pathlib

# Third-party Imports
import pyaudio
import wave

# Internal Imports
from .record import Record


class AudioRecord(Record):
    def __init__(
        self,
        dir: pathlib.Path,
        name: str,
    ):
        super().__init__()

        # Storing input parameters
        self.dir = dir
        self.name = name
        self.first_frame = True
        self.audio_file_path = self.dir / f"{self.name}.wav"
        self.audio_writer = wave.open(str(self.audio_file_path), "wb")

    def write(self, data_chunk: Dict[str, Any]):

        # Only execute if it's the first time
        if self.first_frame:

            # Set recording hyperparameters
            if data_chunk.get("recorder_version") == 1:
                self.audio_writer.setnchannels(data_chunk["channels"])
                self.audio_writer.setsampwidth(
                    pyaudio.get_sample_size(data_chunk["format"])
                )
                self.audio_writer.setframerate(data_chunk["rate"])
            else:
                params = (
                    data_chunk["channels"],
                    data_chunk["sampwidth"],
                    data_chunk["framerate"],
                    data_chunk["nframes"],
                    "NONE",
                    "NONE",
                )

                self.audio_writer.setparams(params)

            # Avoid rewriting the parameters
            self.first_frame = False

        # Write
        recorder_version = data_chunk.get("recorder_version", 1)
        prepped_data = (
            data_chunk["data"].tobytes()
            if recorder_version == 1
            else data_chunk["data"]
        )
        self.audio_writer.writeframes(prepped_data)

    def close(self):

        # Close the audio writer
        self.audio_writer.close()
