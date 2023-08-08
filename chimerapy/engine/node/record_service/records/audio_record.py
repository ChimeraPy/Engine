# Built-in Imports
import pathlib
import datetime

# Third-party Imports
import pyaudio
import wave

# Internal Imports
from .record import Record
from ..entry import AudioEntry


class AudioRecord(Record):
    def __init__(self, dir: pathlib.Path, name: str, start_time: datetime.datetime):
        super().__init__(dir=dir, name=name, start_time=start_time)

        # Storing input parameters
        self.first_frame = True
        self.audio_file_path = self.dir / f"{self.name}.wav"
        self.audio_writer = wave.open(str(self.audio_file_path), "wb")

    def write(self, entry: AudioEntry):

        # Only execute if it's the first time
        if self.first_frame:

            # Set recording hyperparameters
            self.audio_writer.setnchannels(entry.channels)
            self.audio_writer.setsampwidth(pyaudio.get_sample_size(entry.format))
            self.audio_writer.setframerate(entry.rate)

            # Avoid rewriting the parameters
            self.first_frame = False

        # Write
        prepped_data = entry.data.tobytes()
        self.audio_writer.writeframes(prepped_data)

    def close(self):

        # Close the audio writer
        self.audio_writer.close()
