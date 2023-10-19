import os
import time
import json
import hashlib

from chimerapy.orchestrator import sink_node
from chimerapy.engine.node import Node


@sink_node(name="FileCreator", add_to_registry=False)
class FileCreator(Node):
    """A utility class to upload files to the manager."""
    def __init__(self, filename, per_step_mb = 1, name="FileUploader"):
        super().__init__(name=name)
        self.per_step_mb = per_step_mb
        self.filename = filename
        self.file = None

    def step(self) -> None:
        if self.file is None:
            self.file = open(self.state.logdir / self.filename, "wb")

        self.file.write(os.urandom(self.per_step_mb * 1024 * 1024))
        time.sleep(1.0)
        self.logger.info(f"Written {self.per_step_mb}MB to {self.filename}")
        if self.state.fsm == "STOPPED":
            self.file.close()
            self.calculate_md5()

    def calculate_md5(self):
        md5 = hashlib.md5()
        with open(self.state.logdir / self.filename, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5.update(chunk)

        summary = {
            "filename": self.filename,
            "md5": md5.hexdigest()
        }

        with open(self.state.logdir / "summary.json", "w") as f:
            json.dump(summary, f, indent=2)

