import asyncio
import logging
import pathlib
import random
from typing import Any, Dict, Optional

import aiofiles
import zmq.asyncio
from rich.progress import Progress

from chimerapy.engine._logger import fork, getLogger
from chimerapy.engine.networking.utils import ZMQFileChunk


class ZMQFileClient:
    def __init__(
        self,
        context: zmq.asyncio.Context,
        host: str,
        port: int,
        credit: int,
        chunk_size: int,
        files: Dict[str, Any],
        parent_logger: Optional[logging.Logger] = None,
        progressbar: Optional[Progress] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.context = context
        self.files = files
        self.credits = credit
        self.chunk_size = chunk_size
        self.progressbar = progressbar
        self.socket = None
        if parent_logger is None:
            parent_logger = getLogger("chimerapy-engine")
        self.logger = fork(parent_logger, "ZMQFileClient")
        print(self.host, self.port)

    async def async_init(self):
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.sndhwm = self.socket.rcvhwm = 1
        self.socket.connect(f"tcp://{self.host}:{self.port}")
        self.logger.info(f"Connected to tcp://{self.host}:{self.port}")

    async def download_files(self):
        handles = {}
        offsets = {}
        progressbar = self.progressbar
        for name, details in self.files.items():
            fname = details["name"]
            outdir = details["outdir"]
            handles[name] = await aiofiles.open(outdir / fname, "wb")
            offsets[name] = 0

        download_tasks = {}

        credit = self.credits
        chunk_size = self.chunk_size
        completed = []

        total = 0
        seq_no = 0

        progressbar.start()
        while True:
            while credit:
                selected_file = random.choice(list(offsets.keys()))
                await self.socket.send_multipart(
                    [
                        b"fetch",
                        selected_file.encode(),
                        b"%i" % offsets[selected_file],
                        b"%i" % chunk_size,
                        b"%i" % seq_no,
                    ]
                )
                offsets[selected_file] = offsets[selected_file] + chunk_size
                seq_no += 1
                credit -= 1
            try:
                filename, chunk, seq_no_recv_str = await self.socket.recv_multipart()
                filename_str = filename.decode()
                zmq_file_chunk = ZMQFileChunk.from_bytes(chunk)

                if filename_str not in download_tasks and progressbar is not None:
                    download_tasks[filename_str] = progressbar.add_task(
                        f"Downloading({filename_str})", total=100
                    )

                complete = (
                    offsets[filename_str] / self.files[filename_str]["size"]
                ) * 100

                if progressbar is not None:
                    progressbar.update(download_tasks[filename_str], completed=complete)

                await zmq_file_chunk.awrite_into(ahandle=handles[filename_str])

            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM:
                    return
                else:
                    raise e

            credit += 1
            size = len(chunk)
            total += size
            if size < chunk_size:
                completed.append(filename_str)
                progressbar.update(download_tasks[filename_str], completed=100)
                del offsets[filename_str]

            if len(completed) == len(self.files):
                for name, handle in handles.items():
                    await handle.close()
                progressbar.stop()
                break

    async def close(self):
        await self.socket.close()


async def main():
    context = zmq.asyncio.Context()
    host = "localhost"
    port = 6000

    files = {
        "test1": {"name": "test1.mp4", "size": 31376170, "outdir": pathlib.Path("dst")},
        "test2": {"name": "test2.mp4", "size": 36633129, "outdir": pathlib.Path("dst")},
        "test3": {"name": "test3.mp4", "size": 39156488, "outdir": pathlib.Path("dst")},
        "test4": {"name": "test4.mp4", "size": 33941417, "outdir": pathlib.Path("dst")},
        "oele-11-webcam-video": {
            "name": "oele-11-webcam-video.mp4",
            "size": 351384728,
            "outdir": pathlib.Path("dst"),
        },
    }
    from chimerapy.engine.utils import get_progress_bar

    client = ZMQFileClient(
        context,
        host,
        port,
        credit=1,
        chunk_size=250000,
        files=files,
        progressbar=get_progress_bar(),
    )
    await client.async_init()
    await client.download_files()


if __name__ == "__main__":
    asyncio.run(main())
