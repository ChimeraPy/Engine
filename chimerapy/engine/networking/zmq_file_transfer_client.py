import logging
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

    async def async_init(self):
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.sndhwm = self.socket.rcvhwm = 1
        self.socket.connect(f"tcp://{self.host}:{self.port}")
        self.logger.info(f"Connected to tcp://{self.host}:{self.port}")

    async def download_files(self):  # noqa: C901
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
                filekey, chunk, seq_no_recv_str = await self.socket.recv_multipart()
                filekey_str = filekey.decode()
                zmq_file_chunk = ZMQFileChunk.from_bytes(chunk)

                if filekey_str not in download_tasks and progressbar is not None:
                    human_size = (
                        f"{round(self.files[filekey_str]['size'] / 1024 / 1024, 2)} MB"
                    )
                    fname = self.files[filekey_str]["name"]
                    download_tasks[filekey_str] = progressbar.add_task(
                        f"Downloading({fname}|{human_size})", total=100
                    )

                complete = (
                    offsets[filekey_str] / self.files[filekey_str]["size"]
                ) * 100

                if progressbar is not None:
                    progressbar.update(download_tasks[filekey_str], completed=complete)

                await zmq_file_chunk.awrite_into(ahandle=handles[filekey_str])

            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM:
                    return
                else:
                    raise e

            credit += 1
            size = len(chunk)
            total += size
            if size < chunk_size:
                completed.append(filekey_str)
                progressbar.update(download_tasks[filekey_str], completed=100)
                del offsets[filekey_str]

            if len(completed) == len(self.files):
                for name, handle in handles.items():  # noqa: B007
                    await handle.close()
                progressbar.stop()
                break

    async def close(self):
        await self.socket.close()
