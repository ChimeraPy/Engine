import asyncio
from typing import Dict, Any

import pathlib

import aiofiles
import zmq.asyncio

from chimerapy.engine.utils import get_progress_bar
import random

class ZMQFileClient:
    def __init__(self,
                 context: zmq.asyncio.Context,
                 host: str,
                 port: int,
                 outdir: pathlib.Path,
                 files: Dict[str, Any]):
        self.host = host
        self.port = port
        self.context = context
        self.outdir = outdir
        self.files = files
        self.socket = None

    async def async_init(self):
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.sndhwm = self.socket.rcvhwm = 1
        self.socket.connect(f"tcp://{self.host}:{self.port}")


    async def _download_task(self):
        handles = {}
        offsets = {}
        progress_bar = get_progress_bar()
        for name, details in self.files.items():
            fname = details["name"]
            handles[name] = await aiofiles.open(self.outdir / fname, "wb")
            offsets[name] = 0

        download_tasks = {}

        credit = 1
        chunk_size = 250000
        completed = []

        total = 0
        seq_no = 0

        progress_bar.start()
        while True:
            while credit:
                selected_file = random.choice(list(offsets.keys()))
                await self.socket.send_multipart(
                    [b"fetch", selected_file.encode(), b"%i" % offsets[selected_file], b"%i" % chunk_size, b"%i" % seq_no]
                )
                offsets[selected_file] = offsets[selected_file] + chunk_size
                seq_no += 1
                credit -= 1
            try:
                filename, chunk, seq_no_recv_str = await self.socket.recv_multipart()
                filename_str = filename.decode()

                if filename_str not in download_tasks:
                    download_tasks[filename_str] = progress_bar.add_task(f"Downloading({filename_str})", total=100)

                complete = (offsets[filename_str] / self.files[filename_str]["size"]) * 100
                progress_bar.update(download_tasks[filename_str], completed=complete)
                await handles[filename_str].write(chunk)
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
                progress_bar.update(download_tasks[filename_str], completed=100)
                del offsets[filename_str]

            if len(completed) == len(self.files):
                for name, handle in handles.items():
                    await handle.close()

                progress_bar.stop()
                break


async def main():
    context = zmq.asyncio.Context()
    host = "localhost"
    port = 6000

    outdir = pathlib.Path("dst")
    files = {
        "test1": {
            "name": "test1.mp4",
            "size": 31376170
        },
        "test2": {
            "name": "test2.mp4",
            "size": 36633129,
        },
        "test3": {
            "name": "test3.mp4",
            "size": 39156488
        },
        "test4": {
            "name": "test4.mp4",
            "size": 33941417
        }
    }

    client = ZMQFileClient(context, host, port, outdir, files)
    await client.async_init()
    await client._download_task()


if __name__ == "__main__":
    asyncio.run(main())
