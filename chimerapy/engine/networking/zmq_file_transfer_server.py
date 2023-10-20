import asyncio
import logging
import os
import pathlib
from typing import Optional

import aiofiles
import zmq
import zmq.asyncio

from chimerapy.engine._logger import fork, getLogger
from chimerapy.engine.networking.utils import ZMQFileChunk
from chimerapy.engine.utils import get_progress_bar


class ZMQFileServer:
    def __init__(
        self,
        context: zmq.asyncio.Context,
        paths,
        credit: int,
        host: str = "*",
        port: int = 0,
        parent_logger: Optional[logging.Logger] = None,
    ) -> None:
        self.context = context
        self.host = host
        self.port = port
        self.paths = paths
        self.handles = {}
        self.credits = credit
        self.socket: Optional[zmq.Socket] = None
        self.send_task: Optional[asyncio.Task] = None

        if parent_logger is None:
            parent_logger = getLogger("chimerapy-engine-networking")

        self.logger = fork(parent_logger, "ZMQFileServer")

    async def async_init(self) -> None:
        """Initialize the server."""
        await self._initialize_file_handlers()
        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.sndhwm = self.socket.rcvhwm = self.credits

        if self.port != 0:
            self.socket.bind(f"tcp://{self.host}:{self.port}")
        else:
            self.port = self.socket.bind_to_random_port(f"tcp://{self.host}")

        self.logger.info(f"Listening on tcp://{self.host}:{self.port}")
        self.send_task = asyncio.create_task(self._send())
        self.progress_bar = None

    async def _initialize_file_handlers(self) -> None:
        """Initialize the file handlers."""
        for name, file in self.paths.items():
            assert file.exists()
            assert file.is_file()
            self.handles[name] = await aiofiles.open(file, mode="rb")

    async def _send(self):
        """Send the file chunks."""
        assert self.socket is not None
        router = self.socket
        self.progress_bar = get_progress_bar()
        progress_bar = self.progress_bar
        progress_bar.start()
        upload_tasks = {}

        for name, file in self.paths.items():
            upload_tasks[name] = progress_bar.add_task(f"Uploading({name})", total=100)
        uploaded = set()
        while True:
            try:
                msg = await router.recv_multipart()
            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM:
                    return
                else:
                    raise e

            identity, command, fname, offset_str, chunksz_str, seq_nostr = msg
            fname_str = fname.decode()

            if fname_str in self.handles:
                assert command == b"fetch"
                offset = int(offset_str)
                chunksz = int(chunksz_str)
                seq_no = int(seq_nostr)
                handle = self.handles[fname_str]
                zmq_chunk = await ZMQFileChunk.aread_from(
                    ahandle=handle, offset=offset, chunk_size=chunksz
                )

                data = zmq_chunk.data
                if not data:
                    progress_bar.update(upload_tasks[fname_str], completed=100)
                    uploaded.add(fname_str)

                    if uploaded == set(self.paths.keys()):
                        progress_bar.stop()
                        break
                    continue
                else:
                    await router.send_multipart([identity, fname, data, b"%i" % seq_no])
                    completed = (
                        await handle.tell() / os.path.getsize(self.paths[fname_str])
                    ) * 100
                    progress_bar.update(upload_tasks[fname_str], completed=completed)

    async def ashutdown(self):
        """Shutdown the server."""
        if self.progress_bar is not None:
            self.progress_bar.stop()

        if self.send_task is not None:
            await self.send_task
            self.send_task = None

        if self.socket is not None:
            self.socket.close()
            self.socket = None

        for handle in self.handles.values():
            await handle.close()

        self.handles = {}


async def main():
    files = {
        "test1": pathlib.Path("src/test1.mp4"),
        "test2": pathlib.Path("src/test2.mp4"),
        "test3": pathlib.Path("src/test3.mp4"),
        "test4": pathlib.Path("src/test4.mp4"),
        "oele-11-webcam-video": pathlib.Path("src/oele-11-webcam-video.mp4"),
    }
    server = ZMQFileServer(
        context=zmq.asyncio.Context(), paths=files, port=6000, credit=1
    )
    await server.async_init()
    await server.send_task


if __name__ == "__main__":
    asyncio.run(main())
