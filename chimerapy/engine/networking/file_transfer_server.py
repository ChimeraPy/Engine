import asyncio
import os
import pathlib
import aiofiles
from chimerapy.engine.utils import get_progress_bar
import zmq
from typing import Optional, List, Dict, Any
import zmq.asyncio


class ZMQFileChunk:
    def __init__(self, data: bytes):
        self.data = data

    def write_into(self, handle):
        handle.write(self.data)

    async def awrite_into(self, ahandle):
        await ahandle.write(self.data)

    @classmethod
    def from_bytes(cls, data):
        return ZMQFileChunk(data=data)

    @classmethod
    def read_from(cls, handle, offset, chunk_size):
        handle.seek(offset, os.SEEK_SET)
        data = handle.read(chunk_size)
        return cls.from_bytes(data=data)

    @classmethod
    async def aread_from(cls, ahandle, offset, chunk_size):
        await ahandle.seek(offset, os.SEEK_SET)
        data = await ahandle.read(chunk_size)
        return cls.from_bytes(data=data)


class ZMQFileServer:
    def __init__(self, context: zmq.asyncio.Context, paths, host="*", port=0):
        self.context = context
        self.host = host
        self.port = port
        self.paths = paths
        self.handles = {}
        self.socket: Optional[zmq.Socket] = None
        self.send_task: Optional[asyncio.Task] = None

    async def async_init(self):
        await self._initialize_file_handlers()
        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.sndhwm = self.socket.rcvhwm = 1
        if self.port != 0:
            self.socket.bind(f"tcp://{self.host}:{self.port}")
        else:
            self.port = self.socket.bind_to_random_port(f"tcp://{self.host}")

        self.send_task = asyncio.create_task(self._send())

    async def _initialize_file_handlers(self):
        for name, file in self.paths.items():
            assert file.exists()
            assert file.is_file()
            self.handles[name] = await aiofiles.open(file, mode="rb")

    async def _send(self):
        assert self.socket is not None
        router = self.socket
        progress_bar = get_progress_bar()
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
                    raise

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
                    print(f"Uploaded {fname_str}. {uploaded}")
                    if uploaded == set(self.paths.keys()):
                        progress_bar.stop()
                        break
                    continue
                else:
                    await router.send_multipart([identity, fname, data, b"%i" % seq_no])
                    completed = (await handle.tell() / os.path.getsize(self.paths[fname_str])) * 100
                    progress_bar.update(upload_tasks[fname_str], completed=completed)


    async def ashutdown(self):
        if self.send_task is not None:
            self.send_task.cancel()
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
        "test4": pathlib.Path("src/test4.mp4")
    }
    server = ZMQFileServer(context=zmq.asyncio.Context(), paths=files, port=6000)
    await server.async_init()
    await server.send_task

if __name__ == "__main__":
    asyncio.run(main())


