import os


class ZMQFileChunk:
    """A ZeroMQ File Chunk."""

    def __init__(self, data: bytes) -> None:
        self.data = data

    def write_into(self, handle) -> None:
        handle.write(self.data)

    async def awrite_into(self, ahandle) -> None:
        await ahandle.write(self.data)

    @classmethod
    def from_bytes(cls, data) -> "ZMQFileChunk":
        return cls(data=data)

    @classmethod
    def read_from(cls, handle, offset, chunk_size) -> "ZMQFileChunk":
        handle.seek(offset, os.SEEK_SET)
        data = handle.read(chunk_size)
        return cls.from_bytes(data=data)

    @classmethod
    async def aread_from(cls, ahandle, offset, chunk_size) -> "ZMQFileChunk":
        await ahandle.seek(offset, os.SEEK_SET)
        data = await ahandle.read(chunk_size)
        return cls.from_bytes(data=data)
