import aiofiles
import zmq
from zmq.asyncio import Context

import chimerapy.engine.config as cpe_config


def set_socket_hwm(socket: zmq.Socket, hwm: int):
    socket.sndhwm = hwm
    socket.rcvhwm = hwm


class FileTransferClient:
    def __init__(self, ip: str, port: int, ctx: Context, dest_name: str):
        self.url = f"tcp://{ip}:{port}"
        self.ctx = ctx
        self.filename = dest_name

    async def recv(self):
        socket = self.ctx.socket(zmq.DEALER)
        set_socket_hwm(socket, cpe_config.get("comms.file-transfer.max-chunks"))
        socket.connect(self.url)
        chunk_size = cpe_config.get("comms.file-transfer.chunk-size")  # Replace with your actual chunk size
        credit = cpe_config.get("comms.file-transfer.max-chunks")  # Replace with your actual credit

        file = await aiofiles.open(self.filename, mode="wb")

        total = 0
        chunks = 0
        offset = 0

        while True:
            while credit:
                await socket.send_multipart([b"fetch", b"%i" % offset, b"%i" % chunk_size])
                credit -= 1
                offset += chunk_size

            try:
                chunk = await socket.recv()
                if chunk == b"EOF":
                    await file.close()
                    break
            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM:
                    break
                else:
                    raise e

            await file.seek(offset - chunk_size)  # move file pointer back
            await file.write(chunk)

            credit += 1
            chunks += 1
            total += len(chunk)
            print(f"Received {total} bytes in {chunks} chunks.")


if __name__ == "__main__":
    import asyncio
    import sys

    ip = sys.argv[1]
    port = int(sys.argv[2])
    filename = sys.argv[3]

    ctx = Context.instance()
    client = FileTransferClient(ip, port, ctx, filename)
    asyncio.run(client.recv())
