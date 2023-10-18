import asyncio
import json
import logging
import pathlib
from typing import Any, Dict, Optional

import aiofiles
import aiohttp
import aioshutil
from aiohttp import ClientSession
from tqdm import tqdm

from chimerapy.engine import config
from chimerapy.engine._logger import fork, getLogger
from chimerapy.engine.states import ManagerState, NodeState
from chimerapy.engine.utils import async_waiting_for
import zmq
from zmq.asyncio import Context
from rich.progress import Progress

PIPELINE = 1

CHUNK_SIZE = 250000


async def client_task(context, ip, filename: pathlib.Path, expected_size):
    dealer = context.socket(zmq.DEALER)
    dealer.sndhwm = dealer.rcvhwm = PIPELINE
    dealer.connect(f"tcp://{ip}:6000")

    f = open(filename, "wb")
    credit = PIPELINE

    total = 0
    chunks = 0
    offset = 0
    seq_no = 0

    # Create a progress bar
    human_size = round(expected_size / 1024 / 1024, 2)

    with Progress() as progress:
        task = progress.add_task(f"[cyan]Downloading ({filename.name}-{human_size}MB...)", total=100)
        while True:
            while credit:
                await dealer.send_multipart([b"fetch", b"%i" % offset, b"%i" % CHUNK_SIZE, b"%i" % seq_no])
                offset += CHUNK_SIZE
                seq_no += 1
                credit -= 1

            try:
                chunk, seq_no_recv_str = await dealer.recv_multipart()
                seq_no_recv = int(seq_no_recv_str)
                f.write(chunk)
            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM:
                    return
                else:
                    raise

            chunks += 1
            credit += 1
            size = len(chunk)
            total += size
            # print(f"Receiving {total} {offset} {seq_no_recv} {size}")
            progress.update(task, completed=(total / expected_size) * 100)
            if size < CHUNK_SIZE:
                f.close()
                break



class ArtifactsCollector:
    """A utility class to collect artifacts recorded by the nodes."""

    def __init__(
        self,
        state: ManagerState,
        worker_id: str,
        parent_logger: Optional[logging.Logger] = None,
        unzip: bool = False,
    ):
        worker_state = state.workers[worker_id]
        if parent_logger is None:
            parent_logger = getLogger("chimerapy-engine")

        self.logger = fork(
            parent_logger,
            f"ArtifactsCollector-[Worker({worker_state.name})]",
        )

        self.state = state
        self.worker_id = worker_id
        self.base_url = (
            f"http://{self.state.workers[self.worker_id].ip}:"
            f"{self.state.workers[self.worker_id].port}"
        )
        self.unzip = unzip

    async def _artifact_info(self, session: aiohttp.ClientSession):
        self.logger.info(f"Requesting artifact info from {self.base_url}")
        async with session.post("/nodes/request_collect") as resp:
            if resp.ok:
                data = await resp.json()
                return data["zip_path"], data["port"], data["size"]
            else:
                raise Exception(f"Request failed: {resp.status} {resp.reason}")

    async def collect(self):
        client_session = ClientSession(base_url=self.base_url)
        zip_path, port, size = await self._artifact_info(client_session)
        print(pathlib.Path(zip_path).name, self.state.logdir)
        save_path = self.state.logdir / pathlib.Path(zip_path).name
        self.logger.info(f"Downloading {zip_path} from {self.base_url}")
        await client_task(Context(), self.state.workers[self.worker_id].ip, save_path, size)

