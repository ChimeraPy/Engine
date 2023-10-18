import json
import logging
import os
import pathlib
from typing import Optional

import aiofiles
import aiohttp
import aioshutil
import zmq
from aiohttp import ClientSession
from rich.progress import Progress
from zmq.asyncio import Context

from chimerapy.engine import config
from chimerapy.engine._logger import fork, getLogger
from chimerapy.engine.states import ManagerState
from chimerapy.engine.utils import get_ip_address


async def download_task(
    context: zmq.Context,
    ip: str,
    port: int,
    filename: pathlib.Path,
    expected_size,
    progress=None,
):
    """Download a file from a worker."""
    dealer = context.socket(zmq.DEALER)
    dealer.sndhwm = dealer.rcvhwm = config.get("file-transfer.max-chunks")
    dealer.connect(f"tcp://{ip}:{port}")

    f = await aiofiles.open(filename, "wb")
    credit = config.get("file-transfer.max-chunks")
    chunk_size = config.get("file-transfer.chunk-size")

    total = 0
    chunks = 0
    offset = 0
    seq_no = 0

    # Create a progress bar
    human_size = round(expected_size / 1024 / 1024, 2)
    update_task = None
    if progress:
        update_task = progress.add_task(
            f"[cyan]Downloading ({filename.name}-{human_size}MB...)", total=100
        )

    while True:
        while credit:
            await dealer.send_multipart(
                [b"fetch", b"%i" % offset, b"%i" % chunk_size, b"%i" % seq_no]
            )
            offset += chunk_size
            seq_no += 1
            credit -= 1

        try:
            chunk, seq_no_recv_str = await dealer.recv_multipart()
            await f.write(chunk)
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                return
            else:
                raise

        chunks += 1
        credit += 1
        size = len(chunk)
        total += size

        if update_task:
            progress.update(update_task, completed=(total / expected_size) * 100)

        if size < chunk_size:
            await f.close()
            break


class ArtifactsCollector:
    """A utility class to collect artifacts recorded by the nodes."""

    def __init__(
        self,
        state: ManagerState,
        worker_id: str,
        parent_logger: Optional[logging.Logger] = None,
        unzip: bool = False,
        progressbar: Optional[Progress] = None,
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
        self.progressbar = progressbar

    async def _artifact_info(self, session: aiohttp.ClientSession):
        self.logger.info(f"Requesting artifact info from {self.base_url}")
        data = {
            "initiate_remote_transfer": get_ip_address()
            != self.state.workers[self.worker_id].ip
        }

        async with session.post(
            "/nodes/request_collect",
            data=json.dumps(data),
        ) as resp:
            if resp.ok:
                data = await resp.json()
                return data["zip_path"], data["port"], data["size"]
            else:
                # FixMe: Handle this error properly
                raise ConnectionError(
                    f"Artifacts Collection Failed: {resp.status} {resp.reason}"
                )

    async def collect(self) -> bool:
        client_session = ClientSession(base_url=self.base_url)
        try:
            zip_path, port, size = await self._artifact_info(client_session)
        except Exception as e:
            self.logger.error(f"Failed to get artifact info: {e}")
            return False
        save_name = f"{self.state.workers[self.worker_id].name}_{self.worker_id[:8]}"
        zip_save_path = self.state.logdir / f"{save_name}.zip"
        if port is not None:
            try:
                await download_task(
                    Context(),
                    self.state.workers[self.worker_id].ip,
                    int(port),
                    zip_save_path,
                    size,
                    self.progressbar,
                )
            except Exception as e:
                self.logger.error(f"Failed to download artifacts: {e}")
                return False
        else:
            self.logger.info(f"Copying {zip_path} to {zip_save_path}")
            try:
                await aioshutil.copyfile(zip_path, self.state.logdir / zip_save_path)
            except Exception as e:
                self.logger.error(f"Failed to copy artifacts: {e}")
                return False

        if self.unzip:
            self.logger.info(f"Unzipping {zip_save_path}")
            try:
                await aioshutil.unpack_archive(
                    zip_save_path, self.state.logdir / save_name
                )
                self.logger.info(f"Removing {zip_save_path}")
                os.remove(zip_save_path)
            except Exception as e:
                self.logger.error(f"Failed to unzip artifacts: {e}")
                return False

        return True
