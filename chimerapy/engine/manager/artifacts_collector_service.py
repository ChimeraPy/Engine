import logging
import pathlib
from typing import Any, Dict, Optional

import aioshutil
import zmq.asyncio

import chimerapy.engine.config as cpe_config
from chimerapy.engine._logger import fork, getLogger
from chimerapy.engine.networking.zmq_file_transfer_client import ZMQFileClient
from chimerapy.engine.utils import get_progress_bar

from ..eventbus import Event, EventBus, TypedObserver
from ..service import Service
from ..states import ManagerState
from .events import UpdateSendArchiveEvent


class ArtifactsCollectorService(Service):
    def __init__(
        self,
        name: str,
        eventbus: EventBus,
        state: ManagerState,
        parent_logger: Optional[logging.Logger] = None,
    ):
        super().__init__(name=name)
        self.eventbus = eventbus
        self.observers: Dict[str, TypedObserver] = {}
        self.clients: Dict[str, ZMQFileClient] = {}
        self.state = state
        self.progressbar = get_progress_bar()

        if parent_logger is None:
            parent_logger = getLogger("chimerapy-engine")

        self.logger = fork(parent_logger, self.__class__.__name__)

    async def async_init(self):
        self.observers = {
            "artifacts_transfer_ready": TypedObserver(
                "artifacts_transfer_ready", on_asend=self.collect, handle_event="pass"
            )
        }

        for name, observer in self.observers.items():  # noqa: B007
            await self.eventbus.asubscribe(observer)

    async def collect(self, event: Event) -> None:
        assert event.data is not None
        method = event.data["method"]
        if method == "zmq":
            self.logger.debug("Collecting artifacts over ZMQ")
            await self._collect_zmq(
                worker_id=event.data["worker_id"],
                host=event.data["ip"],
                port=event.data["port"],
                artifacts=event.data["data"],
            )
        else:
            self.logger.debug("Collecting artifacts locally")
            await self._collect_local(
                worker_id=event.data["worker_id"], artifacts=event.data["data"]
            )

    async def _collect_zmq(
        self, worker_id: str, host: str, port: int, artifacts: Dict[str, Any]
    ):
        files = {}
        self.logger.debug("Preparing files to download")
        for node_id, artifact_details in artifacts.items():
            out_dir = self._create_node_dir(worker_id, node_id)
            for artifact in artifact_details:
                key = f"{node_id}-{artifact['name']}"
                files[key] = {
                    "name": artifact["filename"],
                    "size": artifact["size"],
                    "outdir": out_dir,
                }
        context = zmq.asyncio.Context.instance()
        client = ZMQFileClient(
            context=context,
            host=host,
            port=port,
            credit=cpe_config.get("file-transfer.max-chunks"),
            chunk_size=cpe_config.get("file-transfer.chunk-size"),
            files=files,
            parent_logger=self.logger,
            progressbar=self.progressbar,
        )
        self.clients[worker_id] = client
        try:
            await client.async_init()
            await client.download_files()
            event_data = UpdateSendArchiveEvent(worker_id=worker_id, success=True)
        except Exception as e:
            event_data = UpdateSendArchiveEvent(
                worker_id=worker_id,
                success=False,
            )
            self.logger.error(
                f"Error while collecting artifacts for worker {worker_id}: {e}"
            )
        finally:
            await self.eventbus.asend(Event("update_send_archive", event_data))
            self.logger.info(f"Successfully collected artifacts for worker {worker_id}")

    async def _collect_local(self, worker_id: str, artifacts: Dict[str, Any]) -> None:
        try:
            for node_id, node_artifacts in artifacts.items():
                node_dir = self._create_node_dir(worker_id, node_id)

                for artifact in node_artifacts:
                    artifact_path = pathlib.Path(artifact["path"])
                    self.logger.debug(f"Copying {artifact_path} to {node_dir}")
                    await aioshutil.copyfile(
                        artifact_path, node_dir / artifact["filename"]
                    )

            await self.eventbus.asend(
                Event(
                    "update_send_archive",
                    UpdateSendArchiveEvent(worker_id=worker_id, success=True),
                )
            )
            event_data = UpdateSendArchiveEvent(
                worker_id=worker_id,
                success=True,
            )
            self.logger.info(f"Successfully collected artifacts for worker {worker_id}")
        except Exception as e:
            event_data = UpdateSendArchiveEvent(
                worker_id=worker_id,
                success=False,
            )
            self.logger.error(
                f"Error while collecting artifacts for worker {worker_id}: {e}"
            )
        finally:
            await self.eventbus.asend(Event("update_send_archive", event_data))

    def _create_worker_dir(self, worker_id):
        worker_name = self.state.workers[worker_id].name
        worker_dir = self.state.logdir / f"{worker_name}-{worker_id[:10]}"
        worker_dir.mkdir(parents=True, exist_ok=True)

        return worker_dir

    def _create_node_dir(self, worker_id, node_id):
        worker_dir = self._create_worker_dir(worker_id)
        nodes = self.state.workers[worker_id].nodes
        node_dir = worker_dir / nodes[node_id].name
        node_dir.mkdir(parents=True, exist_ok=True)

        return node_dir
