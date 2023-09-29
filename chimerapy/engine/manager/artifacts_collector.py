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


class ArtifactsCollector:
    """A utility class to collect artifacts recorded by the nodes."""

    def __init__(
        self,
        state: ManagerState,
        worker_id: str,
        parent_logger: Optional[logging.Logger] = None,
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

    async def _request_artifacts_gather(self, session: ClientSession) -> None:
        """Request the nodes to gather recorded artifacts."""
        self.logger.debug("Requesting nodes to gather recorded artifacts")
        async with session.post(
            url="/nodes/gather_artifacts", data=json.dumps({})
        ) as _:
            ...

    async def _wait_till_artifacts_ready(self, timeout: int) -> None:
        self.logger.debug("Waiting for nodes to gather recorded artifacts")
        success = await async_waiting_for(self._have_nodes_saved, timeout=timeout)

        if not success:
            e_msg = "Nodes did not gather recorded artifacts in time"
            self.logger.error(e_msg)
            raise TimeoutError(e_msg)

        self.logger.info("Nodes gathered recorded artifacts")

    async def _request_artifacts_info(self, session) -> Dict[str, Any]:
        """Request the nodes to send the artifacts info."""
        self.logger.debug("Requesting nodes to send artifacts info")
        async with session.get(
            url="/nodes/artifacts",
        ) as resp:
            if resp.status != 200:
                e_msg = "Could not get artifacts info from nodes"
                self.logger.error(e_msg)
                artifacts = {}
            else:
                artifacts = await resp.json()

            return artifacts

    def _have_nodes_saved(self) -> bool:
        """Check if all nodes have saved the recorded artifacts."""
        worker_state = self.state.workers[self.worker_id]
        node_fsm = map(lambda node: node.fsm, worker_state.nodes.values())

        return all(map(lambda fsm: fsm == "SAVED", node_fsm))

    async def _download_artifacts(self, session, artifacts) -> bool:
        """Download the artifacts from the nodes."""
        parent_path = self._create_worker_dir()
        coros = []
        for node_id, node_artifacts in artifacts.items():
            node_state = self._find_node_state_by_id(node_id)
            node_dir = parent_path / node_state.name
            node_dir.mkdir(exist_ok=True, parents=True)
            for artifact in node_artifacts:
                if self._is_remote_worker_collector():
                    coros.append(
                        self._download_remote_artifact(
                            session, node_id, node_dir, artifact
                        )
                    )
                else:
                    coros.append(self._download_local_artifact(node_dir, artifact))

        results = await asyncio.gather(*coros)
        return all(results)

    def _is_remote_worker_collector(self) -> bool:
        """Check if the worker collector is remote."""
        return self.state.workers[self.worker_id].ip != self.state.ip

    async def _download_local_artifact(
        self, parent_dir: pathlib.Path, artifact: Dict[str, Any]
    ) -> bool:
        """Download a single artifact recorded by a node."""
        dst_path = parent_dir / artifact["filename"]
        src_path = pathlib.Path(artifact["path"])

        if not src_path.exists():
            return False

        self.logger.debug(f"Copying {src_path.name} to {dst_path}")
        await aioshutil.copyfile(src_path, dst_path)
        return True

    async def _download_remote_artifact(
        self,
        session: ClientSession,
        node_id: str,
        parent_dir: pathlib.Path,
        artifact: Dict[str, Any],
    ) -> bool:
        """Download a single artifact from a node."""
        dst_path = parent_dir / artifact["filename"]
        # Stream and Save
        async with session.get(
            f"/nodes/artifacts/{node_id}/{artifact['name']}",
            timeout=config.get("streaming-responses.timeout"),
        ) as resp:
            if resp.status != 200:
                print(await resp.text())
                e_msg = (
                    f"Could not download artifact "
                    f"{artifact['name']} from node {node_id}"
                )
                self.logger.error(e_msg)
                return False

            total_size = artifact["size"]
            try:
                async with aiofiles.open(dst_path, mode="wb") as f:
                    with tqdm(
                        total=1,
                        desc=f"Downloading {dst_path.name}",
                        unit="B",
                        unit_scale=True,
                    ) as pbar:
                        async for chunk in resp.content.iter_chunked(
                            config.get("streaming-responses.chunk-size") * 1024
                        ):
                            await f.write(chunk)
                            pbar.update(len(chunk) / total_size)
            except Exception as e:
                self.logger.error(
                    f"Could not save artifact {artifact['name']} "
                    f"from node {node_id}. Error: {e}"
                )
                return False

        return True

    def _create_worker_dir(self) -> pathlib.Path:
        """Create the worker's directory."""
        worker_dir = (
            self.state.logdir / self.state.workers[self.worker_id].name
        )  # TODO: Match current format
        worker_dir.mkdir(exist_ok=True, parents=True)
        return worker_dir

    def _find_node_state_by_id(self, node_id) -> NodeState:
        """Find the node's state by its id."""
        worker_state = self.state.workers[self.worker_id]
        node_state = worker_state.nodes[node_id]
        return node_state

    async def collect(
        self, timeout=config.get("comms.timeout.artifacts-ready")
    ) -> bool:
        """Collect the recorded artifacts from the nodes."""
        async with aiohttp.ClientSession(base_url=self.base_url) as session:
            await self._request_artifacts_gather(session)
            await self._wait_till_artifacts_ready(timeout)
            artifacts = await self._request_artifacts_info(session)
            return await self._download_artifacts(session, artifacts)
