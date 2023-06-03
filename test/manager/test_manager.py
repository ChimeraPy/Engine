from ..conftest import TEST_DATA_DIR
from ..streams import VideoNode, TabularNode

import time

from aiohttp import web
import pytest

import chimerapy as cp


def test_manager_instance(manager):
    ...


class RemoteTransferWorker(cp.Worker):
    async def _async_send_archive(self, request: web.Request):
        await request.json()

        # Collect data from the Nodes
        success = await self.async_collect()

        # If located in the same computer, just move the data
        if success:
            await self._async_send_archive_remotely(
                self.manager_host, self.manager_port
            )

        # After completion, let the Manager know
        self.logger.debug(f"{self}: Responded to Manager collect request!")
        return web.json_response({"id": self.id, "success": success})


@pytest.mark.parametrize(
    "node_cls",
    [VideoNode, TabularNode],
)
def test_manager_remote_transfer(node_cls):

    manager = cp.Manager(logdir=TEST_DATA_DIR, port=0)
    remote_worker = RemoteTransferWorker(name="remote", id="remote")
    remote_worker.connect(method="ip", host=manager.host, port=manager.port)

    node = node_cls(name="node")

    # Define graph
    graph = cp.Graph()
    graph.add_nodes_from([node])

    future = manager.commit_graph(graph=graph, mapping={remote_worker.id: [node.id]})
    assert future.result(timeout=30)
    assert manager.start().result()

    time.sleep(0.5)

    assert manager.record().result()

    time.sleep(0.5)

    assert manager.stop().result()
    assert manager.collect().result()

    future = manager.reset(keep_workers=True)
    assert future.result(timeout=30)

    manager.shutdown()

    # The files from the remote worker should exists!
    assert (
        manager.logdir / f"{remote_worker.id}-{remote_worker.name}" / node.name
    ).exists()
