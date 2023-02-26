import datetime
import json

import aiohttp
from aiohttp import web

from .manager import Manager
from .networking.enums import MANAGER_MESSAGE
from . import _logger

logger = _logger.getLogger("chimerapy")


class API:
    def __init__(self, manager: Manager):
        self.manager = manager
        self.manager.server.add_routes(
            [
                web.get("/network", self.get_network),
                web.post("/registered_methods", self.execute_registered_method),
            ]
        )

    ####################################################################
    # HTTP Routes
    ####################################################################

    async def get_network(self, request: web.Request):
        return web.json_response(self.manager.state.to_dict())

    async def execute_registered_method(self, request: web.Request):
        msg = await request.json()

        # First, identify which worker has the node
        worker_id = self.manager.node_to_worker_lookup(msg["node_id"])

        if not isinstance(worker_id, str):
            return web.HTTPBadRequest()

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"http://{self.manager.state.workers[worker_id].ip}:{self.manager.state.workers[worker_id].port}"
                + "/nodes/registered_methods",
                data=json.dumps(
                    {
                        "node_id": msg["node_id"],
                        "method_name": msg["method_name"],
                        "params": msg["params"],
                        "timeout": msg["timeout"],
                    }
                ),
            ) as resp:
                return web.json_response(await resp.json())

    ####################################################################
    # WS
    ####################################################################

    async def broadcast_node_update(self):
        await self.manager.server.async_broadcast(
            signal=MANAGER_MESSAGE.NODE_STATUS_UPDATE,
            data=self.manager.state.to_dict(),
        )
