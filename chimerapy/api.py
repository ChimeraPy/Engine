from aiohttp import web

from .manager import Manager
from .networking.enums import MANAGER_MESSAGE


class API:
    def __init__(self, manager: Manager):
        self.manager = manager
        self.manager.server.add_routes([web.get("/network", self.get_network)])

    ####################################################################
    # HTTP Routes
    ####################################################################

    async def get_network(self, request: web.Request):
        return web.json_response(self.manager.state.to_dict())

    ####################################################################
    # WS
    ####################################################################

    async def broadcast_node_update(self):
        await self.manager.server.async_broadcast(
            signal=MANAGER_MESSAGE.NODE_STATUS_UPDATE,
            data=self.manager.state.to_dict(),
        )
