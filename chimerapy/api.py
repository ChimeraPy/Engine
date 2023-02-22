from aiohttp import web

from .manager import Manager


class API:
    def __init__(self, manager: Manager):
        self.manager = manager
        self.manager.server.add_routes([web.get("/network", self.get_network)])

    async def get_network(self, request: web.Request):
        return web.json_response(self.manager.state.to_dict())
