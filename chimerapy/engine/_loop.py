import asyncio
import uvloop


def setup():
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
