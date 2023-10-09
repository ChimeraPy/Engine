import asyncio
import time

import tqdm

import chimerapy.engine as cpe
from chimerapy.engine.worker.node_handler_service import NodeHandlerService
from chimerapy.engine.worker.http_server_service import HttpServerService
from chimerapy.engine.eventbus import EventBus, make_evented, Event
from chimerapy.engine.states import WorkerState

logger = cpe._logger.getLogger("chimerapy-engine")

N = 10
M = 10


class GenNode(cpe.Node):
    def setup(self):
        self.value = 2

    def step(self):
        time.sleep(0.5)
        self.logger.debug(self.value)
        return self.value


async def setup_node_handler():

    # Event Loop
    eventbus = EventBus()

    # Requirements
    state = make_evented(WorkerState(), event_bus=eventbus)
    logger = cpe._logger.getLogger("chimerapy-engine-worker")
    log_receiver = cpe._logger.get_node_id_zmq_listener()
    log_receiver.start(register_exit_handlers=True)

    # Create service
    node_handler = NodeHandlerService(
        name="node_handler",
        state=state,
        eventbus=eventbus,
        logger=logger,
        logreceiver=log_receiver,
    )
    await node_handler.async_init()

    # Necessary dependency
    http_server = HttpServerService(
        name="http_server", state=state, eventbus=eventbus, logger=logger
    )
    await http_server.async_init()

    await eventbus.asend(Event("start"))

    return (node_handler, http_server, eventbus)


async def main():

    node_handler_setup = await setup_node_handler()
    node_handler, _, eventbus = node_handler_setup

    gen_node = GenNode(name="Gen1")
    context = "multiprocessing"

    c_times = []
    d_times = []
    for i in tqdm.tqdm(range(N)):

        tic = time.perf_counter()
        await node_handler.async_create_node(cpe.NodeConfig(gen_node, context=context))
        toc = time.perf_counter()
        c_times.append(toc - tic)

        tic = time.perf_counter()
        await node_handler.async_destroy_node(gen_node.id)
        toc = time.perf_counter()
        d_times.append(toc - tic)

    print(f"Create time: {sum(c_times)/len(c_times)}")
    print(f"Destroy time: {sum(d_times)/len(d_times)}")

    eventbus.send(Event("shutdown"))


async def main_multiple_creation():

    node_handler_setup = await setup_node_handler()
    node_handler, _, eventbus = node_handler_setup

    gen_nodes = [GenNode(name=f"Gen{i}") for i in range(M)]
    context = "multiprocessing"

    c_times = []
    d_times = []
    for i in tqdm.tqdm(range(N)):

        tic = time.perf_counter()
        tasks = [
            asyncio.create_task(
                node_handler.async_create_node(
                    cpe.NodeConfig(gen_node, context=context)
                )
            )
            for gen_node in gen_nodes
        ]
        await asyncio.gather(*tasks)
        toc = time.perf_counter()
        c_times.append(toc - tic)

        tic = time.perf_counter()
        tasks = [
            asyncio.create_task(node_handler.async_destroy_node(gen_node.id))
            for gen_node in gen_nodes
        ]
        await asyncio.gather(*tasks)
        toc = time.perf_counter()
        d_times.append(toc - tic)

    print(f"Create time: {sum(c_times)/len(c_times)}")
    print(f"Destroy time: {sum(d_times)/len(d_times)}")

    await eventbus.asend(Event("shutdown"))


if __name__ == "__main__":
    asyncio.run(main_multiple_creation())
