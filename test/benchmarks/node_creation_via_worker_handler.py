import asyncio
import time
import pathlib
import tempfile
import tqdm
from typing import Dict

import chimerapy.engine as cpe
from chimerapy.engine.manager.worker_handler_service import WorkerHandlerService
from chimerapy.engine.manager.http_server_service import HttpServerService
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread
from chimerapy.engine.eventbus import EventBus, make_evented, Event
from chimerapy.engine.states import ManagerState

N = 10
M = 10


class GenNode(cpe.Node):
    def setup(self):
        self.value = 2

    def step(self):
        time.sleep(0.5)
        self.logger.debug(self.value)
        return self.value


class ConsumeNode(cpe.Node):
    def setup(self):
        self.coef = 3

    def step(self, data_chunks: Dict[str, cpe.DataChunk]):
        time.sleep(0.1)
        # Extract the data
        self.logger.debug(f"{self}: {data_chunks}")
        # self.logger.debug(
        #     f"{self}: inside step, with {data_chunks} - {data_chunks['Gen1']}"
        # )
        value = data_chunks["Gen1"].get("default")["value"]
        output = self.coef * value
        return output


def testbed_setup():
    # Creating worker to communicate
    worker = cpe.Worker(name="local", id="local", port=0)

    thread = AsyncLoopThread()
    thread.start()
    eventbus = EventBus(thread=thread)

    state = make_evented(
        ManagerState(logdir=pathlib.Path(tempfile.mkdtemp())), event_bus=eventbus
    )

    # Define graph
    # gen_node = GenNode(name="Gen1", id="Gen1")
    gen_nodes = [GenNode(name=f"Gen{i}", id=f"Gen{i}") for i in range(M)]
    simple_graph = cpe.Graph()
    simple_graph.add_nodes_from(gen_nodes)

    # Create services
    http_server = HttpServerService(
        name="http_server",
        port=0,
        enable_api=True,
        thread=thread,
        eventbus=eventbus,
        state=state,
    )
    worker_handler = WorkerHandlerService(
        name="worker_handler", eventbus=eventbus, state=state
    )

    eventbus.send(Event("start")).result()

    # Register worker
    worker.connect(host=http_server.ip, port=http_server.port)

    return (worker_handler, worker, simple_graph, eventbus)


async def main():
    worker_handler, worker, simple_graph, eventbus = testbed_setup()

    # Register graph
    worker_handler._register_graph(simple_graph)

    c_times = []
    d_times = []
    for i in tqdm.tqdm(range(N)):

        tic = time.perf_counter()
        assert await worker_handler._request_node_creation(
            worker_id=worker.id, node_id="Gen1"
        )
        toc = time.perf_counter()
        c_times.append(toc - tic)

        tic = time.perf_counter()
        assert await worker_handler._request_node_destruction(
            worker_id=worker.id, node_id="Gen1"
        )
        toc = time.perf_counter()
        d_times.append(toc - tic)

    print(f"Create time: {sum(c_times)/len(c_times)}")
    print(f"Destroy time: {sum(d_times)/len(d_times)}")

    eventbus.send(Event("shutdown")).result()
    worker.shutdown()


async def main_multiple_creation():
    worker_handler, worker, simple_graph, eventbus = testbed_setup()

    # Register graph
    worker_handler._register_graph(simple_graph)

    c_times = []
    d_times = []
    for i in tqdm.tqdm(range(N)):

        tic = time.perf_counter()
        tasks = [
            asyncio.create_task(
                worker_handler._request_node_creation(
                    worker_id=worker.id, node_id=f"Gen{i}"
                )
            )
            for i in range(M)
        ]
        await asyncio.gather(*tasks)
        toc = time.perf_counter()
        c_times.append(toc - tic)

        tic = time.perf_counter()
        tasks = [
            asyncio.create_task(
                worker_handler._request_node_destruction(
                    worker_id=worker.id, node_id=f"Gen{i}"
                )
            )
            for i in range(M)
        ]
        await asyncio.gather(*tasks)
        toc = time.perf_counter()
        d_times.append(toc - tic)

    print(f"Create time: {sum(c_times)/len(c_times)}")
    print(f"Destroy time: {sum(d_times)/len(d_times)}")

    eventbus.send(Event("shutdown")).result()
    worker.shutdown()


if __name__ == "__main__":
    asyncio.run(main_multiple_creation())
