import asyncio
import time
import tempfile
import tqdm
from typing import Dict

import chimerapy.engine as cpe

N = 10
M = 3


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


async def testbed_setup():
    # Create manager
    manager = cpe.Manager(logdir=tempfile.mkdtemp())
    await manager.aserve()

    # Creating worker to communicate
    worker = cpe.Worker(name="local", id="local", port=0)
    await worker.aserve()
    await worker.async_connect(host="localhost", port=manager.port)

    # Define graph
    gen_nodes = [GenNode(name=f"Gen{i}", id=f"Gen{i}") for i in range(M)]
    simple_graph = cpe.Graph()
    simple_graph.add_nodes_from(gen_nodes)

    return (manager, worker, simple_graph)


async def main():
    manager, worker, simple_graph = await testbed_setup()

    # Register graph
    manager._register_graph(simple_graph)

    c_times = []
    d_times = []
    for i in tqdm.tqdm(range(N)):

        tic = time.perf_counter()
        assert await manager.async_commit(
            graph=simple_graph, mapping={worker.id: ["Gen1"]}
        )
        toc = time.perf_counter()
        c_times.append(toc - tic)

        tic = time.perf_counter()
        assert await manager.async_reset()
        toc = time.perf_counter()
        d_times.append(toc - tic)

    print(f"Create time: {sum(c_times)/len(c_times)}")
    print(f"Destroy time: {sum(d_times)/len(d_times)}")

    await manager.async_shutdown()
    await worker.async_shutdown()


async def main_multiple_creation():
    manager, worker, simple_graph = await testbed_setup()

    # Register graph
    manager._register_graph(simple_graph)

    c_times = []
    d_times = []
    for i in tqdm.tqdm(range(N)):

        tic = time.perf_counter()
        try:
            assert await manager.async_commit(
                graph=simple_graph, mapping={worker.id: [f"Gen{i}" for i in range(M)]}
            )
        except Exception as e:
            print(e)
        toc = time.perf_counter()
        c_times.append(toc - tic)

        tic = time.perf_counter()
        try:
            assert await manager.async_reset()
        except Exception as e:
            print(e)
        toc = time.perf_counter()
        d_times.append(toc - tic)

    print(f"Create time: {sum(c_times)/len(c_times)}")
    print(f"Destroy time: {sum(d_times)/len(d_times)}")

    await manager.async_shutdown()
    await worker.async_shutdown()


if __name__ == "__main__":
    # asyncio.run(main())
    asyncio.run(main_multiple_creation())
