import asyncio
import os
import pathlib
import time
from typing import Dict

import cv2
import numpy as np

import chimerapy.engine as cpe

CWD = pathlib.Path(os.path.abspath(__file__)).parent
cpe.debug()


class WebcamNode(cpe.Node):
    def setup(self):
        self.vid = cv2.VideoCapture(0)

    def step(self) -> cpe.DataChunk:
        time.sleep(1 / 30)
        ret, frame = self.vid.read()
        self.save_video(name="test", data=frame, fps=15)
        data_chunk = cpe.DataChunk()
        data_chunk.add("frame", frame, "image")
        return data_chunk

    def teardown(self):
        self.vid.release()


class ShowWindow(cpe.Node):
    def step(self, data_chunks: Dict[str, cpe.DataChunk]):

        for name, data_chunk in data_chunks.items():
            # self.logger.debug(f"{self}: got from {name}, data={data_chunk}")
            cv2.imshow(name, data_chunk.get("frame")["value"])
            cv2.waitKey(1)


class RemoteCameraGraph(cpe.Graph):
    def __init__(self):
        super().__init__()
        self.web = WebcamNode(name="web")
        self.show = ShowWindow(name="show")

        self.add_nodes_from([self.web, self.show])
        self.add_edge(src=self.web, dst=self.show)
        self.node_ids = [self.web.id, self.show.id]


async def main():

    # Create default manager and desired graph
    manager = cpe.Manager(logdir=CWD / "runs")
    await manager.aserve()
    await manager.async_zeroconf()
    worker = cpe.Worker(name="local", id="local")
    await worker.aserve()

    # Then register graph to Manager
    await worker.async_connect(host=manager.host, port=manager.port)

    # Wait until workers connect
    while True:
        q = input("All workers connected? (Y/n)")
        if q.lower() == "y":
            break

    # Assuming one worker
    # mapping = {"remote": [graph.web.id], worker.id: [graph.show.id]}
    # For local only
    if len(manager.workers) == 1:
        graph = RemoteCameraGraph()
        mapping = {worker.id: graph.node_ids}
    else:

        # For mutliple workers (remote and local)
        graph = cpe.Graph()
        show_node = ShowWindow(name="show")
        graph.add_node(show_node)
        mapping = {worker.id: [show_node.id]}

        for worker_id in manager.workers:
            if worker_id == "local":
                continue
            else:
                web_node = WebcamNode(name=f"web-{worker_id}")
                graph.add_nodes_from([web_node])
                graph.add_edges_from([(web_node, show_node)])
                mapping[worker_id] = [web_node.id]

    # Commit the graph
    try:
        await manager.async_commit(graph=graph, mapping=mapping)
        await manager.async_start()

        # Wail until user stops
        # while True:
        #     q = input("Ready to start? (Y/n)")
        #     if q.lower() == "y":
        #         break

        await manager.async_record()

        # Wail until user stops
        while True:
            q = input("Stop? (Y/n)")
            if q.lower() == "y":
                break

        await manager.async_stop()
        await manager.async_collect()
    except Exception:
        print("System failed")
    finally:
        await manager.async_shutdown()
        await worker.async_shutdown()


if __name__ == "__main__":
    asyncio.run(main())
