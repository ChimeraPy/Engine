from typing import Dict, Any
import time
import pathlib
import os

import numpy as np
import cv2

import chimerapy as cp

CWD = pathlib.Path(os.path.abspath(__file__)).parent
cp.debug()


class WebcamNode(cp.Node):
    def prep(self):
        self.vid = cv2.VideoCapture(0)

    def step(self) -> cp.DataChunk:
        time.sleep(1 / 30)
        ret, frame = self.vid.read()
        self.save_video(name="test", data=frame, fps=20)
        data_chunk = cp.DataChunk()
        data_chunk.add("frame", frame, "image")
        return data_chunk

    def teardown(self):
        self.vid.release()


class ShowWindow(cp.Node):
    def step(self, data_chunks: Dict[str, cp.DataChunk]):

        for name, data_chunk in data_chunks.items():
            self.logger.debug(f"{self}: got from {name}, data={data_chunk}")

            cv2.imshow(name, data_chunk.get("frame")["value"])
            cv2.waitKey(1)


class RemoteCameraGraph(cp.Graph):
    def __init__(self):
        super().__init__()
        web = WebcamNode(name="web")
        show = ShowWindow(name="show")

        self.add_nodes_from([web, show])
        self.add_edge(src=web, dst=show)


if __name__ == "__main__":

    # Create default manager and desired graph
    manager = cp.Manager(logdir=CWD / "runs")
    graph = RemoteCameraGraph()
    worker = cp.Worker(name="local")

    # Then register graph to Manager
    worker.connect(host=manager.host, port=manager.port)

    # Wait until workers connect
    while True:
        q = input("All workers connected? (Y/n)")
        if q.lower() == "y":
            break

    # Assuming one worker
    # mapping = {"remote": ["web"], "local": ["show"]}
    mapping = {"local": ["web", "show"]}

    # Commit the graph
    manager.commit_graph(graph=graph, mapping=mapping)

    # Wail until user stops
    while True:
        q = input("Ready to start? (Y/n)")
        if q.lower() == "y":
            break

    manager.start()

    # Wail until user stops
    while True:
        q = input("Stop? (Y/n)")
        if q.lower() == "y":
            break

    manager.stop()
    manager.collect()
    manager.shutdown()
