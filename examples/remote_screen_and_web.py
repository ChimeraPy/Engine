from typing import Dict
import time
import pathlib
import os
import platform

import numpy as np
import cv2
import imutils
from PIL import ImageGrab

import chimerapy.engine as cpe

cpe.debug()

CWD = pathlib.Path(os.path.abspath(__file__)).parent


class WebcamNode(cpe.Node):
    def setup(self):
        self.vid = cv2.VideoCapture(0)

    def step(self):
        time.sleep(1 / 15)
        ret, frame = self.vid.read()
        self.save_video(name="test", data=frame, fps=20)
        data_chunk = cpe.DataChunk()
        data_chunk.add("frame", frame, "image")
        return data_chunk

    def teardown(self):
        self.vid.release()


class ScreenCaptureNode(cpe.Node):
    def setup(self):
        if platform.system() == "Windows":
            import dxcam

            self.camera = dxcam.create()
        else:
            self.camera = None

    def step(self):
        # Noticed that screencapture methods highly depend on OS
        if platform.system() == "Windows":
            time.sleep(1 / 15)
            frame = self.camera.grab()
            if not isinstance(frame, np.ndarray):
                return None
            else:
                frame = cv2.cvtColor(frame, cv2.COLOR_RGB2BGR)
        else:
            time.sleep(1 / 15)
            frame = cv2.cvtColor(
                np.array(ImageGrab.grab(), dtype=np.uint8), cv2.COLOR_RGB2BGR
            )

        # Save the frame and package it
        self.save_video(name="screen", data=frame, fps=15)
        data_chunk = cpe.DataChunk()
        data_chunk.add("frame", imutils.resize(frame, width=720), "image")

        # Then send it
        return data_chunk


class ShowWindow(cpe.Node):
    def step(self, data_chunks: Dict[str, cpe.DataChunk]):

        for name, data_chunk in data_chunks.items():
            self.logger.debug(f"{self}: got from {name}, data={data_chunk}")

            cv2.imshow(name, data_chunk.get("frame")["value"])
            cv2.waitKey(1)


class RemoteCameraGraph(cpe.Graph):
    def __init__(self):
        super().__init__()
        web = WebcamNode(name="web")
        screen = ScreenCaptureNode(name="screen")
        show = ShowWindow(name="show")

        self.add_nodes_from([web, show, screen])
        self.add_edges_from([(web, show), (screen, show)])
        self.node_ids = [web.id, screen.id, show.id]


if __name__ == "__main__":

    cpe.debug()

    # Create default manager and desired graph
    manager = cpe.Manager(logdir=CWD / "runs", port=9000)
    manager.zeroconf()
    worker = cpe.Worker(name="local", id="local", port=0)

    # Then register graph to Manager
    worker.connect(host=manager.host, port=manager.port)

    # Wait until workers connect
    while True:
        q = input("All workers connected? (Y/n)")
        if q.lower() == "y":
            break

    # For local only
    if len(manager.workers) == 1:
        graph = RemoteCameraGraph()
        mapping = {worker.id: graph.node_ids}
    else:

        print(
            "WARNING: ScreenCaptureNode is faulty for this "
            "configuration for unknown reasons"
        )

        # For mutliple workers (remote and local)
        graph = cpe.Graph()
        show_node = ShowWindow(name="show")
        graph.add_node(show_node)
        mapping = {worker.id: [show_node.id]}

        for worker_id in manager.workers:
            if worker_id == "local":
                continue
            else:
                web_node = WebcamNode(name="web")
                screen_node = ScreenCaptureNode(name="screen")
                graph.add_nodes_from([web_node, screen_node])
                graph.add_edges_from([(screen_node, show_node), (web_node, show_node)])
                mapping[worker_id] = [web_node.id, screen_node.id]

    # Commit the graph
    manager.commit_graph(graph=graph, mapping=mapping).result()
    manager.start().result(timeout=5)

    # Wail until user stops
    while True:
        q = input("Ready to start? (Y/n)")
        if q.lower() == "y":
            break

    manager.record().result(timeout=5)

    # Wail until user stops
    while True:
        q = input("Stop? (Y/n)")
        if q.lower() == "y":
            break

    manager.stop().result(timeout=5)
    manager.collect().result()
    manager.shutdown()
