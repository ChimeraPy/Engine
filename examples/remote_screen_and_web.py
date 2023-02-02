from typing import Dict, Any
import time
import pathlib
import os
import platform

import numpy as np
import cv2
import imutils
from PIL import ImageGrab

import chimerapy as cp
cp.debug()

CWD = pathlib.Path(os.path.abspath(__file__)).parent


class WebcamNode(cp.Node):
    def prep(self):
        self.vid = cv2.VideoCapture(0)

    def step(self):
        time.sleep(1 / 15)
        ret, frame = self.vid.read()
        self.save_video(name="test", data=frame, fps=20)
        data_chunk = cp.DataChunk()
        data_chunk.add("frame", frame, "image")
        return data_chunk

    def teardown(self):
        self.vid.release()


class ScreenCaptureNode(cp.Node):
    def prep(self):

        if platform.system() == "Windows":
            import dxcam

            self.camera = dxcam.create()
        else:
            self.camera = None

    def step(self):
        # Noticed that screencapture methods highly depend on OS
        if platform.system() == "Windows":
            time.sleep(1 / 30)
            frame = self.camera.grab()
            if not isinstance(frame, np.ndarray):
                return None
            else:
                frame = cv2.cvtColor(frame, cv2.COLOR_RGB2BGR)
        else:
            frame = cv2.cvtColor(
                np.array(ImageGrab.grab(), dtype=np.uint8), cv2.COLOR_RGB2BGR
            )

        # Save the frame and package it
        self.save_video(name="screen", data=frame, fps=20)
        data_chunk = cp.DataChunk()
        data_chunk.add("frame", imutils.resize(frame, width=720), "image")

        # Then send it
        return data_chunk


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
        screen = ScreenCaptureNode(name="screen")
        show = ShowWindow(name="show")

        self.add_nodes_from([web, show, screen])
        self.add_edges_from([(web, show), (screen, show)])


if __name__ == "__main__":

    cp.debug()

    # Create default manager and desired graph
    manager = cp.Manager(logdir=CWD / "runs")
    worker = cp.Worker(name="local", port=0)

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
        mapping = {"local": ["web", "show", "screen"]}
    else:

        # For mutliple workers (remote and local)
        graph = cp.Graph()
        show_node = ShowWindow(name=f"local-show")
        graph.add_node(show_node)
        mapping = {"local": ["local-show"]}

        for worker_name in manager.workers:
            if worker_name == "local":
                continue
            else:
                web_node = WebcamNode(name=f"{worker_name}-web")
                screen_node = ScreenCaptureNode(name=f"{worker_name}-screen")
                graph.add_nodes_from([web_node, screen_node])
                graph.add_edges_from([(screen_node, show_node), (web_node, show_node)])
                mapping[worker_name] = [f"{worker_name}-web", f"{worker_name}-screen"]

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
