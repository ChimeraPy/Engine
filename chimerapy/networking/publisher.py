# Built-in Imports
import threading
import pickle
import time

# Third-party Imports
from PIL import ImageGrab
import numpy as np
import imutils
import zmq
import mss
import cv2
import datetime
import simplejpeg

# Resources:
# https://learning-0mq-with-pyzmq.readthedocs.io/en/latest/pyzmq/patterns/pushpull.html


def serialize(msg):
    return pickle.dumps(msg, pickle.HIGHEST_PROTOCOL)


class Publisher:
    def __init__(self, port: int):

        self.port = port
        self.running = False

    def send(self, frame):
        self.frame = frame
        self.ready.set()

    def send_loop(self):

        while self.running:

            # # Wait until we have data to send!
            flag = self.ready.wait(timeout=1)
            if not flag:
                continue

            # Send
            self.zmq_socket.send(
                serialize(
                    {
                        "timestamp": datetime.datetime.now(),
                        "frame": simplejpeg.encode_jpeg(
                            np.ascontiguousarray(self.frame)
                        ),
                    }
                )
            )

            # Mark finish
            self.ready.clear()
            self.prev_frame = self.frame.copy()

    def start(self):

        # Mark that the producer is running
        self.running = True

        # Create the socket
        self.zmq_context = zmq.Context()
        self.zmq_socket = self.zmq_context.socket(zmq.PUB)
        self.zmq_socket.bind(f"tcp://*:{self.port}")

        # Create send thread
        self.ready = threading.Event()
        self.ready.clear()
        self.send_thread = threading.Thread(target=self.send_loop)
        self.send_thread.start()

    def shutdown(self):

        # Mark to stop
        self.running = False
        self.send_thread.join()
