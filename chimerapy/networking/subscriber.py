# Built-in
import threading
import pickle
import datetime

# Third-party Imports
import numpy as np
import imutils
import zmq
import cv2
import simplejpeg


class Subscriber:
    def __init__(self, port: int, host: str):

        self.port = port
        self.host = host
        self.running = False

    def receive_loop(self):

        while self.running:

            # Send
            pickled_msg = self.zmq_socket.recv()
            msg = pickle.loads(pickled_msg)
            self.frame = simplejpeg.decode_jpeg(msg["frame"])

            self.ready.set()

            # Compute delta time
            delta = (datetime.datetime.now() - msg["timestamp"]).total_seconds()
            print(f"fps: {(1/delta):.2f}")

    def receive(self):

        while self.running:

            flag = self.ready.wait(timeout=1)
            if not flag:
                continue

            return self.frame

    def start(self):

        # Mark that the Subscriber is running
        self.running = True
        self.frame = None

        # Create socket
        self.zmq_context = zmq.Context()
        self.zmq_socket = self.zmq_context.socket(zmq.SUB)
        self.zmq_socket.setsockopt(zmq.CONFLATE, 1)
        self.zmq_socket.connect(f"tcp://{self.host}:{self.port}")
        self.zmq_socket.subscribe(b"")

        # Create thread
        self.ready = threading.Event()
        self.ready.clear()
        self.receive_thread = threading.Thread(target=self.receive_loop)
        self.receive_thread.start()

    def shutdown(self):
        self.running = False
        self.receive_thread.join()


if __name__ == "__main__":

    subscriber = Subscriber(port=5555, host="localhost")
    subscriber.start()

    try:
        while True:
            frame = subscriber.receive()

            cv2.imshow("subscriber", frame)
            if cv2.waitKey(1) == ord("q"):
                break

    except KeyboardInterrupt:
        print("KeyboardInterrupt detected, shutting down")
    finally:
        subscriber.shutdown()
