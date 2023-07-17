import threading
import time

import zmq
from zmq import Context

from chimerapy.engine._logger import ZMQLogHandlerConfig, add_zmq_handler, getLogger


def test_logger_publishing():
    logger = getLogger("chimerapy-engine")
    config = ZMQLogHandlerConfig(
        publisher_port=0, root_topic="chimerapy_logs", transport="ws"
    )
    add_zmq_handler(logger, config)

    # Determine the port
    endpoint = logger.handlers[1].socket.getsockopt(zmq.LAST_ENDPOINT).decode()
    port = endpoint.split(":")[-1][:-1]

    logs = [f"Test log {i}" for i in range(1, 10)]
    received_logs = []

    def subscribe_and_read():
        context = Context()
        socket = context.socket(zmq.SUB)
        socket.connect(f"ws://localhost:{port}")
        socket.subscribe("chimerapy_logs")
        while True:
            [_, msg] = socket.recv_multipart()
            received_logs.append(msg.decode("utf-8"))
            if len(received_logs) == len(logs):
                break

    def log_():
        time.sleep(1)  # Wait for the subscriber to connect
        for log in logs:
            logger.info(log)

    thread = threading.Thread(target=subscribe_and_read)
    thread.start()
    log_()
    thread.join()
    for i, log in enumerate(received_logs):
        assert log in received_logs[i]
