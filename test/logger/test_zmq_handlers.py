import logging
from logging.handlers import BufferingHandler
import uuid
from multiprocessing import Process
import time


from chimerapy.engine.logger.zmq_handlers import (
    NodeIDZMQPullListener,
    NodeIdZMQPushHandler,
)


def get_log_and_messages(port, node_id):
    zmq_push_handler = NodeIdZMQPushHandler("localhost", port)
    logger = logging.getLogger("test")
    logger.setLevel(logging.DEBUG)
    zmq_push_handler.setLevel(logging.DEBUG)
    logger.addHandler(zmq_push_handler)
    zmq_push_handler.register_node_id(node_id)
    assert len(logger.handlers) == 1
    for j in range(10):
        logger.debug(f"Message {j}")


def test_zmq_push_pull_node_id_logging():
    handler = BufferingHandler(capacity=300)
    handler.setLevel(logging.DEBUG)
    logreceiver = NodeIDZMQPullListener(handlers=[handler])
    logreceiver.start()
    ids = [str(uuid.uuid4()) for _ in range(2)]

    p1 = Process(target=get_log_and_messages, args=(logreceiver.port, ids[0]))
    p2 = Process(target=get_log_and_messages, args=(logreceiver.port, ids[1]))

    p1.start()
    p2.start()

    p1.join()
    p2.join()

    time.sleep(5)

    assert len(handler.buffer) == 20
    assert set(map(lambda record: record.node_id, handler.buffer)) == set(ids)

    logreceiver.stop()
    logreceiver.join()
