import logging
import uuid
from multiprocessing import Process

from chimerapy.logger.zmq_handlers import NodeIDZMQPullListener, NodeIdZMQPushHandler


class LogRecordsCollector(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


def test_zmq_push_pull_node_id_logging():
    handler = LogRecordsCollector()
    handler.setLevel(logging.DEBUG)
    listener = NodeIDZMQPullListener(handlers=(handler,), respect_handler_level=True)
    listener.start()
    ids = [str(uuid.uuid4()) for _ in range(2)]

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

    p1 = Process(target=get_log_and_messages, args=(listener.port, ids[0]))
    p2 = Process(target=get_log_and_messages, args=(listener.port, ids[1]))

    p1.start()
    p2.start()

    p1.join()
    p2.join()

    assert len(handler.records) == 20
    assert set(map(lambda record: record.node_id, handler.records)) == set(ids)
    listener.stop()
