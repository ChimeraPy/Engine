import random
import time

import zmq.log.handlers
import logging

from faker import Faker


class DummyLogServer:
    def __init__(self, port=10000):
        self.port = port
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind("ws://*:%s" % self.port)

        self.logger = logging.getLogger("DummyLogServer")
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        self.pub_handler = zmq.log.handlers.PUBHandler(self.socket)
        self.pub_handler.setFormatter(formatter)
        self.console_handler = logging.StreamHandler()

        self.pub_handler.root_topic = "mock_logs"
        self.logger.addHandler(self.pub_handler)
        self.logger.addHandler(self.console_handler)

        self.data_generator = Faker()

    def run(self):
        while True:
            callable = random.choice(
                [
                    self.logger.debug,
                    self.logger.info,
                    self.logger.warning,
                    self.logger.error,
                    self.logger.critical,
                ]
            )
            callable(
                self.data_generator.sentence(
                    nb_words=15, variable_nb_words=True, ext_word_list=None
                )
            )
            time.sleep(random.random())


if __name__ == "__main__":
    server = DummyLogServer()
    server.run()
