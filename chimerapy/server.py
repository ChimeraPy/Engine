from typing import Literal, Callable, Dict
import logging
import collections
import uuid
import struct

import pdb

logger = logging.getLogger("chimerapy")

import socket
import threading
import time

from .utils import (
    threaded,
    create_payload,
    log,
    get_open_port,
    decode_payload,
    get_ip_address,
)
from . import enums


class Server(threading.Thread):
    def __init__(
        self,
        port: int,
        name: str,
        max_num_of_clients: int,
        sender_msg_type: Literal["WORKER_MESSAGE", "MANAGER_MESSAGE", "NODE_MESSAGE"],
        accepted_msg_type: Literal["WORKER_MESSAGE", "MANAGER_MESSAGE", "NODE_MESSAGE"],
        handlers: Dict[str, Callable],
    ):
        super().__init__()

        # Saving input parameters
        self.name = name
        self.max_num_of_clients = max_num_of_clients
        self.sender_msg_type = sender_msg_type
        self.accepted_msg_type = accepted_msg_type
        self.handlers = handlers

        # Create listening socket
        self.socket = get_open_port(port)
        self.host = get_ip_address()
        _, self.port = self.socket.getsockname()
        self.socket.listen(self.max_num_of_clients)
        self.socket.settimeout(0.2)

        # Saving thread state information
        self.is_running = threading.Event()
        self.is_running.set()

        # Keeping track of client threads
        self.client_comms = {}

    def __repr__(self):
        return f"<Server {self.name} {self.sender_msg_type}->{self.accepted_msg_type}>"

    def __str__(self):
        return self.__repr__()

    def run(self):

        while self.is_running.is_set():

            try:
                s, addr = self.socket.accept()
            except socket.timeout:
                continue

            # Logging and configuring socket
            logger.info(f"{self}: Got connection from {addr}")
            s.settimeout(0.2)

            # Start thread for new client
            thread = self.client_comm_thread(s, addr)
            thread.start()

            # Saving new client thread
            self.client_comms[s] = {"thread": thread, "acks": collections.deque([], 10)}

    def process_msg(self, msg: Dict, s: socket.socket):

        # Check that it is from a client
        if msg["type"] == self.accepted_msg_type:

            # Handle ACK differently
            if msg["signal"] != enums.DATA_ACK:

                # If ACK requested, send it
                if msg["ack"]:

                    msg_bytes, msg_length = create_payload(
                        type=self.sender_msg_type,
                        signal=enums.DATA_ACK,
                        data={"success": 1},
                        provided_uuid=msg["uuid"],
                        ack=True,
                    )

                    # Send ACK
                    s.sendall(msg_length + msg_bytes)

                # Obtain the fn to execute
                fn = self.handlers[msg["signal"]]
                fn(msg, s)

            # If ACK, update table information
            else:
                logger.debug(f"{self}: Update ACK for msg: {msg['uuid']}, client: {s}")
                self.client_comms[s]["acks"].append(msg["uuid"])

        else:
            logger.error("Invalid message from client")

    @threaded
    def client_comm_thread(self, s: socket.socket, addr):

        # Continue checking for messages from client until not running
        while self.is_running.is_set():

            # Check if the socket is closed
            if s.fileno() == -1:
                break

            # Get message while not blocking
            try:
                # Get the data
                try:
                    bs = s.recv(8)
                except ConnectionResetError:
                    logger.warning(f"{self}: connection lost, shutting down")
                    break

                # If null, skip
                if bs == b"":
                    continue

                # Get the length
                (length,) = struct.unpack(">Q", bs)
                data = b""

                # Use the length to get the entire message
                while len(data) < int(length):
                    to_read = int(length) - len(data)
                    data += s.recv(4096 if to_read > 4096 else to_read)

            except socket.timeout:
                continue

            # If end, stop it
            if data == b"":
                break

            # Decode the message so we can process it
            msg = decode_payload(data)

            logger.info(f"{self} msg received, content: {msg['signal']}")

            # Process the msg
            self.process_msg(msg, s)

        s.close()

    def send(self, s: socket.socket, msg: Dict, ack: bool = False):

        # Check first if the connection is alive
        if s.fileno() == -1:
            logger.warning(
                f"Tried to send {msg} to dead client connection name: socket: {s}."
            )
            return

        # Create an uuid to track the message
        msg_uuid = str(uuid.uuid4())

        # Convert msg data to bytes
        msg_bytes, msg_length = create_payload(
            type=self.sender_msg_type,
            signal=msg["signal"],
            data=msg["data"],
            provided_uuid=msg_uuid,
            ack=ack,
        )

        # Sending message
        try:
            s.sendall(msg_length + msg_bytes)
            logger.debug(f"{self}: send {msg['signal']}")
        except socket.timeout:
            logger.warning(f"{self}: Socket Timeout: skipping")
            return
        except:
            logger.warning(
                f"{self}: Broken Pipe Error, handled for {msg['signal']}", exc_info=True
            )
            return

        # If ACK requested, wait
        if ack:

            # Wait for an ACK message that changes client's ACK status
            miss_counter = 0
            while self.is_running:
                time.sleep(0.1)
                if msg_uuid in self.client_comms[s]["acks"]:
                    logger.debug(f"Server received ACK")
                    break
                else:
                    logger.debug(
                        f"Waiting for ACK for msg: {(msg['signal'], msg_uuid)} from s: {s}, ack: {self.client_comms[s]['acks']}"
                    )

                    if miss_counter >= 20:
                        raise RuntimeError(f"Server ACK timeout for {msg}")

                    miss_counter += 1

    def broadcast(self, msg: Dict, ack: bool = False):

        for s in self.client_comms:
            self.send(s, msg, ack)

    def shutdown(self):

        # First, send shutdown message to clients
        for client_socket in self.client_comms:
            try:
                self.send(client_socket, {"signal": enums.SHUTDOWN, "data": {}})
            except socket.error:
                logger.warning("Socket send msg to broken pipe", exc_info=True)

        # First, indicate the end
        self.is_running.clear()

        # Wait for threads
        for client_comm_data in self.client_comms.values():
            client_comm_data["thread"].join()
