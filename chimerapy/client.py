# Built-in Imports
from typing import Literal, Callable, Dict, Any
import os
import logging
import collections
import uuid
import struct
import pdb
import pathlib
import platform
import tempfile
import shutil
import socket
import threading
import time
import math

# Third-party Imports

# Internal Imports
from . import socket_handling as sh
from .utils import create_payload, decode_payload
from . import enums
from . import _logger

logger = _logger.getLogger("chimerapy-networking")


class Client(threading.Thread):
    def __init__(
        self,
        host: str,
        port: int,
        name: str,
        connect_timeout: float,
        sender_msg_type: Literal["WORKER_MESSAGE", "NODE_MESSAGE"],
        accepted_msg_type: Literal["WORKER_MESSAGE", "MANAGER_MESSAGE", "NODE_MESSAGE"],
        handlers: Dict[str, Callable],
    ):
        super().__init__()

        # State variables
        self.has_shutdown = False

        # Saving input parameters
        self.name = name
        self.sender_msg_type = sender_msg_type
        self.accepted_msg_type = accepted_msg_type
        self.handlers = handlers

        # Tracking of the uuids of ACKS
        self.ack_uuids = collections.deque([], 10)

        # Saving thread state information
        self.is_running = threading.Event()
        self.is_running.set()

        # Creating tempfolder to host items
        self.tempfolder = pathlib.Path(tempfile.mkdtemp())
        self.handlers.update({enums.FILE_TRANSFER_START: self.receive_file})
        self.file_transfer_records = collections.defaultdict(dict)

        # Create the socket and connect
        try:
            self.socket = socket.create_connection(
                (host, port), timeout=connect_timeout
            )
            logger.debug(f"{self} connection to Server successful")
        except socket.error:
            raise RuntimeError(
                f"{self}: Connection refused! Check that you have the correct ip address and port. Tried {host}, {port}"
            )

        # Modifying socket and signal information
        self.socket.settimeout(0.2)

    def __repr__(self):
        return f"<Client {self.name} {self.sender_msg_type}->{self.accepted_msg_type}>"

    def __str__(self):
        return self.__repr__()

    def process_msg(self, msg: Dict):

        # Check that it is from a client
        if msg["type"] == self.accepted_msg_type:

            # Handle ACK differently
            if msg["signal"] != enums.DATA_ACK:

                # If msg request acknoledgement, send it
                if msg["ack"]:

                    # Get message information
                    msg_bytes, msg_length = create_payload(
                        type=self.sender_msg_type,
                        signal=enums.DATA_ACK,
                        data={"success": 1},
                        provided_uuid=msg["uuid"],
                        ack=True,
                    )

                    # Send ACK
                    self.socket.sendall(msg_length + msg_bytes)

                # Obtain the fn to execute
                fn = self.handlers[msg["signal"]]
                fn(msg)

            # If ACK, update table information
            else:
                self.ack_uuids.append(msg["uuid"])

        else:
            logger.error("Client: Invalid message")

    def run(self):

        # Continue checking for messages from client until not running
        while self.is_running.is_set():

            # Monitor the socket
            try:
                success, msg = sh.monitor(self.name, self.socket)
            except (ConnectionResetError, ConnectionAbortedError):
                break

            # Process the msg
            if success:
                self.process_msg(msg)

        self.socket.close()

    def send(self, msg: Dict, ack: bool = False):

        # Sending the data
        success, msg_uuid = sh.send(
            name=self.name,
            s=self.socket,
            msg=msg,
            sender_msg_type=self.sender_msg_type,
            ack=ack,
        )

        # If not successful, skip ACK
        if not success:
            return None

        # If requested ACK, wait
        if ack:

            # Wait until ACK
            miss_counter = 0
            while self.is_running.is_set():
                time.sleep(0.1)
                if msg_uuid in self.ack_uuids:
                    break
                else:
                    logger.debug(
                        f"{self}: waiting ACK for msg: {msg_uuid} given {self.ack_uuids}."
                    )

                    if miss_counter > 20:
                        raise RuntimeError("Client ACK waiting timeout!")

                    miss_counter += 1

    def receive_file(self, msg: Dict[str, Any]):

        # Obtain the file
        success, sender_name, dst_filepath = sh.file_transfer_receive(
            name=self.name, s=self.socket, msg=msg, tempfolder=self.tempfolder
        )

        # Create a record of the files transferred and from whom
        if success:
            self.file_transfer_records[sender_name][dst_filepath.name] = dst_filepath

    def send_file(self, name: str, filepath: pathlib.Path):

        assert filepath.exists() and filepath.is_file()

        sh.send_file(
            sender_name=name,
            sender_msg_type=self.sender_msg_type,
            s=self.socket,
            filepath=filepath,
            buffersize=4096,
        )

    def send_folder(self, name: str, folderpath: pathlib.Path):

        assert folderpath.exists() and folderpath.is_dir()

        sh.send_folder(
            name=name,
            s=self.socket,
            dir=folderpath,
            tempfolder=self.tempfolder,
            sender_msg_type=self.sender_msg_type,
        )

    def shutdown(self):

        # First, indicate the end
        self.is_running.clear()

        # Delete temp folder if requested
        if self.tempfolder.exists():
            shutil.rmtree(self.tempfolder)

        # Mark that the Server has shutdown
        self.has_shutdown = True

    def __del__(self):

        # Also good to shutdown anything that isn't
        if not self.has_shutdown:
            self.shutdown()
            if self.is_alive():
                self.join()
