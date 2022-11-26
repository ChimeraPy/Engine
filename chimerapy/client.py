# Built-in Imports
from typing import Literal, Callable, Dict
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
from .utils import create_payload, decode_payload
from . import enums

logger = logging.getLogger("chimerapy")


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

            # Check if the socket is closed
            if self.socket.fileno() == -1:
                break

            # Get message while not blocking
            try:
                # Get the data
                bs = self.socket.recv(8)

                # If null, skip
                if bs == b"":
                    continue

                # Get the length
                (length,) = struct.unpack(">Q", bs)
                data = b""

                # Use the length to get the entire message
                while len(data) < int(length):
                    to_read = int(length) - len(data)
                    data += self.socket.recv(4096 if to_read > 4096 else to_read)

            except socket.timeout:
                continue

            # If end, stop it
            if data == b"":
                break

            # Decode the message so we can process it
            msg = decode_payload(data)

            # Process the msg
            self.process_msg(msg)

        self.socket.close()

    def send(self, msg: Dict, ack: bool = False):

        # Create an uuid to track the message
        msg_uuid = str(uuid.uuid4())

        # Convert msg data to bytes
        msg_bytes, msg_length = create_payload(
            type=self.sender_msg_type,
            signal=msg["signal"],
            data=msg["data"],
            ack=ack,
            provided_uuid=msg_uuid,
        )

        # Send the message
        try:
            self.socket.sendall(msg_length + msg_bytes)
            logger.debug(f"{self}: send {msg['signal']}")
        except socket.timeout:
            logger.warning(f"{self}: Socket Timeout: skipping")
            return
        except:
            logger.warning(
                f"{self}: Broken Pipe Error, handled for {msg['signal']}", exc_info=True
            )
            return

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

    def send_folder(self, name: str, dir: pathlib.Path, buffersize: int = 4096):

        assert (
            dir.is_dir() and dir.exists()
        ), f"Sending {dir} needs to be a folder that exists."

        # First, we need to archive the folder into a zip file
        format = "zip"
        shutil.make_archive(str(dir), format, dir.parent, dir.name)
        zip_file = dir.parent / f"{dir.name}.{format}"

        # Relocate zip to the tempfolder
        temp_zip_file = self.tempfolder / f"_{zip_file.name}"
        shutil.move(zip_file, temp_zip_file)

        # Get information about the filesize
        filesize = os.path.getsize(temp_zip_file)
        max_num_steps = math.ceil(filesize / buffersize)

        # Now start the process of sending content to the server
        # First, we send the message inciting file transfer
        init_msg = {
            "type": enums.WORKER_MESSAGE,
            "signal": enums.FILE_TRANSFER_START,
            "data": {
                "name": name,
                "filename": f"{dir.name}.{format}",
                "filesize": filesize,
                "buffersize": buffersize,
                "max_num_steps": max_num_steps,
            },
        }
        self.send(init_msg)
        logger.debug(f"{self}: sent file transfer initialization")

        # Having counter tracking number of messages
        msg_counter = 1

        with open(temp_zip_file, "rb") as f:
            while True:

                # Read the data to be sent
                data = f.read(buffersize)
                if not data:
                    break

                logger.debug(
                    f"{self}: file transfer, step {msg_counter}/{max_num_steps}"
                )

                # Send the data
                self.socket.sendall(data)
                msg_counter += 1

        logger.debug(f"{self}: finished file transfer")

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
            self.join()
