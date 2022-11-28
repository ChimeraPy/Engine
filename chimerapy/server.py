# Built-in Imports
from typing import Literal, Callable, Dict, Any
import logging
import collections
import uuid
import struct
import socket
import threading
import time
import pathlib
import os
import tempfile
import shutil

# Third-Party Imports
import tqdm

# Internal Imports
from .utils import (
    threaded,
    create_payload,
    get_open_port,
    decode_payload,
    get_ip_address,
)
from . import enums

logger = logging.getLogger("chimerapy")


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

        # State variables
        self.has_shutdown = False

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

        # Adding file transfer capabilities
        self.tempfolder = pathlib.Path(tempfile.mkdtemp())
        self.handlers.update({enums.FILE_TRANSFER_START: self.file_receive})
        self.file_transfer_records = collections.defaultdict(list)

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
            logger.debug(f"{self}: Got connection from {addr}")
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

            logger.debug(f"{self} msg received, content: {msg['signal']}")

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

    def file_receive(self, msg: Dict[str, Any], client_socket: socket.socket):

        logger.debug(f"{self}: just received file transfer initialization")

        # First, extract the filename and filesize
        name = msg["data"]["name"]
        filename = msg["data"]["filename"]
        filesize = int(msg["data"]["filesize"])
        buffer_size = int(msg["data"]["buffersize"])
        max_num_steps = int(msg["data"]["max_num_steps"])

        # start receiving the file from the socket
        # and writing to the file stream
        total_bytes_read = 0
        progress = tqdm.tqdm(
            range(filesize),
            f"Receiving {filename}",
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
        )

        # Create the filepath to the tempfolder
        dst_filepath = self.tempfolder / filename

        logger.debug(f"{self}: ready for file transfer")

        # Having counter tracking number of messages
        msg_counter = 1

        # Compute the number of bytes left
        bytes_left = filesize - total_bytes_read

        with open(dst_filepath, "wb") as f:
            while total_bytes_read < filesize:

                # read 1024 bytes from the socket (receive)
                try:
                    data = client_socket.recv(
                        buffer_size if bytes_left > buffer_size else bytes_left
                    )
                    total_bytes_read += len(data)
                    bytes_left = filesize - total_bytes_read
                    logger.debug(
                        f"{self}: file transfer, step {msg_counter}/{max_num_steps}"
                    )
                    msg_counter += 1
                except socket.timeout:
                    time.sleep(0.1)
                    continue

                # write to the file the bytes we just received
                f.write(data)

                # update the progress bar
                progress.update(len(data))

                # Safety check
                if msg_counter > max_num_steps + 5:
                    raise RuntimeError("File transfer took longer than expected!")

        logger.debug(f"{self}: finished file transfer")

        # Create a record of the files transferred and from whom
        self.file_transfer_records[name].append(dst_filepath)

    def move_transfer_files(self, dst: pathlib.Path, unzip: bool):

        for name, filepath_list in self.file_transfer_records.items():
            # Create a folder for the name
            named_dst = dst / name
            os.mkdir(named_dst)

            # Move all the content inside
            for filepath in filepath_list:
                # If not unzip, just move it
                if not unzip:
                    shutil.move(filepath, named_dst / filepath.name)

                # Otherwise, unzip, move content to the original folder,
                # and delete the zip file
                else:
                    shutil.unpack_archive(filepath, named_dst)
                    new_file = named_dst / filepath.stem
                    for file in new_file.iterdir():
                        shutil.move(file, named_dst)
                    shutil.rmtree(new_file)

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
