# Built-in Imports
from typing import Literal, Callable, Dict, Any, List, Tuple
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
import pdb
from concurrent.futures import Future, ThreadPoolExecutor, wait

# Third-Party Imports

# Internal Imports
from .utils import (
    threaded,
    create_payload,
    get_open_port,
    get_ip_address,
)
from . import enums
from . import socket_handling as sh
from . import _logger

logger = _logger.getLogger("chimerapy-networking")


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

        # Create ThreadPoolExecutor for submitting sends and keep futures inside container
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.futures: List[Future] = []

        # Keeping track of client threads
        self.client_comms = {}

        # Adding file transfer capabilities
        self.tempfolder = pathlib.Path(tempfile.mkdtemp())
        self.handlers.update({enums.FILE_TRANSFER_START: self.receive_file})
        self.file_transfer_records = collections.defaultdict(dict)

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

    @threaded
    def client_comm_thread(self, s: socket.socket, addr):

        # Continue checking for messages from client until not running
        while self.is_running.is_set():

            # Monitor the socket
            try:
                success, msg = sh.monitor(self.name, s)
            except (ConnectionResetError, ConnectionAbortedError):
                break

            # Process the msg
            if success:
                self.process_msg(msg, s)

        # Then shutdown socket
        s.close()

    def process_msg(self, msg: Dict, s: socket.socket):

        # Check that it is from a client
        if msg["type"] == self.accepted_msg_type:

            # Handle ACK differently
            if msg["signal"] != enums.DATA_ACK:

                # If ACK requested, send it
                if msg["ack"]:

                    logger.debug(f"{self}: Sending ACK for {msg['uuid']}")
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

    def waiting_for_ack(
        self, s: socket.socket, msg_uuid: str, send_future: Future[Tuple[bool, str]]
    ) -> Tuple[bool, str]:

        # Check if the send message was successful
        success, p_msg_uuid = send_future.result()  # possible_msg_uuid

        # Handle different configurations of msg_uuid
        if msg_uuid == "":
            msg_uuid = p_msg_uuid

        # Wait for an ACK message that changes client's ACK status
        miss_counter = 0
        while self.is_running:
            time.sleep(0.1)
            if msg_uuid in self.client_comms[s]["acks"]:
                logger.debug(f"Server received ACK")
                break
            else:
                logger.debug(
                    f"Waiting for ACK for msg: {msg_uuid} from s: {s}, ack: {self.client_comms[s]['acks']}"
                )

                if miss_counter >= 20:
                    raise RuntimeError(f"Server ACK timeout for {msg_uuid}")

                miss_counter += 1

        return success, msg_uuid

    def send(
        self, s: socket.socket, msg: Dict, ack: bool = False
    ) -> Future[Tuple[bool, str]]:

        # Submit send request to the ThreadPoolExecutor
        send_future = self.executor.submit(
            sh.send,
            name=self.name,
            s=s,
            msg=msg,
            sender_msg_type=self.sender_msg_type,
            ack=ack,
        )

        # Store the future
        self.futures.append(send_future)

        # If ACK, add dependent job
        if ack:
            ack_future = self.executor.submit(self.waiting_for_ack, s, "", send_future)
            return ack_future

        else:
            return send_future

    def send_bytes(
        self, s: socket.socket, msg: bytes, msg_uuid: str, ack: bool = False
    ) -> Future[Tuple[bool, str]]:

        # Submit send request to the ThreadPoolExecutor
        send_future = self.executor.submit(
            sh.send_bytes,
            name=self.name,
            s=s,
            msg=msg,
        )

        # Store the future
        self.futures.append(send_future)

        # If ACK, add dependent job
        if ack:
            ack_future = self.executor.submit(
                self.waiting_for_ack, s, msg_uuid, send_future
            )
            return ack_future

        else:
            return send_future

    def broadcast(self, msg: Dict, ack: bool = False) -> List[Future]:

        # Create msg_uuid
        msg_uuid = str(uuid.uuid4())

        # First create payload and then use send it via all client sockets
        msg_bytes, msg_length = create_payload(
            type=self.sender_msg_type,
            signal=msg["signal"],
            data=msg["data"],
            provided_uuid=msg_uuid,
            ack=ack,
        )

        # Create total msg
        total_msg = msg_length + msg_bytes

        # Obtain the futures
        futures = [
            self.send_bytes(s, total_msg, msg_uuid, ack) for s in self.client_comms
        ]
        return futures

    def receive_file(self, msg: Dict[str, Any], client_socket: socket.socket):

        # Obtain the file
        success, sender_name, dst_filepath = sh.file_transfer_receive(
            name=self.name, s=client_socket, msg=msg, tempfolder=self.tempfolder
        )

        # Create a record of the files transferred and from whom
        if success:
            self.file_transfer_records[sender_name][dst_filepath.name] = dst_filepath

    def send_file(self, name: str, filepath: pathlib.Path):

        assert filepath.exists() and filepath.is_file()

        # Send the file to all client
        for s in self.client_comms:
            sh.send_file(
                sender_name=name,
                sender_msg_type=self.sender_msg_type,
                s=s,
                filepath=filepath,
                buffersize=4096,
            )

    def send_folder(self, name: str, folderpath: pathlib.Path):

        assert folderpath.exists() and folderpath.is_dir()

        for s in self.client_comms:

            sh.send_folder(
                name=name,
                s=s,
                dir=folderpath,
                tempfolder=self.tempfolder,
                sender_msg_type=self.sender_msg_type,
            )

    def move_transfer_files(self, dst: pathlib.Path, unzip: bool):

        for name, filepath_dict in self.file_transfer_records.items():
            # Create a folder for the name
            named_dst = dst / name
            os.mkdir(named_dst)

            # Move all the content inside
            for filename, filepath in filepath_dict.items():

                # If not unzip, just move it
                if not unzip:
                    shutil.move(filepath, named_dst / filename)

                # Otherwise, unzip, move content to the original folder,
                # and delete the zip file
                else:
                    shutil.unpack_archive(filepath, named_dst)

                    # Handling if temp folder includes a _ in the beginning
                    new_filename = filepath.stem
                    if new_filename[0] == "_":
                        new_filename = new_filename[1:]

                    new_file = named_dst / new_filename

                    # Wait until file is ready
                    miss_counter = 0
                    delay = 0.5
                    timeout = 10
                    while not new_file.exists():
                        time.sleep(delay)
                        miss_counter += 1
                        if timeout < delay * miss_counter:
                            raise TimeoutError(
                                f"File zip unpacking took too long! - {name}:{filepath}:{new_file}"
                            )

                    for file in new_file.iterdir():
                        shutil.move(file, named_dst)
                    shutil.rmtree(new_file)

    def shutdown(self):

        # First, send shutdown message to clients
        futures = []
        for client_socket in self.client_comms:
            try:
                futures.append(
                    self.send(client_socket, {"signal": enums.SHUTDOWN, "data": {}})
                )
            except socket.error:
                logger.debug("Socket send msg to broken pipe", exc_info=True)

        # Waiting for messages to be send to clients
        wait(futures)

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
