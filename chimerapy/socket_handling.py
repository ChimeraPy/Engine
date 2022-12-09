from typing import Dict, Callable, Any, Tuple
import socket
import logging
import struct
import time
import uuid
import shutil
import pathlib
import os
import math

from . import enums
from .utils import decode_payload, create_payload, logging_tqdm
from . import _logger

logger = _logger.getLogger("chimerapy-networking")


def read_socket_buffer(s: socket.socket, total_size: int, buffer_size: int = 4096):

    # Container for the whole data
    data = b""

    # Use the length to get the entire message
    while len(data) < int(total_size):
        to_read = int(total_size) - len(data)

        while True:
            try:
                data += s.recv(buffer_size if to_read > buffer_size else to_read)
                break
            except socket.timeout:
                time.sleep(0.1)

    # Return the desired data
    return data


def monitor(
    name: str,
    s: socket.socket,
) -> Tuple[bool, Dict[str, Any]]:

    # Check if the socket is closed
    if s.fileno() == -1:
        raise ConnectionAbortedError

    # Get the header data (containing the size)
    try:
        bs = s.recv(8)
    except socket.timeout:
        return False, {}

    # No message
    if bs == b"":
        return False, {}

    # Get the length
    (length,) = struct.unpack(">Q", bs)

    # Get the body
    data = read_socket_buffer(s, length)

    # If end, stop it
    if data == b"":
        raise ConnectionAbortedError

    # Decode the message so we can process it
    msg = decode_payload(data)

    logger.debug(f"{name}: msg received, content: {msg['signal']}")

    return True, msg


def send(
    name: str,
    s: socket.socket,
    msg: Dict[str, Any],
    sender_msg_type: str,
    ack: bool = False,
):

    # Check first if the connection is alive
    if s.fileno() == -1:
        logger.debug(
            f"Tried to send {msg} to dead client connection name: socket: {s}."
        )
        return False, None

    # Create an uuid to track the message
    msg_uuid = str(uuid.uuid4())

    # Convert msg data to bytes
    msg_bytes, msg_length = create_payload(
        type=sender_msg_type,
        signal=msg["signal"],
        data=msg["data"],
        provided_uuid=msg_uuid,
        ack=ack,
    )

    # Sending message
    try:
        s.sendall(msg_length + msg_bytes)
        logger.debug(f"{name}: send {msg['signal']}")
    except socket.timeout:
        logger.debug(f"{name}: Socket Timeout: skipping")
        return False, None
    except:
        logger.debug(
            f"{name}: Broken Pipe Error, handled for {msg['signal']}", exc_info=True
        )
        return False, None

    return True, msg_uuid


def file_transfer_receive(
    name: str, msg: Dict, s: socket.socket, tempfolder: pathlib.Path
):

    logger.debug(f"{name}: just received file transfer initialization")

    # First, extract the filename and filesize
    sender_name = msg["data"]["name"]
    filename = msg["data"]["filename"]
    filesize = int(msg["data"]["filesize"])
    buffer_size = int(msg["data"]["buffersize"])
    max_num_steps = int(msg["data"]["max_num_steps"])

    # start receiving the file from the socket
    # and writing to the file stream
    total_bytes_read = 0
    progress = logging_tqdm(range(filesize))

    # Update the client_socket timeout
    delay = 1
    s.settimeout(delay)

    # Create the filepath to the tempfolder
    dst_filepath = tempfolder / filename
    logger.debug(f"{name}: ready for file transfer")

    # Having counter tracking number of messages
    msg_counter = 1

    # Compute the number of bytes left
    bytes_left = filesize - total_bytes_read

    with open(dst_filepath, "wb") as f:
        while total_bytes_read < filesize:

            # read 1024 bytes from the socket (receive)
            try:
                data = s.recv(buffer_size if bytes_left > buffer_size else bytes_left)
                total_bytes_read += len(data)
                bytes_left = filesize - total_bytes_read

                logger.debug(
                    f"{name}: file transfer, step {msg_counter}/{max_num_steps}"
                )
                msg_counter += 1

            except socket.timeout:
                time.sleep(0.5)
                continue

            # write to the file the bytes we just received
            f.write(data)

            # update the progress bar
            progress.update(len(data))

            # Safety check
            if msg_counter > max_num_steps * 2 + 5:
                raise TimeoutError("File transfer took longer than expected!")

    logger.debug(f"{name}: finished file transfer")

    return True, sender_name, dst_filepath


def send_file(
    sender_name: str,
    sender_msg_type: str,
    s: socket.socket,
    filepath: pathlib.Path,
    buffersize: int = 4096,
):

    # Get information about the filesize
    filesize = os.path.getsize(filepath)
    max_num_steps = math.ceil(filesize / buffersize)

    # Now start the process of sending content to the server
    # First, we send the message inciting file transfer
    init_msg = {
        "type": enums.WORKER_MESSAGE,
        "signal": enums.FILE_TRANSFER_START,
        "data": {
            "name": sender_name,
            "filename": filepath.name,
            "filesize": filesize,
            "buffersize": buffersize,
            "max_num_steps": max_num_steps,
        },
    }
    send(
        name=sender_name,
        s=s,
        msg=init_msg,
        sender_msg_type=sender_msg_type,
    )
    logger.debug(f"{sender_name}: sent file transfer initialization")

    # Having counter tracking number of messages
    msg_counter = 1

    # Modifying socket to longer
    delay = 1
    miss_counter = 0
    s.settimeout(delay)

    with open(filepath, "rb") as f:
        while True:

            # Read the data to be sent
            data = f.read(buffersize)
            if not data:
                break

            logger.debug(
                f"{sender_name}: file transfer, step {msg_counter}/{max_num_steps}"
            )

            # Continue trying to send data until accepted
            while True:
                try:
                    # Send the data
                    s.sendall(data)
                    msg_counter += 1
                    break

                except socket.timeout:
                    miss_counter += 1
                    time.sleep(0.5)

                    if miss_counter * delay > 100:
                        raise TimeoutError("Timeout multiple attemptindg sending data")

                    continue

    logger.debug(f"{sender_name}: finished file transfer")


def send_folder(
    name: str,
    s: socket.socket,
    dir: pathlib.Path,
    tempfolder: pathlib.Path,
    sender_msg_type: str,
    buffersize: int = 4096,
):

    assert (
        dir.is_dir() and dir.exists()
    ), f"Sending {dir} needs to be a folder that exists."

    # Having continuing trying to make zip folder
    miss_counter = 0
    delay = 1
    zip_timeout = 10

    # First, we need to archive the folder into a zip file
    while True:
        try:
            shutil.make_archive(str(dir), "zip", dir.parent, dir.name)
            break
        except:
            time.sleep(delay)
            miss_counter += 1

            if zip_timeout < delay * miss_counter:
                raise SystemError("Temp folder couldn't be zipped.")

    zip_file = dir.parent / f"{dir.name}.zip"

    # Relocate zip to the tempfolder
    temp_zip_file = tempfolder / f"_{zip_file.name}"
    shutil.move(zip_file, temp_zip_file)

    send_file(
        sender_name=name,
        sender_msg_type=sender_msg_type,
        s=s,
        filepath=temp_zip_file,
        buffersize=buffersize,
    )
