from typing import Any, Dict, Tuple, Optional
import socket
import queue
import errno
import datetime
import threading
import logging
import functools
import struct

import jsonpickle

# import lz4.block
import gzip

logger = logging.getLogger("chimerapy")


def threaded(fn):
    """Decorator for class methods to be spawn new thread.

    From: https://stackoverflow.com/a/19846691/13231446
    Args:
        fn: The method of a class.
    """

    def wrapper(*args, **kwargs):
        thread = threading.Thread(target=fn, args=args, kwargs=kwargs)
        thread.deamon = True
        return thread

    return wrapper


def log(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        args_repr = [repr(a) for a in args]
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
        signature = ", ".join(args_repr + kwargs_repr)
        # logger.debug(f"function {func.__name__} called with args {signature}")
        logger.debug(f"{args_repr[0]}: function {func.__name__}")
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            logger.exception(
                f"Exception raised in {func.__name__}. exception: {str(e)}"
            )
            raise e

    return wrapper


def get_open_port(host: str, start_port: int) -> Tuple[socket.socket, int]:

    # Creating socket to connect
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    offset = 0

    while True:
        try:
            current_attempt_port = start_port + offset
            s.bind((host, current_attempt_port))
            break
        except socket.error as e:
            offset += 10
            if e.errno == errno.EADDRINUSE:
                logger.debug(f"Port {current_attempt_port} is already in use.")
            else:
                logger.error("Unknown socket error", exc_info=True)
                raise e

    return s, current_attempt_port


def create_payload(
    data: Any,
    type: str,
    signal: str,
    provided_uuid: str,
    ack: bool,
    timestamp: datetime.timedelta = datetime.timedelta(),
) -> Tuple[bytes, bytes]:

    payload = {
        "type": type,
        "signal": signal,
        "timestamp": str(timestamp),
        "uuid": provided_uuid,
        "data": data,
        "ack": int(ack),
    }

    jsonpickle_payload = jsonpickle.encode(payload)
    # compressed_bytes_payload = lz4.block.compress(jsonpickle_payload.encode(), mode="fast")
    compressed_bytes_payload = gzip.compress(jsonpickle_payload.encode())
    decompressed_bytes_payload = gzip.decompress(compressed_bytes_payload)
    assert decompressed_bytes_payload == jsonpickle_payload.encode()

    finished_payload = compressed_bytes_payload
    # finished_payload = jsonpickle_payload.encode()

    return finished_payload, struct.pack(">Q", len(finished_payload))


def decode_payload(data: bytes) -> Dict:

    # bytes_payload = lz4.block.decompress(data)
    bytes_payload = gzip.decompress(data)
    payload: Dict = jsonpickle.decode(bytes_payload)

    # payload: Dict = jsonpickle.decode(data)

    return payload


def clear_queue(input_queue: queue.Queue):
    """Clear a queue.
    Args:
        input_queue (queue.Queue): Queue to be cleared.
    """

    while input_queue.qsize() != 0:
        logger.debug(f"size={input_queue.qsize()}")
        # Make sure to account for possible atomic modification of the
        # queue
        try:
            data = input_queue.get(timeout=0.1, block=False)
            del data
        except queue.Empty:
            logger.debug(f"clear_queue: empty!")
            return
        except EOFError:
            logger.warning("Queue EOFError --- data corruption")
            return
