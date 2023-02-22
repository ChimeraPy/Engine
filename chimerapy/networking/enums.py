from enum import Enum

# Server <--> Client
class GENERAL_MESSAGE(Enum):  # Used only Client and Server
    SHUTDOWN = -1
    OK = 0
    FILE_TRANSFER_START = 1
    FILE_TRANSFER_END = 2
    CLIENT_REGISTER = 3


class WORKER_MESSAGE(Enum):
    BROADCAST_NODE_SERVER_DATA = 21
    REQUEST_STEP = 24
    REQUEST_GATHER = 25
    REQUEST_SAVING = 26
    START_NODES = 29
    STOP_NODES = 30


# Node -> Worker
class NODE_MESSAGE(Enum):
    STATUS = 32
    REPORT_GATHER = 33
    REPORT_SAVING = 34
