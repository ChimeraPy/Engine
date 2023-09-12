from enum import Enum

# Server <--> Client
class GENERAL_MESSAGE(Enum):  # Used only Client and Server
    SHUTDOWN = -1
    OK = 0
    FILE_TRANSFER_START = 1
    FILE_TRANSFER_END = 2
    CLIENT_REGISTER = 3


# Manager --> Front-End WS
class MANAGER_MESSAGE(Enum):
    NODE_STATUS_UPDATE = 4
    NETWORK_STATUS_UPDATE = 5


# Worker -> Node
class WORKER_MESSAGE(Enum):
    BROADCAST_NODE_SERVER = 20
    REQUEST_STEP = 21
    REQUEST_GATHER = 22
    REQUEST_COLLECT = 23
    START_NODES = 24
    RECORD_NODES = 25
    STOP_NODES = 26
    REQUEST_METHOD = 27
    DIAGNOSTICS = 28


# Node -> Worker
class NODE_MESSAGE(Enum):
    STATUS = 50
    REPORT_GATHER = 51
    REPORT_SAVING = 52
    REPORT_RESULTS = 53
    DIAGNOSTICS = 54
