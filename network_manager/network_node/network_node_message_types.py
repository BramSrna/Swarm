from enum import Enum


class NetworkNodeMessageTypes(Enum):
    MSG_RESPONSE = 1
    REQUEST_CONNECTION = 2
    ACCEPT_CONNECTION_REQUEST = 3
