from enum import Enum


class MessageTypes(Enum):
    NEW_SWARM_BOT_ID = 1
    FORWARD_MESSAGE = 2
    SYNC_INTERMEDIARIES = 3
    REQUEST_PATH_TO_BOT = 4
    MSG_RESPONSE = 5
