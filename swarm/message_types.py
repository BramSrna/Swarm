from enum import Enum

class MessageTypes(Enum):
    NEW_TASK = 1
    REQUEST_TASK_TRANSFER = 2
    TASK_TRANSFER = 3