from enum import Enum


class SwarmTaskMessageTypes(Enum):
    EXECUTION_GROUP_CREATION = 1
    REQUEST_JOIN_EXECUTION_GROUP = 2
    START_TASK_EXECUTION = 3
    TASK_OUTPUT = 4
    EXECUTION_GROUP_DELETION = 5