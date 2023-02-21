from enum import Enum


class MessageTypes(Enum):
    SWARM_MEMORY_OBJECT_LOCATION = 1
    REQUEST_SWARM_MEMORY_READ = 2
    POP_FROM_SWARM_MEMORY = 3
    DELETE_FROM_SWARM_MEMORY = 4
    EXECUTION_GROUP_CREATION = 5
    REQUEST_JOIN_EXECUTION_GROUP = 6
    START_TASK_EXECUTION = 7
    EXECUTION_GROUP_TEARDOWN = 8
    TASK_OUTPUT = 9
    EXECUTION_GROUP_DELETION = 10
    RESPOND_TO_READ = 11
