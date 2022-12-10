from enum import Enum

"""
NetworkConnectivityLevel

Enum for specifying different connectivity levels for the network.
"""


class NetworkConnectivityLevel(Enum):
    FULLY_CONNECTED = 1
    PARTIALLY_CONNECTED = 2
    CENTRALIZED = 3
