from random import choice

from network_manager.network_connectivity_level import NetworkConnectivityLevel
from network_manager.network_node.network_node import NetworkNode
from network_manager.network_node.network_node_idle_listener_interface import NetworkNodeIdleListenerInterface


"""
NetworkManager

Helper class for managing networks. Provides helpers
for adding and removing nodes to a network and ensures
the nodes are connected according to the specified connectivity level.
Also provides protection against orphaned nodes.
"""


class NetworkManager(NetworkNodeIdleListenerInterface):
    def __init__(self, network_connectivity_level: NetworkConnectivityLevel):
        """
        __init__

        Creates a new NetworkManager.

        @param network_connectivity_level [NetworkConnectivityLevel] The connectivity level to use for the network

        @return [NetworkManager] The newly created NetworkManager
        """
        NetworkNodeIdleListenerInterface.__init__(self)
        
        self.network_connectivity_level = network_connectivity_level

        self.central_network_node = None

        self.network_nodes = {}

    def startup(self):
        """
        startup

        Starts up each node in the network controlled by the manager.

        @param None

        @return None
        """
        for _, node in self.network_nodes.items():
            node.startup()

    def teardown(self) -> None:
        """
        teardown

        Tears down each node in the network controlled by the manager.

        @param None

        @return None
        """
        for _, node in self.network_nodes.items():
            node.teardown()

    def add_network_node(self, new_node: NetworkNode) -> None:
        """
        add_network_node

        Adds the given network node to the network controlled by the network

        @param new_node [NetworkNode] The node to add to the network

        @return None
        """
        if self.network_connectivity_level == NetworkConnectivityLevel.FULLY_CONNECTED:
            for node_id, node in self.network_nodes.items():
                node.connect_to_network_node(new_node)
                new_node.connect_to_network_node(node)
        elif self.network_connectivity_level == NetworkConnectivityLevel.PARTIALLY_CONNECTED:
            if len(self.network_nodes.keys()) > 0:
                connected_node_id, connected_node = choice(list(self.network_nodes.items()))
                connected_node.connect_to_network_node(new_node)
                new_node.connect_to_network_node(connected_node)
        elif self.network_connectivity_level == NetworkConnectivityLevel.CENTRALIZED:
            if len(self.network_nodes.keys()) == 0:
                self.central_network_node = new_node
            else:
                self.central_network_node.connect_to_network_node(new_node)
                new_node.connect_to_network_node(self.central_network_node)
        else:
            raise Exception("ERROR: unknown connectivity level: " + str(self.network_connectivity_level))

        new_id = new_node.get_id()
        if new_id not in self.network_nodes:
            self.network_nodes[new_id] = new_node
            
        new_node.add_idle_listener(self)

    def get_central_network_node(self) -> NetworkNode:
        """
        get_central_network_node

        Returns the central network node in the manager's network. If
        the connectivity level is not NetworkConnectivityLevel.CENTRALIZED,
        then None is returned

        @param None

        @return [NetworkNode] The central network node
        """
        return self.central_network_node

    def get_network_nodes(self) -> list:
        """
        get_network_nodes

        Returns a list of the nodes in the manager's network

        @param None

        @return [list] The nodes in the manager
        """
        return self.network_nodes

    def remove_network_node(self, id_to_remove: int) -> bool:
        """
        remove_network_node

        Removes the node with the given ID from the manager's network.
        If removing the node would result in orphaned nodes, then an
        exception is raised. If no nodes will be orphaned, then the
        node will be removed from the network.

        @param id_to_remove [int] The ID of the node to remove

        @return [bool] True if the node is removed successfully
        """
        if id_to_remove not in self.network_nodes:
            return True

        for node_id, node in self.network_nodes.items():
            connections = node.get_connections()
            if ((len(connections) == 1) and (connections[0] == id_to_remove)):
                raise Exception("ERROR: Removing node from network would leave an orphaned node: " + str(node_id))

        for node_id, node in self.network_nodes.items():
            connections = node.get_connections()
            if id_to_remove in connections:
                node.disconnect_from_network_node(id_to_remove)

        node_to_remove = self.network_nodes[id_to_remove]
        for node_id in node_to_remove.get_connections():
            node_to_remove.disconnect_from_network_node(node_id)

        return True
