import traceback
import logging

from random import randint

from network_manager.network_node.propagation_strategy.propagation_strategy import PropagationStrategy
from network_manager.network_node.network_node import NetworkNode  # noqa: E402
from network_manager.network_node.network_node_idle_listener_interface import NetworkNodeIdleListenerInterface  # noqa: E402


"""
PropagationStrategyComparer

Class for investigating potential propagation strategy implementations.
The primary use case is to generate messages and propagate them
throughout the network. The comparer will then return information
detailing how the message was propagated throughout the network.
"""


class PropagationStrategyComparer(NetworkNodeIdleListenerInterface):
    def __init__(
        self,
        num_nodes: int,
        connectivity_percentage: int,
        num_messages: int,
        propagation_strategy: PropagationStrategy
    ):
        """
        __init__

        Creates a new PropagationStrategyComparer object.

        @param num_nodes [int] The number of nodes to use
        @param connectivity_percentage [int] The connectivity percentage of the network.
            0% = No nodes are connected
            50% = Every node is connected to half the nodes in the network
            100% = Every node is connected to every other node in the network
        @param num_messages [int] The number of messages to propagate throughout the network
        @param propagation_strategy [PropagationStrategy] The propagate strategy to use

        @return [PropagationStrategyComparer] The newly created PropagationStrategyComparer
        """
        NetworkNodeIdleListenerInterface.__init__(self)

        self.logger = logging.getLogger('NetworkNode')

        self.num_nodes = num_nodes
        self.connectivity_percentage = connectivity_percentage
        self.num_messages = num_messages
        self.propagation_strategy = propagation_strategy

        self.network_nodes = []

    def simulate_prop_strat(self, display_snapshot_info: bool) -> dict:
        """
        simulate_prop_strat

        Initializes a new network, then generates traffic throughout
        the network, and returns the propagation statistics.

        @param display_snapshot_info [bool] True if the propagation stats should be printed out. False otherwise

        @return [dict] The propagation statistics
        """
        data = None

        try:
            self._initialize_network()

            start_snapshot = self._get_state_snapshot()
            self._run_traffic(self.num_messages)
            end_snapshot = self._get_state_snapshot()

            data = self._compare_snapshots(start_snapshot, end_snapshot)
        except Exception:
            self.logger.exception(traceback.format_exc())

        for node in self.network_nodes:
            node.teardown()

        if display_snapshot_info:
            self._display_snapshot_info(data)

        return self.network_nodes, data

    def _display_snapshot_info(self, snapshot: dict) -> None:
        """
        _display_snapshot_info

        Prints out the given snapshot information to the log.

        @param snapshot [dict] The snapshot information to log

        @return None
        """
        for node_id, node_info in snapshot.items():
            self.logger.info("BOT_ID: {}".format(node_id))
            for key in node_info.keys():
                value = node_info[key]
                if isinstance(value, dict):
                    self.logger.info("\t{}".format(key))
                    for sub_key, sub_val in value.items():
                        self.logger.info("\t\t{}: {}".format(sub_key, sub_val))
                else:
                    self.logger.info("\t{}: {}".format(key, value))

    def _initialize_network(self) -> None:
        """
        _initialize_network

        Initializes a new network for the snapshot comparer. The
        number of nodes and configuration for the nodes is controlled
        by this objects parameters

        @param None

        @return None
        """
        self.network_nodes = []

        for _ in range(self.num_nodes):
            new_node = NetworkNode(additional_config_dict={"propagation_strategy": self.propagation_strategy})
            self.network_nodes.append(new_node)
            new_node.add_idle_listener(self)

        for i in range(len(self.network_nodes)):
            connected = [i]
            num_connections = (self.num_nodes - 1) * self.connectivity_percentage / 100.0
            while num_connections > 0:
                rand_node_ind = i
                while rand_node_ind in connected:
                    rand_node_ind = randint(0, self.num_nodes - 1)
                self.logger.debug("Connecting {} to {}".format(
                    self.network_nodes[i].get_id(),
                    self.network_nodes[rand_node_ind].get_id()
                ))
                self.network_nodes[i].connect_to_network_node(self.network_nodes[rand_node_ind])
                connected.append(rand_node_ind)
                num_connections -= 1
        self.wait_for_idle_network(60)

    def _run_traffic(self, num_messages: int) -> None:
        """
        _run_traffic

        Runs traffic through the network.

        @param num_messages [int] The number of messages to propagate throughout the network

        @return None
        """
        node_ind = 0
        while num_messages > 0:
            node = self.network_nodes[node_ind]
            msg_id = node.send_propagation_message("TEST", {})
            self.wait_for_idle_network(60)
            for node in self.network_nodes:
                assert(node.interacted_with_msg_with_id(msg_id))
            node_ind += 1
            node_ind %= len(self.network_nodes)
            num_messages -= 1

    def _get_state_snapshot(self) -> dict:
        """
        _get_state_snapshot

        Returns the current snapshot of the network containing
        the number of messages sent, received, and ignored by
        each node.

        @param None

        @return [dict<str, dict>] The snapshot information. The keys are the
            node IDs and the values are dicts with the node's information.
        """
        info_dict = {}
        for node in self.network_nodes:
            info_dict[node.get_id()] = {
                "SENT_MSGS": node.get_sent_messages(),
                "RCVD_MSGS": node.get_received_messages(),
                "NUM_IGNORED_MSGS": node.get_num_ignored_msgs()
            }
        return info_dict

    def _compare_snapshots(self, start_snapshot: dict, end_snapshot: dict) -> dict:
        """
        _compare_snapshots

        Compares the two given snapshots and returns the difference
        between the two.

        @param start_snapshot [dict<str, dict>] The starting snapshot
        @param end_snapshot [dict<str, dict>] The end snapshot

        @return [dict<str, dict>] The snapshot comparison information. The keys are the
            node IDs and the values are dicts with the node's information.
        """
        data = {}
        for node_id, node_info in start_snapshot.items():
            data[node_id] = {}
            for key in node_info.keys():
                end_val = end_snapshot[node_id][key]
                start_val = start_snapshot[node_id][key]

                if isinstance(end_snapshot[node_id][key], dict):
                    data[node_id][key] = {}
                    for msg_id in end_val.keys():
                        if msg_id not in start_val.keys():
                            data[node_id][key][msg_id] = end_val[msg_id]
                else:
                    data[node_id][key] = end_val - start_val
        return data
