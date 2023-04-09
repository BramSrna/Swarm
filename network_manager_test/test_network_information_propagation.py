import logging
import unittest

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from network_manager_test.propagation_strategy_comparer import PropagationStrategyComparer
from network_manager.network_node.network_node import NetworkNode


class TestNetworkInformationPropagation(NetworkNodeTestClass):
    def test_all_nodes_in_the_network_receive_a_sent_message_when_naive_propagation_is_used_in_double_layer_network(self):
        test_network_node_1 = self.create_network_node(
            NetworkNode,
            additional_config_dict={"propagation_strategy": "NaivePropagation"}
        )
        test_network_node_2 = self.create_network_node(
            NetworkNode,
            additional_config_dict={"propagation_strategy": "NaivePropagation"}
        )
        test_network_node_3 = self.create_network_node(
            NetworkNode,
            additional_config_dict={"propagation_strategy": "NaivePropagation"}
        )

        test_network_node_1.connect_to_network_node(test_network_node_2)
        test_network_node_1.connect_to_network_node(test_network_node_3)

        self.wait_for_idle_network()

        msg_id = test_network_node_1.send_propagation_message("TEST", {})

        self.wait_for_idle_network()

        self.assertTrue(test_network_node_2.received_msg_with_id(msg_id))
        self.assertTrue(test_network_node_3.received_msg_with_id(msg_id))

    def test_all_nodes_in_the_network_receive_a_sent_message_when_naive_propagation_is_used_in_multi_layer_network(self):
        test_network_node_1 = self.create_network_node(
            NetworkNode,
            additional_config_dict={"propagation_strategy": "NaivePropagation"}
        )
        test_network_node_2 = self.create_network_node(
            NetworkNode,
            additional_config_dict={"propagation_strategy": "NaivePropagation"}
        )
        test_network_node_3 = self.create_network_node(
            NetworkNode,
            additional_config_dict={"propagation_strategy": "NaivePropagation"}
        )
        test_network_node_4 = self.create_network_node(
            NetworkNode,
            additional_config_dict={"propagation_strategy": "NaivePropagation"}
        )
        test_network_node_5 = self.create_network_node(
            NetworkNode,
            additional_config_dict={"propagation_strategy": "NaivePropagation"}
        )
        test_network_node_6 = self.create_network_node(
            NetworkNode,
            additional_config_dict={"propagation_strategy": "NaivePropagation"}
        )
        test_network_node_7 = self.create_network_node(
            NetworkNode,
            additional_config_dict={"propagation_strategy": "NaivePropagation"}
        )

        test_network_node_1.connect_to_network_node(test_network_node_2)
        test_network_node_1.connect_to_network_node(test_network_node_3)

        test_network_node_2.connect_to_network_node(test_network_node_4)
        test_network_node_2.connect_to_network_node(test_network_node_5)

        test_network_node_3.connect_to_network_node(test_network_node_6)
        test_network_node_3.connect_to_network_node(test_network_node_7)

        self.wait_for_idle_network()

        msg_id = test_network_node_1.send_propagation_message("TEST", {})

        self.wait_for_idle_network()

        self.assertTrue(test_network_node_2.received_msg_with_id(msg_id))
        self.assertTrue(test_network_node_3.received_msg_with_id(msg_id))

        self.assertTrue(test_network_node_4.received_msg_with_id(msg_id))
        self.assertTrue(test_network_node_5.received_msg_with_id(msg_id))

        self.assertTrue(test_network_node_6.received_msg_with_id(msg_id))
        self.assertTrue(test_network_node_7.received_msg_with_id(msg_id))

    def test_all_nodes_in_the_network_receive_a_sent_message_when_naive_propagation_is_used_in_circular_network(self):
        test_network_node_1 = self.create_network_node(
            NetworkNode,
            additional_config_dict={"propagation_strategy": "NaivePropagation"}
        )
        test_network_node_2 = self.create_network_node(
            NetworkNode,
            additional_config_dict={"propagation_strategy": "NaivePropagation"}
        )
        test_network_node_3 = self.create_network_node(
            NetworkNode,
            additional_config_dict={"propagation_strategy": "NaivePropagation"}
        )
        test_network_node_4 = self.create_network_node(
            NetworkNode,
            additional_config_dict={"propagation_strategy": "NaivePropagation"}
        )
        test_network_node_5 = self.create_network_node(
            NetworkNode,
            additional_config_dict={"propagation_strategy": "NaivePropagation"}
        )

        test_network_node_1.connect_to_network_node(test_network_node_2)
        test_network_node_2.connect_to_network_node(test_network_node_3)
        test_network_node_3.connect_to_network_node(test_network_node_4)
        test_network_node_4.connect_to_network_node(test_network_node_5)
        test_network_node_5.connect_to_network_node(test_network_node_1)

        self.wait_for_idle_network()

        msg_id = test_network_node_1.send_propagation_message("TEST", {})

        self.wait_for_idle_network()

        self.assertTrue(test_network_node_2.received_msg_with_id(msg_id))
        self.assertTrue(test_network_node_3.received_msg_with_id(msg_id))
        self.assertTrue(test_network_node_4.received_msg_with_id(msg_id))
        self.assertTrue(test_network_node_5.received_msg_with_id(msg_id))

    def test_naive_propagation_is_better_than_worst_case_implementation(self):
        num_nodes = 3
        connectivity_percentage = 100
        num_messages = 1

        comparer = PropagationStrategyComparer(num_nodes, connectivity_percentage, num_messages, "NaivePropagation")
        nodes, test_output = comparer.simulate_prop_strat(False)

        self.assertEqual(num_nodes, len(test_output.keys()))

        ignoring_n_minus_one = 0
        ignoring_n_minus_two = 0
        ignoring_else = 0

        for _, node_info in test_output.items():
            total_sent_msgs = 0
            total_rcvd_msgs = 0

            for _, msg_info in node_info["SENT_MSGS"].items():
                total_sent_msgs += msg_info[1]
            for _, msg_info in node_info["RCVD_MSGS"].items():
                total_rcvd_msgs += msg_info[1]

            self.assertEqual(num_nodes - 1, total_sent_msgs)
            self.assertEqual(num_nodes - 1, total_rcvd_msgs)

            num_ignored = node_info["NUM_IGNORED_MSGS"]
            if num_ignored == num_nodes - 1:
                ignoring_n_minus_one += 1
            elif num_ignored == num_nodes - 2:
                ignoring_n_minus_two += 1
            else:
                ignoring_else += 1

        # Only the source node should ignore n - 1 messages
        self.assertEqual(1, ignoring_n_minus_one)

        # All receiver nodes should ignore n - 2 messages
        self.assertEqual(num_nodes - 1, ignoring_n_minus_two)

        self.assertEqual(0, ignoring_else)

    def test_smart_propagation_is_better_than_worst_case_implementation(self):
        num_nodes = 3
        connectivity_percentage = 100
        num_messages = 1

        comparer = PropagationStrategyComparer(num_nodes, connectivity_percentage, num_messages, "SmartPropagation")
        nodes, test_output = comparer.simulate_prop_strat(False)

        self.assertEqual(num_nodes, len(test_output.keys()))

        total_sent_msgs = 0
        total_rcvd_msgs = 0
        total_ignored_msgs = 0

        for _, node_info in test_output.items():
            for _, msg_info in node_info["SENT_MSGS"].items():
                total_sent_msgs += msg_info[1]
            for _, msg_info in node_info["RCVD_MSGS"].items():
                total_rcvd_msgs += msg_info[1]

            total_ignored_msgs += node_info["NUM_IGNORED_MSGS"]

        # Source node should send a message to every node except itself
        # Receiver nodes should send a message to every node except itself and the node it received the message from
        expected_total_sent_msgs = (num_nodes - 1) + (num_nodes - 1) * (num_nodes - 2)

        expected_total_rcvd_msgs = expected_total_sent_msgs

        # Source node should ignore all msgs
        # Receiver nodes should ignore all messages except one
        expected_total_ignored_msgs = expected_total_rcvd_msgs - num_nodes + 1

        self.assertEqual(expected_total_ignored_msgs, total_ignored_msgs)
        self.assertEqual(expected_total_rcvd_msgs, total_rcvd_msgs)
        self.assertEqual(expected_total_sent_msgs, total_sent_msgs)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
