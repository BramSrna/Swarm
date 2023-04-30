import logging
import unittest

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from swarm.swarm_bot import SwarmBot


class TestSwarmBotMessaging(NetworkNodeTestClass):
    def test_swarm_bot_can_send_asynchronous_messages_between_directly_connected_bots(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)

        msg_id = test_swarm_bot_1.send_directed_message(test_swarm_bot_2.get_id(), "TEST", {})

        self.wait_for_idle_network()
        self.assertTrue(test_swarm_bot_2.received_msg_with_id(msg_id))

    def test_error_raised_when_sync_message_does_not_receive_response_within_the_time_limit_directly_connected_bots(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)

        self.wait_for_idle_network()

        with self.assertRaises(Exception) as raised_error:
            test_swarm_bot_1.send_sync_directed_message(test_swarm_bot_2.get_id(), "TEST", {})

        self.assertIn("Did not receive message response within time limit", str(raised_error.exception))

    def test_swarm_bot_can_send_asynchronous_messages_between_non_directly_connected_bots(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_3)

        self.wait_for_idle_network()
        msg_id = test_swarm_bot_1.send_directed_message(test_swarm_bot_3.get_id(), "TEST", {})

        self.wait_for_idle_network()
        self.assertTrue(test_swarm_bot_3.received_msg_with_id(msg_id))

    def test_error_raised_when_sync_message_does_not_receive_response_within_the_time_limit_non_directly_connected_bots(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_3)

        self.wait_for_idle_network()

        with self.assertRaises(Exception) as raised_error:
            test_swarm_bot_1.send_sync_directed_message(test_swarm_bot_3.get_id(), "TEST", {})

        self.assertIn("Did not receive message response within time limit", str(raised_error.exception))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
