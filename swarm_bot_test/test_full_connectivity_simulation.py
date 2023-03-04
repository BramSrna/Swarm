import logging
import unittest

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from swarm.swarm_bot import SwarmBot


class TestFullConnectivitySimulation(NetworkNodeTestClass):
    def test_swarm_bots_that_are_directly_connected_can_still_exchange_messages(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()
        test_swarm_bot_3.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)

        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_3)

        self.wait_for_idle_network()

        msg_id = test_swarm_bot_1.send_directed_message(test_swarm_bot_2.get_id(), "TEST", {})

        self.wait_for_idle_network()

        self.assertTrue(test_swarm_bot_2.received_msg_with_id(msg_id))

    def test_swarm_bots_that_are_not_directly_connected_can_still_exchange_messages_short_distance(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()
        test_swarm_bot_3.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        self.wait_for_idle_network()

        swarm_bot_1_intermediaries = test_swarm_bot_1.get_msg_intermediaries()
        swarm_bot_2_intermediaries = test_swarm_bot_2.get_msg_intermediaries()

        self.assertEqual(swarm_bot_1_intermediaries[test_swarm_bot_2.get_id()]["INTERMEDIARY_ID"], test_swarm_bot_2.get_id())
        self.assertEqual(swarm_bot_2_intermediaries[test_swarm_bot_1.get_id()]["INTERMEDIARY_ID"], test_swarm_bot_1.get_id())

        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_3)
        self.wait_for_idle_network()

        swarm_bot_1_intermediaries = test_swarm_bot_1.get_msg_intermediaries()
        swarm_bot_2_intermediaries = test_swarm_bot_2.get_msg_intermediaries()
        swarm_bot_3_intermediaries = test_swarm_bot_3.get_msg_intermediaries()

        self.assertEqual(swarm_bot_1_intermediaries[test_swarm_bot_2.get_id()]["INTERMEDIARY_ID"], test_swarm_bot_2.get_id())
        self.assertEqual(swarm_bot_1_intermediaries[test_swarm_bot_3.get_id()]["INTERMEDIARY_ID"], test_swarm_bot_2.get_id())

        self.assertEqual(swarm_bot_2_intermediaries[test_swarm_bot_1.get_id()]["INTERMEDIARY_ID"], test_swarm_bot_1.get_id())
        self.assertEqual(swarm_bot_2_intermediaries[test_swarm_bot_3.get_id()]["INTERMEDIARY_ID"], test_swarm_bot_3.get_id())

        self.assertEqual(swarm_bot_3_intermediaries[test_swarm_bot_2.get_id()]["INTERMEDIARY_ID"], test_swarm_bot_2.get_id())
        self.assertEqual(swarm_bot_3_intermediaries[test_swarm_bot_1.get_id()]["INTERMEDIARY_ID"], test_swarm_bot_2.get_id())

        msg_id = test_swarm_bot_1.send_directed_message(test_swarm_bot_3.get_id(), "TEST", {})

        self.wait_for_idle_network()

        self.assertTrue(test_swarm_bot_3.received_msg_with_id(msg_id))

    def test_swarm_bots_that_are_not_directly_connected_can_still_exchange_messages_long_distance(self):
        num_bots = 5
        test_bots = []
        for _ in range(num_bots):
            new_bot = self.create_network_node(SwarmBot)
            new_bot.startup()
            if len(test_bots) > 0:
                new_bot.connect_to_network_node(test_bots[-1])
                self.wait_for_idle_network()
            test_bots.append(new_bot)

        self.wait_for_idle_network()

        for bot_1 in test_bots:
            intermediaries = bot_1.get_msg_intermediaries()
            for bot_2 in test_bots:
                if bot_1 != bot_2:
                    self.assertIn(bot_2.get_id(), intermediaries)

        msg_id = test_bots[0].send_directed_message(test_bots[-1].get_id(), "TEST", {})

        self.wait_for_idle_network()

        self.assertTrue(test_bots[-1].received_msg_with_id(msg_id))

    def test_can_propagate_messages_to_non_directly_connected_bots(self):
        num_bots = 5
        test_bots = []
        for _ in range(num_bots):
            new_bot = self.create_network_node(SwarmBot)
            new_bot.startup()
            if len(test_bots) > 0:
                new_bot.connect_to_network_node(test_bots[-1])
                self.wait_for_idle_network()
            test_bots.append(new_bot)

        self.wait_for_idle_network()

        for bot_1 in test_bots:
            intermediaries = bot_1.get_msg_intermediaries()
            for bot_2 in test_bots:
                if bot_1 != bot_2:
                    self.assertIn(bot_2.get_id(), intermediaries)

        for origin_bot in test_bots:
            msg_ids = origin_bot.send_propagation_message("TEST", {})
            self.wait_for_idle_network()
            for bot in test_bots:
                if bot != origin_bot:
                    rcvd_msgs = list(bot.get_received_messages().keys())
                    common_ids = list(set(msg_ids).intersection(rcvd_msgs))
                    self.assertEqual(1, len(common_ids))

    def test_shortest_message_intermediary_is_always_saved(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)
        test_swarm_bot_4 = self.create_network_node(SwarmBot)
        test_swarm_bot_5 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()
        test_swarm_bot_3.startup()
        test_swarm_bot_4.startup()
        test_swarm_bot_5.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        self.wait_for_idle_network()
        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_3)
        self.wait_for_idle_network()

        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_4)
        self.wait_for_idle_network()
        test_swarm_bot_3.connect_to_network_node(test_swarm_bot_4)
        self.wait_for_idle_network()

        test_swarm_bot_4.connect_to_network_node(test_swarm_bot_5)
        self.wait_for_idle_network()

        self.assertEqual(1, test_swarm_bot_1.get_num_jumps_to(test_swarm_bot_2.get_id()))
        self.assertEqual(1, test_swarm_bot_1.get_num_jumps_to(test_swarm_bot_3.get_id()))
        self.assertEqual(2, test_swarm_bot_1.get_num_jumps_to(test_swarm_bot_4.get_id()))
        self.assertEqual(3, test_swarm_bot_1.get_num_jumps_to(test_swarm_bot_5.get_id()))

        self.assertEqual(1, test_swarm_bot_2.get_num_jumps_to(test_swarm_bot_1.get_id()))
        self.assertEqual(2, test_swarm_bot_2.get_num_jumps_to(test_swarm_bot_3.get_id()))
        self.assertEqual(1, test_swarm_bot_2.get_num_jumps_to(test_swarm_bot_4.get_id()))
        self.assertEqual(2, test_swarm_bot_2.get_num_jumps_to(test_swarm_bot_5.get_id()))

        self.assertEqual(1, test_swarm_bot_3.get_num_jumps_to(test_swarm_bot_1.get_id()))
        self.assertEqual(2, test_swarm_bot_3.get_num_jumps_to(test_swarm_bot_2.get_id()))
        self.assertEqual(1, test_swarm_bot_3.get_num_jumps_to(test_swarm_bot_4.get_id()))
        self.assertEqual(2, test_swarm_bot_3.get_num_jumps_to(test_swarm_bot_5.get_id()))

        self.assertEqual(2, test_swarm_bot_4.get_num_jumps_to(test_swarm_bot_1.get_id()))
        self.assertEqual(1, test_swarm_bot_4.get_num_jumps_to(test_swarm_bot_2.get_id()))
        self.assertEqual(1, test_swarm_bot_4.get_num_jumps_to(test_swarm_bot_3.get_id()))
        self.assertEqual(1, test_swarm_bot_4.get_num_jumps_to(test_swarm_bot_5.get_id()))

        self.assertEqual(3, test_swarm_bot_5.get_num_jumps_to(test_swarm_bot_1.get_id()))
        self.assertEqual(2, test_swarm_bot_5.get_num_jumps_to(test_swarm_bot_2.get_id()))
        self.assertEqual(2, test_swarm_bot_5.get_num_jumps_to(test_swarm_bot_3.get_id()))
        self.assertEqual(1, test_swarm_bot_5.get_num_jumps_to(test_swarm_bot_4.get_id()))

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_4)
        self.wait_for_idle_network()

        self.assertEqual(1, test_swarm_bot_1.get_num_jumps_to(test_swarm_bot_2.get_id()))
        self.assertEqual(1, test_swarm_bot_1.get_num_jumps_to(test_swarm_bot_3.get_id()))
        self.assertEqual(1, test_swarm_bot_1.get_num_jumps_to(test_swarm_bot_4.get_id()))
        self.assertEqual(2, test_swarm_bot_1.get_num_jumps_to(test_swarm_bot_5.get_id()))

        self.assertEqual(1, test_swarm_bot_2.get_num_jumps_to(test_swarm_bot_1.get_id()))
        self.assertEqual(2, test_swarm_bot_2.get_num_jumps_to(test_swarm_bot_3.get_id()))
        self.assertEqual(1, test_swarm_bot_2.get_num_jumps_to(test_swarm_bot_4.get_id()))
        self.assertEqual(2, test_swarm_bot_2.get_num_jumps_to(test_swarm_bot_5.get_id()))

        self.assertEqual(1, test_swarm_bot_3.get_num_jumps_to(test_swarm_bot_1.get_id()))
        self.assertEqual(2, test_swarm_bot_3.get_num_jumps_to(test_swarm_bot_2.get_id()))
        self.assertEqual(1, test_swarm_bot_3.get_num_jumps_to(test_swarm_bot_4.get_id()))
        self.assertEqual(2, test_swarm_bot_3.get_num_jumps_to(test_swarm_bot_5.get_id()))

        self.assertEqual(1, test_swarm_bot_4.get_num_jumps_to(test_swarm_bot_1.get_id()))
        self.assertEqual(1, test_swarm_bot_4.get_num_jumps_to(test_swarm_bot_2.get_id()))
        self.assertEqual(1, test_swarm_bot_4.get_num_jumps_to(test_swarm_bot_3.get_id()))
        self.assertEqual(1, test_swarm_bot_4.get_num_jumps_to(test_swarm_bot_5.get_id()))

        self.assertEqual(2, test_swarm_bot_5.get_num_jumps_to(test_swarm_bot_1.get_id()))
        self.assertEqual(2, test_swarm_bot_5.get_num_jumps_to(test_swarm_bot_2.get_id()))
        self.assertEqual(2, test_swarm_bot_5.get_num_jumps_to(test_swarm_bot_3.get_id()))
        self.assertEqual(1, test_swarm_bot_5.get_num_jumps_to(test_swarm_bot_4.get_id()))

    def test_simulated_full_connectivity_is_maintained_when_bot_is_removed_from_the_swarm(self):
        test_bots = [
            self.create_network_node(SwarmBot),
            self.create_network_node(SwarmBot),
            self.create_network_node(SwarmBot),
            self.create_network_node(SwarmBot)
        ]

        test_bots[0].startup()
        test_bots[1].startup()
        test_bots[2].startup()
        test_bots[3].startup()

        test_bots[0].connect_to_network_node(test_bots[1])
        self.wait_for_idle_network()
        test_bots[0].connect_to_network_node(test_bots[2])
        self.wait_for_idle_network()
        test_bots[1].connect_to_network_node(test_bots[3])
        self.wait_for_idle_network()
        test_bots[2].connect_to_network_node(test_bots[3])
        self.wait_for_idle_network()

        for origin_bot in test_bots:
            msg_ids = origin_bot.send_propagation_message("TEST", {})
            self.wait_for_idle_network()
            for bot in test_bots:
                if bot != origin_bot:
                    rcvd_msgs = list(bot.get_received_messages().keys())
                    common_ids = list(set(msg_ids).intersection(rcvd_msgs))
                    self.assertEqual(1, len(common_ids))

        test_bots[1].teardown()
        test_bots.pop(1)
        self.wait_for_idle_network()

        self.assertEqual(3, len(test_bots))

        self.assertEqual(1, test_bots[0].get_num_jumps_to(test_bots[1].get_id()))
        self.assertEqual(2, test_bots[0].get_num_jumps_to(test_bots[2].get_id()))

        self.assertEqual(1, test_bots[1].get_num_jumps_to(test_bots[0].get_id()))
        self.assertEqual(1, test_bots[1].get_num_jumps_to(test_bots[2].get_id()))

        self.assertEqual(2, test_bots[2].get_num_jumps_to(test_bots[0].get_id()))
        self.assertEqual(1, test_bots[2].get_num_jumps_to(test_bots[1].get_id()))

        for origin_bot in test_bots:
            print(origin_bot.get_id(), origin_bot.get_msg_intermediaries())
            msg_ids = origin_bot.send_propagation_message("TEST", {})
            self.wait_for_idle_network()
            for bot in test_bots:
                if bot != origin_bot:
                    rcvd_msgs = list(bot.get_received_messages().keys())
                    common_ids = list(set(msg_ids).intersection(rcvd_msgs))
                    self.assertEqual(1, len(common_ids))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
