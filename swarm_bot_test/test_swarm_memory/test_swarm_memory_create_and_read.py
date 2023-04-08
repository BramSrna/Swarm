import logging
import unittest
import yaml
import os

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from swarm.swarm_bot import SwarmBot


class TestSwarmMemoryCreateAndRead(NetworkNodeTestClass):
    def test_can_read_swarm_memory_when_info_stored_on_local_swarm_bot(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()

        test_path = "str"
        test_mem_val = "TEST_VAL"

        test_swarm_bot_1.create_swarm_memory_entry(test_path, test_mem_val)

        self.assertEqual(test_mem_val, test_swarm_bot_1.read_from_swarm_memory(test_path))

    def test_can_read_swarm_memory_when_info_stored_on_directly_connected_swarm_bot(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        self.wait_for_idle_network()

        test_mem_id = "TEST_ID"
        test_mem_val = "TEST_VAL"

        test_swarm_bot_1.create_swarm_memory_entry(test_mem_id, test_mem_val)
        self.wait_for_idle_network()

        self.assertEqual(test_mem_val, test_swarm_bot_2.read_from_swarm_memory(test_mem_id))

    def test_can_read_swarm_memory_when_info_stored_on_non_directly_connected_swarm_bot(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()
        test_swarm_bot_3.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_3)
        self.wait_for_idle_network()

        test_mem_id = "TEST_ID"
        test_mem_val = "TEST_VAL"

        test_swarm_bot_1.create_swarm_memory_entry(test_mem_id, test_mem_val)
        self.wait_for_idle_network()

        self.assertEqual(test_mem_val, test_swarm_bot_3.read_from_swarm_memory(test_mem_id))

    def test_read_will_only_return_one_result_when_multiple_copies_of_the_same_data_exist(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()
        test_swarm_bot_3.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_3)
        self.wait_for_idle_network()

        child_path = "CHILD_PATH"
        test_val = "CHILD_VAL"

        test_swarm_bot_2.create_swarm_memory_entry(child_path, test_val)
        test_swarm_bot_3.create_swarm_memory_entry(child_path, test_val)
        self.wait_for_idle_network()

        self.assertEqual(test_val, test_swarm_bot_1.read_from_swarm_memory(child_path))

    def test_create_will_update_other_copies_of_information_in_the_swarm(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()
        test_swarm_bot_3.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_3)
        self.wait_for_idle_network()

        child_path = "CHILD_PATH"
        test_val = "CHILD_VAL"

        test_swarm_bot_1.create_swarm_memory_entry(child_path, test_val + "_START")
        self.wait_for_idle_network()
        test_swarm_bot_2.create_swarm_memory_entry(child_path, test_val + "_MIDDLE")
        self.wait_for_idle_network()
        test_swarm_bot_3.create_swarm_memory_entry(child_path, test_val + "_END")
        self.wait_for_idle_network()

        self.assertEqual(test_val + "_END", test_swarm_bot_1.read_from_swarm_memory(child_path))
        self.assertEqual(test_val + "_END", test_swarm_bot_2.read_from_swarm_memory(child_path))
        self.assertEqual(test_val + "_END", test_swarm_bot_3.read_from_swarm_memory(child_path))

    def test_read_will_gather_all_indepedent_items_when_reading_seperated_path(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()
        test_swarm_bot_3.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_3)
        self.wait_for_idle_network()

        parent_path = "PARENT_PATH"

        child_path = "CHILD_PATH"
        test_val = "CHILD_VAL"

        test_swarm_bot_1.create_swarm_memory_entry(parent_path + "/" + child_path + "_1", test_val + "_1")
        test_swarm_bot_2.create_swarm_memory_entry(parent_path + "/" + child_path + "_2", test_val + "_2")
        test_swarm_bot_3.create_swarm_memory_entry(parent_path + "/" + child_path + "_3", test_val + "_3")
        self.wait_for_idle_network()

        expected_dict = {
            child_path + "_1": test_val + "_1",
            child_path + "_2": test_val + "_2",
            child_path + "_3": test_val + "_3"
        }

        self.assertEqual(expected_dict, test_swarm_bot_1.read_from_swarm_memory(parent_path))

    def test_swarm_bot_will_send_data_to_other_bots_when_local_swarm_memory_is_full(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        self.wait_for_idle_network()

        default_config = yaml.load(
            open(os.path.join(os.path.dirname(__file__), "../../swarm/default_swarm_bot_config.yml")),
            Loader=yaml.FullLoader
        )

        test_key = "ID_"
        test_value = "VALUE_"

        swarm_memory_key_count_threshold = default_config["swarm_memory_key_count_threshold"]
        for i in range(swarm_memory_key_count_threshold):
            test_swarm_bot_1.create_swarm_memory_entry(test_key + str(i), test_value + str(i))
        self.wait_for_idle_network()

        for i in range(swarm_memory_key_count_threshold):
            self.assertIn(test_key + str(i), test_swarm_bot_1.get_local_swarm_memory_contents())
            self.assertNotIn(test_key + str(i), test_swarm_bot_2.get_local_swarm_memory_contents())

        for i in range(swarm_memory_key_count_threshold - 1):
            self.assertEqual(test_value + str(i), test_swarm_bot_2.read_from_swarm_memory(test_key + str(i)))

        test_swarm_bot_1.create_swarm_memory_entry(
            test_key + str(swarm_memory_key_count_threshold),
            test_value + str(swarm_memory_key_count_threshold)
        )
        self.wait_for_idle_network()

        for i in range(swarm_memory_key_count_threshold - 1):
            self.assertIn(test_key + str(i), test_swarm_bot_1.get_local_swarm_memory_contents())
            self.assertNotIn(test_key + str(i), test_swarm_bot_2.get_local_swarm_memory_contents())

        self.assertIn(test_key + str(swarm_memory_key_count_threshold), test_swarm_bot_1.get_local_swarm_memory_contents())
        self.assertIn(test_key + str(swarm_memory_key_count_threshold - 1), test_swarm_bot_2.get_local_swarm_memory_contents())


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
