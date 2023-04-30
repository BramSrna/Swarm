import logging
import unittest
import os
import yaml
import math

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from swarm.swarm_bot import SwarmBot


class TestSwarmMemory(NetworkNodeTestClass):
    def test_contents_written_to_one_table_are_unique_to_that_table(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)

        table_id_1 = "TABLE_ID_1"
        table_id_2 = "TABLE_ID_2"

        table_key_1 = "TABLE_KEY_1"

        table_val_1 = "TABLE_VAL_1"
        table_val_2 = "TABLE_VAL_2"

        test_swarm_bot_1.write_to_swarm_memory(table_id_1 + "/" + table_key_1, table_val_1)
        test_swarm_bot_1.write_to_swarm_memory(table_id_2 + "/" + table_key_1, table_val_2)

        self.assertEqual({table_key_1: table_val_1}, test_swarm_bot_1.read_from_swarm_memory(table_id_1))
        self.assertEqual({table_key_1: table_val_2}, test_swarm_bot_1.read_from_swarm_memory(table_id_2))

    def test_newly_added_bot_can_read_from_the_swarm(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)

        test_mem_id_1 = "TEST_ID_1"
        test_mem_val_1 = "TEST_VAL_1"

        test_mem_id_2 = "TEST_ID_2"
        test_mem_val_2 = "TEST_VAL_2"

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id_1, test_mem_val_1)

        test_swarm_bot_2.write_to_swarm_memory(test_mem_id_2, test_mem_val_2)

        self.wait_for_idle_network()
        self.assertEqual(None, test_swarm_bot_1.read_from_swarm_memory(test_mem_id_2))
        self.assertEqual(None, test_swarm_bot_2.read_from_swarm_memory(test_mem_id_1))

        self.assertEqual(test_mem_val_1, test_swarm_bot_1.read_from_swarm_memory(test_mem_id_1))
        self.assertEqual(test_mem_val_2, test_swarm_bot_2.read_from_swarm_memory(test_mem_id_2))

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)

        self.wait_for_idle_network()

        self.assertEqual(test_mem_val_2, test_swarm_bot_1.read_from_swarm_memory(test_mem_id_2))
        self.assertEqual(test_mem_val_1, test_swarm_bot_2.read_from_swarm_memory(test_mem_id_1))

        self.assertEqual(test_mem_val_1, test_swarm_bot_1.read_from_swarm_memory(test_mem_id_1))
        self.assertEqual(test_mem_val_2, test_swarm_bot_2.read_from_swarm_memory(test_mem_id_2))

    def test_swarm_memory_integrity_is_maintained_when_bots_leaves_the_network(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)

        test_mem_id_1 = "TEST_ID_1"
        test_mem_val_1 = "TEST_VAL_1"

        test_mem_id_2 = "TEST_ID_2"
        test_mem_val_2 = "TEST_VAL_2"

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id_1, test_mem_val_1)
        test_swarm_bot_2.write_to_swarm_memory(test_mem_id_2, test_mem_val_2)

        self.wait_for_idle_network()
        self.assertEqual(test_mem_val_1, test_swarm_bot_1.read_from_swarm_memory(test_mem_id_1))
        self.assertEqual(test_mem_val_2, test_swarm_bot_1.read_from_swarm_memory(test_mem_id_2))
        self.assertEqual(test_mem_val_1, test_swarm_bot_2.read_from_swarm_memory(test_mem_id_1))
        self.assertEqual(test_mem_val_2, test_swarm_bot_2.read_from_swarm_memory(test_mem_id_2))

        test_swarm_bot_2.teardown()

        self.wait_for_idle_network()
        self.assertEqual(test_mem_val_1, test_swarm_bot_1.read_from_swarm_memory(test_mem_id_1))
        self.assertEqual(test_mem_val_2, test_swarm_bot_1.read_from_swarm_memory(test_mem_id_2))

    def test_sm_contents_wont_change_when_solitary_bot_hits_the_optimization_threshold_is_hit(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)

        test_key = "ID_1"
        test_value = "VALUE_1"

        test_swarm_bot_1.write_to_swarm_memory(test_key, test_value)

        self.wait_for_idle_network()
        self.assertIn(test_key, test_swarm_bot_1.get_local_swarm_memory_contents())

        default_config = yaml.load(
            open(os.path.join(os.path.dirname(__file__), "../../swarm/default_swarm_bot_config.yml")),
            Loader=yaml.FullLoader
        )

        for _ in range(default_config["swarm_memory_optimization_operation_threshold"]):
            self.assertEqual(test_value, test_swarm_bot_1.read_from_swarm_memory(test_key))

        self.wait_for_idle_network()
        self.assertIn(test_key, test_swarm_bot_1.get_local_swarm_memory_contents())

    def test_swarm_memory_will_move_data_when_only_one_bot_is_using_the_data(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_3)

        test_key = "ID_1"
        test_value = "VALUE_1"

        test_swarm_bot_1.write_to_swarm_memory(test_key, test_value)

        self.wait_for_idle_network()
        self.assertIn(test_key, test_swarm_bot_1.get_local_swarm_memory_contents())
        self.assertNotIn(test_key, test_swarm_bot_2.get_local_swarm_memory_contents())
        self.assertNotIn(test_key, test_swarm_bot_3.get_local_swarm_memory_contents())

        default_config = yaml.load(
            open(os.path.join(os.path.dirname(__file__), "../../swarm/default_swarm_bot_config.yml")),
            Loader=yaml.FullLoader
        )

        for _ in range(default_config["swarm_memory_optimization_operation_threshold"]):
            self.assertEqual(test_value, test_swarm_bot_3.read_from_swarm_memory(test_key))

        self.wait_for_idle_network()
        self.assertIn(test_key, test_swarm_bot_1.get_local_swarm_memory_contents())
        self.assertNotIn(test_key, test_swarm_bot_2.get_local_swarm_memory_contents())
        self.assertIn(test_key, test_swarm_bot_3.get_local_swarm_memory_contents())

    def test_swarm_memory_will_duplicate_data_when_multiple_bots_are_accessing_it_frequently(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_3)

        test_key = "ID_1"
        test_value = "VALUE_1"

        test_swarm_bot_1.write_to_swarm_memory(test_key, test_value)

        self.wait_for_idle_network()
        self.assertIn(test_key, test_swarm_bot_1.get_local_swarm_memory_contents())
        self.assertNotIn(test_key, test_swarm_bot_2.get_local_swarm_memory_contents())
        self.assertNotIn(test_key, test_swarm_bot_3.get_local_swarm_memory_contents())

        default_config = yaml.load(
            open(os.path.join(os.path.dirname(__file__), "../../swarm/default_swarm_bot_config.yml")),
            Loader=yaml.FullLoader
        )

        optimization_threshold = default_config["swarm_memory_optimization_operation_threshold"]
        for _ in range(math.floor(optimization_threshold / 2)):
            self.assertEqual(test_value, test_swarm_bot_2.read_from_swarm_memory(test_key))
        for _ in range(math.ceil(optimization_threshold / 2)):
            self.assertEqual(test_value, test_swarm_bot_3.read_from_swarm_memory(test_key))

        self.wait_for_idle_network()
        self.assertIn(test_key, test_swarm_bot_1.get_local_swarm_memory_contents())
        self.assertIn(test_key, test_swarm_bot_2.get_local_swarm_memory_contents())
        self.assertIn(test_key, test_swarm_bot_3.get_local_swarm_memory_contents())


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
