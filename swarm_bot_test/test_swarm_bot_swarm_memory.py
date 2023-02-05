import logging
import unittest
import time
import pytest

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from swarm.swarm_task.swarm_task import SwarmTask
from swarm.swarm_bot import SwarmBot


class SimpleTask(SwarmTask):
    def __init__(self):
        super().__init__()
        self.task_complete = False
        self.sleep_time = 3

    def is_complete(self):
        return self.task_complete

    def execute_task(self):
        time.sleep(self.sleep_time)
        self.task_complete = True


class TestSwarmBotSwarmMemory(NetworkNodeTestClass):
    def test_can_access_swarm_memory_when_info_stored_on_local_swarm_bot(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()

        test_mem_id = "TEST_ID"
        test_mem_val = "TEST_VAL"

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id, test_mem_val, "str")

        ret_val = test_swarm_bot_1.read_from_swarm_memory(test_mem_id)

        self.assertEqual(test_mem_val, ret_val)

    def test_can_access_swarm_memory_when_info_stored_on_directly_connected_swarm_bot(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_1)

        test_mem_id = "TEST_ID"
        test_mem_val = "TEST_VAL"

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id, test_mem_val, "str")

        self.wait_for_idle_network()

        ret_val = test_swarm_bot_2.read_from_swarm_memory(test_mem_id)

        self.wait_for_idle_network()

        self.assertEqual(test_mem_val, ret_val)

    @pytest.mark.skip(reason="Will be executable once https://github.com/users/BramSrna/projects/4?pane=issue&itemId=19697674 is finished.")
    def test_can_access_swarm_memory_when_info_stored_on_non_directly_connected_swarm_bot(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()
        test_swarm_bot_3.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_1)

        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_3)
        test_swarm_bot_3.connect_to_network_node(test_swarm_bot_2)

        test_mem_id = "TEST_ID"
        test_mem_val = "TEST_VAL"

        test_swarm_bot_1.write_to_swarm_memory(test_mem_id, test_mem_val, "str")

        self.wait_for_idle_network()

        ret_val = test_swarm_bot_3.read_from_swarm_memory(test_mem_id)

        self.wait_for_idle_network()

        self.assertEqual(test_mem_val, ret_val)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
