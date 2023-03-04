import logging
import unittest
import time

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from swarm.swarm_task.swarm_task import SwarmTask
from swarm.swarm_bot import SwarmBot
from swarm.swarm_task.swarm_task_bundle import SwarmTaskBundle


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

    def get_task_output(self):
        return self.sleep_time


class TestSwarmBotTaskInbox(NetworkNodeTestClass):
    def test_incoming_task_bundle_will_be_stored_in_swarm_memory(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()
        test_swarm_bot_3.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_3)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_3)

        self.wait_for_idle_network()

        test_swarm_bot_1.set_task_executor_status(False)
        test_swarm_bot_2.set_task_executor_status(False)
        test_swarm_bot_3.set_task_executor_status(False)

        test_task_bundle = SwarmTaskBundle()
        test_task_bundle.add_task(SimpleTask, 1)

        check_val = test_swarm_bot_1.receive_task_bundle(test_task_bundle)
        self.assertTrue(check_val)

        self.wait_for_idle_network()

        task = test_task_bundle.get_tasks()[0]

        bot_2_ret_val = test_swarm_bot_2.read_from_swarm_memory(task.get_id())

        self.wait_for_idle_network()

        bot_2_ret_val = test_swarm_bot_2.read_from_swarm_memory(task.get_id())

        self.assertEqual(task, bot_2_ret_val["TASK"])

    def test_task_bundle_is_removed_from_swarm_memory_after_execution(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()
        test_swarm_bot_3.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_3)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_3)

        self.wait_for_idle_network()

        test_swarm_bot_1.set_task_executor_status(False)
        test_swarm_bot_2.set_task_executor_status(False)
        test_swarm_bot_3.set_task_executor_status(False)

        test_task_bundle = SwarmTaskBundle()
        test_task_bundle.add_task(SimpleTask, 1)

        check_val = test_swarm_bot_1.receive_task_bundle(test_task_bundle)
        self.assertTrue(check_val)

        self.wait_for_idle_network()

        task = test_task_bundle.get_tasks()[0]

        bot_2_ret_val = test_swarm_bot_2.read_from_swarm_memory(task.get_id())

        self.wait_for_idle_network()

        bot_2_ret_val = test_swarm_bot_2.read_from_swarm_memory(task.get_id())

        self.assertEqual(task, bot_2_ret_val["TASK"])

        test_swarm_bot_1.set_task_executor_status(True)

        self.wait_for_idle_network()

        bot_2_ret_val = test_swarm_bot_2.read_from_swarm_memory(task.get_id())

        self.wait_for_idle_network()

        bot_2_ret_val = test_swarm_bot_2.read_from_swarm_memory(task.get_id())

        self.assertEqual(None, bot_2_ret_val)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
