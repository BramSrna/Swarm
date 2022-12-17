import logging
import unittest
import time

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from swarm.swarm_task import SwarmTask
from swarm.swarm_bot import SwarmBot
from swarm.swarm_manager import SwarmManager
from network_manager.network_connectivity_level import NetworkConnectivityLevel


class SimpleTask(SwarmTask):
    def __init__(self, req_num_bots, sleep_time):
        super().__init__()
        self.req_num_bots = req_num_bots
        self.task_complete = False
        self.sleep_time = sleep_time

    def is_task_complete(self):
        return self.task_complete

    def execute_task(self):
        time.sleep(self.sleep_time)
        self.task_complete = True


class TestSwarmManager(NetworkNodeTestClass):
    def setUp(self):
        super().setUp()
        self.test_swarm_managers = []

    def tearDown(self):
        for network_manager in self.test_swarm_managers:
            network_manager.teardown()

    def create_swarm_manager(self, connectivity_type):
        new_manager = SwarmManager(connectivity_type)
        self.test_swarm_managers.append(new_manager)
        return new_manager

    def test_swarm_manager_will_delegate_task_to_bots_in_the_swarm_when_task_requires_one_bot(self):
        test_swarm_manager = self.create_swarm_manager(NetworkConnectivityLevel.FULLY_CONNECTED)

        test_swarm_bot = SwarmBot()
        test_swarm_bot.startup()

        test_swarm_manager.add_network_node(test_swarm_bot)

        test_task = SimpleTask(1, 0)

        self.assertFalse(test_task.is_task_complete())

        test_swarm_manager.receive_task(test_task)

        self.wait_for_idle_network()

        self.assertTrue(test_task.is_task_complete())

        self.assertIn(test_task, test_swarm_bot.get_task_execution_history())

        self.assertIn(test_swarm_bot.get_id(), test_swarm_manager.get_idle_bots())

    def test_swarm_manager_will_delegate_task_to_bots_in_the_swarm_when_task_requires_multiple_bots(self):
        test_swarm_manager = self.create_swarm_manager(NetworkConnectivityLevel.FULLY_CONNECTED)

        test_task = SimpleTask(3, 0)

        self.assertFalse(test_task.is_task_complete())

        test_swarm_manager.receive_task(test_task)

        test_swarm_bot_1 = SwarmBot()
        test_swarm_bot_1.startup()
        test_swarm_bot_2 = SwarmBot()
        test_swarm_bot_2.startup()
        test_swarm_bot_3 = SwarmBot()
        test_swarm_bot_3.startup()

        test_swarm_manager.add_network_node(test_swarm_bot_1)
        test_swarm_manager.add_network_node(test_swarm_bot_2)

        self.assertFalse(test_task.is_task_complete())

        self.wait_for_idle_network()

        test_swarm_manager.add_network_node(test_swarm_bot_3)

        self.wait_for_idle_network()

        self.assertTrue(test_task.is_task_complete())

        self.assertIn(test_task, test_swarm_bot_1.get_task_execution_history())
        self.assertIn(test_task, test_swarm_bot_2.get_task_execution_history())
        self.assertIn(test_task, test_swarm_bot_3.get_task_execution_history())

        self.assertIn(test_swarm_bot_1.get_id(), test_swarm_manager.get_idle_bots())
        self.assertIn(test_swarm_bot_2.get_id(), test_swarm_manager.get_idle_bots())
        self.assertIn(test_swarm_bot_3.get_id(), test_swarm_manager.get_idle_bots())

        self.assertTrue(test_swarm_bot_1.is_connected_to(test_swarm_bot_2.get_id()))
        self.assertTrue(test_swarm_bot_1.is_connected_to(test_swarm_bot_3.get_id()))
        self.assertTrue(test_swarm_bot_2.is_connected_to(test_swarm_bot_1.get_id()))
        self.assertTrue(test_swarm_bot_2.is_connected_to(test_swarm_bot_3.get_id()))
        self.assertTrue(test_swarm_bot_3.is_connected_to(test_swarm_bot_1.get_id()))
        self.assertTrue(test_swarm_bot_3.is_connected_to(test_swarm_bot_2.get_id()))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()