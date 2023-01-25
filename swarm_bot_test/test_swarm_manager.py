import logging
import unittest
import time
import pytest

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from swarm.swarm_task import SwarmTask
from swarm.swarm_bot import SwarmBot
from swarm.swarm_manager import SwarmManager
from network_manager.network_connectivity_level import NetworkConnectivityLevel
from swarm.swarm_task_bundle import SwarmTaskBundle


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

    @pytest.mark.skip(reason="Will be executable once https://github.com/users/BramSrna/projects/4 is finished.")
    def test_swarm_manager_will_delegate_task_to_bots_in_the_swarm_when_task_requires_one_bot(self):
        test_swarm_manager = self.create_swarm_manager(NetworkConnectivityLevel.FULLY_CONNECTED)

        test_swarm_bot = SwarmBot()
        test_swarm_bot.startup()

        test_swarm_manager.add_network_node(test_swarm_bot)

        test_task_bundle = SwarmTaskBundle()
        test_task_bundle.add_task(SimpleTask, 1)

        self.assertFalse(test_task_bundle.is_complete())

        test_swarm_manager.receive_task(test_task_bundle)

        self.wait_for_idle_network()

        self.assertTrue(test_task_bundle.is_complete())

        self.assertIn(test_task_bundle, test_swarm_bot.get_task_execution_history())

        self.assertIn(test_swarm_bot.get_id(), test_swarm_manager.get_idle_bots())

    @pytest.mark.skip(reason="Will be executable once https://github.com/users/BramSrna/projects/4 is finished.")
    def test_swarm_manager_will_delegate_task_to_bots_in_the_swarm_when_task_requires_multiple_bots(self):
        test_swarm_manager = self.create_swarm_manager(NetworkConnectivityLevel.FULLY_CONNECTED)

        test_task_bundle = SwarmTaskBundle()
        test_task_bundle.add_task(SimpleTask, 3)

        self.assertFalse(test_task_bundle.is_complete())

        test_swarm_manager.receive_task(test_task_bundle)

        test_swarm_bot_1 = SwarmBot()
        test_swarm_bot_1.startup()
        test_swarm_bot_2 = SwarmBot()
        test_swarm_bot_2.startup()
        test_swarm_bot_3 = SwarmBot()
        test_swarm_bot_3.startup()

        test_swarm_manager.add_network_node(test_swarm_bot_1)
        test_swarm_manager.add_network_node(test_swarm_bot_2)

        self.assertFalse(test_task_bundle.is_complete())

        self.wait_for_idle_network()

        test_swarm_manager.add_network_node(test_swarm_bot_3)

        self.wait_for_idle_network()

        self.assertTrue(test_task_bundle.is_complete())

        self.assertIn(test_task_bundle, test_swarm_bot_1.get_task_execution_history())
        self.assertIn(test_task_bundle, test_swarm_bot_2.get_task_execution_history())
        self.assertIn(test_task_bundle, test_swarm_bot_3.get_task_execution_history())

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
