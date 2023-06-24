import logging
import unittest
import time

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from swarm.swarm_task.swarm_task import SwarmTask
from swarm.swarm_bot import SwarmBot
from swarm.swarm_manager import SwarmManager
from network_manager.network_connectivity_level import NetworkConnectivityLevel
from swarm.swarm_task.swarm_task_bundle import SwarmTaskBundle


class SimpleTask(SwarmTask):
    def __init__(self, task_params):
        super().__init__(task_params)
        self.task_complete = False
        self.sleep_time = 1

    def is_complete(self):
        return self.task_complete

    def execute_task(self):
        time.sleep(self.sleep_time)
        self.task_complete = True

    def get_task_output(self):
        return self.sleep_time


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

        test_swarm_bot = self.create_network_node(SwarmBot)

        test_swarm_manager.add_network_node(test_swarm_bot)

        test_task_bundle = SwarmTaskBundle()
        test_task_bundle.add_task(SimpleTask, 1, [])

        self.assertFalse(test_task_bundle.is_complete())

        task_output = test_swarm_manager.receive_task_bundle(test_task_bundle)

        self.assertEqual(1, task_output["SimpleTask"][0])
        self.assertTrue(test_task_bundle.is_complete())
        self.assertIn(test_task_bundle.get_tasks()[0], test_swarm_bot.get_task_execution_history())

    def test_swarm_manager_will_delegate_task_to_bots_in_the_swarm_when_task_requires_two_bots(self):
        test_swarm_manager = self.create_swarm_manager(NetworkConnectivityLevel.FULLY_CONNECTED)

        test_swarm_manager.add_network_node(self.create_network_node(SwarmBot))
        test_swarm_manager.add_network_node(self.create_network_node(SwarmBot))

        test_task_bundle = SwarmTaskBundle()
        test_task_bundle.add_task(SimpleTask, 2, [])

        self.assertFalse(test_task_bundle.is_complete())

        task_output = test_swarm_manager.receive_task_bundle(test_task_bundle)

        self.assertEqual([1, 1], task_output["SimpleTask"])
        self.assertTrue(test_task_bundle.is_complete())

    def test_swarm_manager_will_delegate_task_to_bots_in_the_swarm_when_task_requires_multiple_bots(self):
        test_swarm_manager = self.create_swarm_manager(NetworkConnectivityLevel.FULLY_CONNECTED)

        test_swarm_manager.add_network_node(self.create_network_node(SwarmBot))
        test_swarm_manager.add_network_node(self.create_network_node(SwarmBot))
        test_swarm_manager.add_network_node(self.create_network_node(SwarmBot))

        test_task_bundle = SwarmTaskBundle()
        test_task_bundle.add_task(SimpleTask, 3, [])

        self.assertFalse(test_task_bundle.is_complete())

        task_output = test_swarm_manager.receive_task_bundle(test_task_bundle)

        self.assertEqual([1, 1, 1], task_output["SimpleTask"])
        self.assertTrue(test_task_bundle.is_complete())


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
