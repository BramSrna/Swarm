import logging
import unittest
import time

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from swarm.swarm_task import SwarmTask
from swarm.swarm_bot import SwarmBot


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


class TestSwarmTaskInteraction(NetworkNodeTestClass):
    def test_swarm_bot_will_execute_task_when_not_already_executing_a_task(self):
        test_swarm_bot = self.create_network_node(SwarmBot)

        test_swarm_bot.startup()

        test_task = SimpleTask(1, 0)

        self.assertFalse(test_task.is_task_complete())

        test_swarm_bot.receive_task(test_task)

        self.wait_for_idle_network()

        self.assertTrue(test_task.is_task_complete())

    def test_swarm_bot_will_pick_up_next_task_in_queue_when_done_executing_current_task(self):
        test_swarm_bot = self.create_network_node(SwarmBot)

        test_swarm_bot.startup()

        test_task_1 = SimpleTask(1, 3)
        test_task_2 = SimpleTask(1, 3)

        self.assertFalse(test_task_1.is_task_complete())
        self.assertFalse(test_task_2.is_task_complete())

        test_swarm_bot.receive_task(test_task_1)
        test_swarm_bot.receive_task(test_task_2)

        self.wait_for_idle_network()

        self.assertTrue(test_task_1.is_task_complete())
        self.assertTrue(test_task_2.is_task_complete())

    def test_second_swarm_bot_will_pick_up_received_task_if_receiver_bot_is_busy(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.startup()
        test_swarm_bot_2.startup()

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_1)

        test_task_1 = SimpleTask(1, 3)
        test_task_2 = SimpleTask(1, 3)

        self.assertFalse(test_task_1.is_task_complete())
        self.assertFalse(test_task_2.is_task_complete())

        test_swarm_bot_1.receive_task(test_task_1)
        test_swarm_bot_1.receive_task(test_task_2)

        self.wait_for_idle_network()

        self.assertTrue(test_task_1.is_task_complete())
        self.assertTrue(test_task_2.is_task_complete())

        self.assertIn(test_task_1, test_swarm_bot_1.get_task_execution_history())
        self.assertIn(test_task_2, test_swarm_bot_2.get_task_execution_history())

        self.assertNotIn(test_task_2, test_swarm_bot_1.get_task_execution_history())
        self.assertNotIn(test_task_1, test_swarm_bot_2.get_task_execution_history())


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
