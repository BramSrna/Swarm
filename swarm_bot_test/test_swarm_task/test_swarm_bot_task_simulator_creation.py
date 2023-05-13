import logging
import unittest
import random

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from swarm.swarm_task.swarm_task import SwarmTask
from swarm.swarm_bot import SwarmBot
from swarm.swarm_task.swarm_task_bundle import SwarmTaskBundle
from swarm_bot_test.test_swarm_task.simulator.shortest_distance_simulator import ShortestDistanceSimulator


class ShortestDistanceSolverTask(SwarmTask):
    def __init__(self):
        super().__init__()
        self.simulator = ShortestDistanceSimulator()
        self.shortest_path = []

    def is_complete(self):
        return self.simulator.check_for_finish()

    def execute_task(self):
        curr_loc = self.simulator.get_curr_location()
        random.choice(self.simulator.get_possible_options())()
        new_loc = self.simulator.get_curr_location()
        if curr_loc != new_loc:
            self.shortest_path.append(new_loc)

    def get_task_output(self):
        return self.shortest_path


class TestSwarmBotTaskSimulatorCreation(NetworkNodeTestClass):
    def test_swarm_bot_task_will_create_mock_simulator_while_executing_task(self):
        test_swarm_bot = self.create_network_node(SwarmBot)

        test_task_bundle = SwarmTaskBundle()
        test_task_bundle.add_task(ShortestDistanceSolverTask, 1)

        test_swarm_bot.receive_task_bundle(test_task_bundle)

        self.wait_for_idle_network()

        self.assertTrue(test_task_bundle.is_complete())


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
