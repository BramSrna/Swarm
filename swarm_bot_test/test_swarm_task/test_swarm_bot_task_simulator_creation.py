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
        curr_state = self.simulator.get_current_simulator_state()
        random.choice(self.simulator.get_possible_actions_as_methods(curr_state))()
        new_state = self.simulator.get_current_simulator_state()
        if curr_state != new_state:
            self.shortest_path.append((new_state[0], new_state[1]))

    def get_task_output(self):
        return self.shortest_path
    
    def get_simulator(self):
        return self.simulator

    def get_state(self):
        return self.simulator.get_current_simulator_state()

    def get_possible_actions(self, state):
        return self.simulator.get_possible_actions(state)


class TestSwarmBotTaskSimulatorCreation(NetworkNodeTestClass):
    def test_swarm_bot_task_will_create_mock_simulator_while_executing_task(self):
        test_swarm_bot = self.create_network_node(SwarmBot)

        test_task_bundle = SwarmTaskBundle()
        test_task_bundle.add_task(ShortestDistanceSolverTask, 1)

        test_swarm_bot.receive_task_bundle(test_task_bundle)

        self.wait_for_idle_network()

        self.assertTrue(test_task_bundle.is_complete())

        expected_simulator = test_task_bundle.get_tasks()[0].get_simulator()
        actual_simulator = test_swarm_bot.get_simulator_for_task(ShortestDistanceSolverTask)
        self.assertNotEqual(None, actual_simulator)

        possible_states = expected_simulator.get_traversed_path()
        for state in possible_states:
            expected_possible_actions = expected_simulator.get_possible_actions(state)
            actual_possible_actions = actual_simulator.get_possible_actions(state)[0]
            print(expected_possible_actions, actual_possible_actions)
            self.assertEqual(len(expected_possible_actions), len(actual_possible_actions))
            self.assertEqual(expected_possible_actions[0], actual_possible_actions[0])
            self.assertEqual(expected_possible_actions[1], actual_possible_actions[1])
            self.assertEqual(expected_possible_actions[2], actual_possible_actions[2])
            self.assertEqual(expected_possible_actions[3], actual_possible_actions[3])


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
