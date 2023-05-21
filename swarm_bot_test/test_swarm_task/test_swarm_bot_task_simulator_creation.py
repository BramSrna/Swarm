import logging
import unittest
import random

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from swarm.swarm_task.swarm_task import SwarmTask
from swarm.swarm_bot import SwarmBot
from swarm.swarm_task.swarm_task_bundle import SwarmTaskBundle
from swarm_bot_test.test_swarm_task.simulator.shortest_distance_simulator import ShortestDistanceSimulator
from swarm.swarm_task.task_simulator import TaskSimulator
from swarm.swarm_task.swarm_bot_optimized_task import SwarmBotOptimizedTask
from swarm.executor_interface import ExecutorInterface
from unittest.mock import MagicMock

class MockExecutorInterface(ExecutorInterface):
    def __init__(self, simulator):
        super().__init__(None)
        self.simulator = simulator

    def check_simulator_for_finish(self):
        finished = self.simulator.check_for_finish()
        self.add_flow_info("check_simulator_for_finish", [], [finished])
        return finished

    def move_up(self):
        ret_val = self.simulator.move_up()
        self.add_flow_info("move_up", [], [ret_val])
        return ret_val

    def move_right(self):
        ret_val = self.simulator.move_right()
        self.add_flow_info("move_right", [], [ret_val])
        return ret_val

    def move_down(self):
        ret_val = self.simulator.move_down()
        self.add_flow_info("move_down", [], [ret_val])
        return ret_val

    def move_left(self):
        ret_val = self.simulator.move_left()
        self.add_flow_info("move_left", [], [ret_val])
        return ret_val
    
    def get_current_simulator_state(self):
        state = self.simulator.get_current_simulator_state()
        self.add_flow_info("get_current_simulator_state", [], [state])
        return state
    
    def get_possible_actions(self, state):
        actions = self.simulator.get_possible_actions(state)
        self.add_flow_info("get_possible_actions", [state], [actions])
        return actions
    
    def get_possible_actions_for_current_state(self):
        curr_state = self.get_current_simulator_state()
        bin_possible_actions = self.get_possible_actions(curr_state)
        method_possible_actions = []
        if bin_possible_actions[0]:
            method_possible_actions.append(self.move_up)
        if bin_possible_actions[1]:
            method_possible_actions.append(self.move_right)
        if bin_possible_actions[2]:
            method_possible_actions.append(self.move_down)
        if bin_possible_actions[3]:
            method_possible_actions.append(self.move_left)
        self.add_flow_info("get_possible_actions_for_current_state", [], [method_possible_actions])
        return method_possible_actions


class ShortestDistanceSolverTask(SwarmTask):
    def __init__(self):
        super().__init__()
        self.path_traversed = []

    def is_complete(self):
        return self.executor_interface.check_simulator_for_finish()

    def execute_task(self):
        possible_actions = self.executor_interface.get_possible_actions_for_current_state()
        random.choice(possible_actions)()
        self.path_traversed.append(self.executor_interface.get_current_simulator_state())

    def get_task_output(self):
        return self.path_traversed


class TestSwarmBotTaskSimulatorCreation(NetworkNodeTestClass):
    # @classmethod
    # def setUpClass(cls):
    #     cls.shortest_dist_sim = ShortestDistanceSimulator()
    #     shortest_distance_solver_task = ShortestDistanceSolverTask()
    #     cls.task_simulator = TaskSimulator()
    #     executor_interface = ExecutorInterface(None)
    #     shortest_distance_solver_task.setup(executor_interface, {})

    #     sim_iteration = 0
    #     while (not cls.task_simulator.is_ready_for_use()) and (sim_iteration < 100):
    #         task_iteration = 0
    #         cls.shortest_dist_sim.reset()
    #         while (not shortest_distance_solver_task.is_complete()) and (task_iteration < 1000):
    #             shortest_distance_solver_task.execute_task()
    #             from_bot_info = executor_interface.get_flow_info_from_bot()
    #             to_bot_info = executor_interface.get_flow_info_to_bot()
    #             executor_interface.clear_flow_info()
    #             cls.task_simulator.save_flow_info(to_bot_info, from_bot_info)
    #             task_iteration += 1
    #         sim_iteration += 1

    #     assert(cls.task_simulator.is_ready_for_use())

    # @classmethod
    # def tearDownClass(cls):
    #     pass

    def test_executor_interface_monitors_flow_info(self):
        simulator = ShortestDistanceSimulator()
        test_executor_interface = MockExecutorInterface(simulator)
        flow_info = test_executor_interface.get_flow_info()
        self.assertEqual(0, len(flow_info))

        test_executor_interface.check_simulator_for_finish()

        flow_info = test_executor_interface.get_flow_info()
        self.assertEqual("check_simulator_for_finish", flow_info[0][0])
        self.assertEqual([], flow_info[0][1])
        self.assertEqual([False], flow_info[0][2])

    def test_task_simulator_can_parse_state_and_action_information_from_flow_info(self):
        simulator = ShortestDistanceSimulator()
        test_executor_interface = MockExecutorInterface(simulator)
        test_task = ShortestDistanceSolverTask()
        test_task.setup(test_executor_interface, None)
        test_task_simulator = TaskSimulator()

        self.assertEqual(0, len(test_task_simulator.get_all_known_states()))

        test_task.execute_task()

        flow_info = test_executor_interface.get_flow_info()
        test_task_simulator.save_execution_flow_info(flow_info)
        self.assertEqual(1, len(test_task_simulator.get_all_known_states()))

    def test_task_execution_controller_will_send_execution_data_to_task_simulator_after_task_execution(self):
        self.assertTrue(False)

    def test_task_execution_contoller_will_send_baseline_task_execution_data_to_optimized_task_after_task_execution(self):
        self.assertTrue(False)

    def test_task_simulator_will_update_upon_receiving_execution_data(self):
        self.assertTrue(False)

    def test_optimized_task_will_run_RHLF_when_baseline_and_task_simulator_are_received(self):
        self.assertTrue(False)

    def test_optimized_task_will_send_new_task_to_task_execution_controller_when_better_task_is_found(self):
        self.assertTrue(False)

    def test_task_executor_and_optimized_task_will_run_in_seperate_threads(self):
        self.assertTrue(False)

    def test_unoptimum_task_will_be_improved_after_running_through_task_controller(self):
        self.assertTrue(False)


        


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
