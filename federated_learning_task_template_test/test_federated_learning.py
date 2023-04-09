import logging
import unittest
import pytest

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from swarm.swarm_bot import SwarmBot
from swarm.swarm_manager import SwarmManager
from network_manager.network_connectivity_level import NetworkConnectivityLevel
from federated_learning_task_template.federated_learning_task_bundle import get_federated_learning_task_bundle
from federated_learning_task_template.federated_learning_model import FederatedLearningModel


class TestFederatedLearning(NetworkNodeTestClass):
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
    def test_new_model_will_be_generated_after_running_full_e2e_federated_learning(self):
        manager = self.create_swarm_manager(NetworkConnectivityLevel.FULLY_CONNECTED)

        num_client_bots = 3
        num_node_bots = 3
        num_aggregator_bots = 3
        num_coordinator_bots = 1

        for _ in range(num_client_bots + num_node_bots + num_aggregator_bots + num_coordinator_bots):
            new_bot = self.create_network_node(SwarmBot)
            manager.add_network_node(new_bot)

        task_output = manager.receive_task_bundle(get_federated_learning_task_bundle())

        test_input_1 = 5
        test_input_2 = 8

        test_model = FederatedLearningModel()
        test_model.set_from_task_output(task_output["FederatedLearningClientTask"][0])

        self.assertEqual(test_input_1 + test_input_2, test_model.execute(test_input_1, test_input_2))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
