import logging
import unittest
import numpy as np

from unittest.mock import MagicMock

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from swarm.swarm_bot import SwarmBot
from swarm.machine_learning.federated_learning.models.federated_learning_model import FederatedLearningModel
from swarm.machine_learning.federated_learning.models.federated_learning_sgd_regressor_model import FederatedLearningSGDRegressorModel
from swarm.machine_learning.federated_learning.tasks.federated_learning_model_training_task import FederatedLearningModelTrainingTask
from swarm.machine_learning.federated_learning.tasks.federated_learning_model_aggregation_task import FederatedLearningModelAggregationTask
from swarm.swarm_task.swarm_task_bundle import SwarmTaskBundle

class BasicFederatedLearningModelTrainingTask(FederatedLearningModelTrainingTask):
    def get_data_point(self):
        curr_x = np.random.randn(1, 5)[0]
        curr_y = np.random.randn(1)[0]

        return curr_x, curr_y
    

class TestFederatedLearning(NetworkNodeTestClass):
    def test_federated_learning_model_training_task_will_push_local_model_when_done_training(self):
        model_id = 0
        data_threshold = 10
        aggregation_threshold = 1

        def mock_read(arg):
            ret_map = {
                "models/" + str(model_id) + "/current_model": FederatedLearningSGDRegressorModel(),
                "models/" + str(model_id) + "/validation_info": {"validation_data": [], "validation_targets": []}
            }
            return ret_map[arg]

        executor_interface = MagicMock()

        executor_interface.read_from_swarm_memory = MagicMock()
        executor_interface.read_from_swarm_memory.side_effect = mock_read

        test_task = BasicFederatedLearningModelTrainingTask([model_id, data_threshold])
        test_task.setup(executor_interface, {})

        while not test_task.is_complete():
            test_task.execute_task()

        executor_interface.write_to_swarm_memory.assert_called()

    def test_federated_learning_model_aggregation_task_will_aggregate_local_models_and_push_new_model_when_done_training(self):
        model_id = 0
        data_threshold = 10
        aggregation_threshold = 1

        executor_interface = MagicMock()

        def mock_read(arg):
            ret_map = {
                "models/" + str(model_id) + "/current_model": FederatedLearningSGDRegressorModel(),
                "models/" + str(model_id) + "/validation_info": {"validation_data": [], "validation_targets": []}
            }
            return ret_map[arg]

        executor_interface.read_from_swarm_memory = MagicMock()
        executor_interface.read_from_swarm_memory.side_effect = mock_read

        test_training_task = BasicFederatedLearningModelTrainingTask([model_id, data_threshold])
        test_training_task.setup(executor_interface, {})

        while not test_training_task.is_complete():
            test_training_task.execute_task()

        executor_interface.read_from_swarm_memory.return_value = {"123": test_training_task.get_task_output()}

        def mock_read(arg):
            ret_map = {
                "models/" + str(model_id) + "/current_model": FederatedLearningSGDRegressorModel(),
                "models/" + str(model_id) + "/validation_info": {"validation_data": [], "validation_targets": []},
                "models/" + str(model_id) + "/local_models": {"123": test_training_task.get_task_output()}
            }
            return ret_map[arg]

        executor_interface.read_from_swarm_memory = MagicMock()
        executor_interface.read_from_swarm_memory.side_effect = mock_read
        test_task = FederatedLearningModelAggregationTask([model_id])
        test_task.setup(executor_interface, {})
        test_task.execute_task()
        executor_interface.write_to_swarm_memory.assert_called_with("models/" + str(model_id) + "/current_model", test_task.get_task_output())

    def test_federated_learning_models_shall_be_saved_to_swarm_memory(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.set_task_executor_status(False)

        model = test_swarm_bot_1.initalize_federated_learning_model(FederatedLearningSGDRegressorModel, BasicFederatedLearningModelTrainingTask, 10, 1)
        initialized_model = test_swarm_bot_1.read_from_swarm_memory("models/" + str(model.get_id()))["current_model"]

        self.assertIsInstance(initialized_model, FederatedLearningSGDRegressorModel)

    def test_models_that_still_require_training_will_create_training_tasks(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.set_task_executor_status(False)

        self.assertEqual(0, len(test_swarm_bot_1.get_task_queue()))

        test_swarm_bot_1.initalize_federated_learning_model(FederatedLearningSGDRegressorModel, BasicFederatedLearningModelTrainingTask, 10, 1)

        task_queue = test_swarm_bot_1.get_task_queue()
        self.assertEqual(1, len(task_queue))
        self.assertIsInstance(test_swarm_bot_1.read_from_swarm_memory("TASK_QUEUE/" + str(task_queue[0]))["TASK"], FederatedLearningModelTrainingTask)

    def test_when_succificient_local_models_are_trained_federated_learning_models_shall_create_an_aggregation_task(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)

        initial_model = test_swarm_bot_1.initalize_federated_learning_model(FederatedLearningSGDRegressorModel, BasicFederatedLearningModelTrainingTask, 10, 1)
        
        self.wait_for_idle_network()

        end_model = test_swarm_bot_1.read_from_swarm_memory("models/" + str(initial_model.get_id()))["current_model"]

        self.assertNotEqual(None, initial_model)
        self.assertNotEqual(None, end_model)
        self.assertNotEqual(initial_model, end_model)

    def test_new_swarm_bots_can_join_the_training_process_when_the_number_of_clients_is_beneath_the_threshold(self):
        test_swarm_bot_1 = self.create_network_node(SwarmBot)
        test_swarm_bot_2 = self.create_network_node(SwarmBot)
        test_swarm_bot_3 = self.create_network_node(SwarmBot)

        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_2)
        test_swarm_bot_1.connect_to_network_node(test_swarm_bot_3)
        test_swarm_bot_2.connect_to_network_node(test_swarm_bot_3)

        initial_model = test_swarm_bot_1.initalize_federated_learning_model(FederatedLearningSGDRegressorModel, BasicFederatedLearningModelTrainingTask, 10, 3)

        self.wait_for_idle_network()

        end_model = test_swarm_bot_1.read_from_swarm_memory("models/" + str(initial_model.get_id()))["current_model"]

        self.assertNotEqual(None, initial_model)
        self.assertNotEqual(None, end_model)
        self.assertNotEqual(initial_model, end_model)




if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
