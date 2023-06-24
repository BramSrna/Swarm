from swarm.swarm_task.swarm_task import SwarmTask
from swarm.machine_learning.federated_learning.models.federated_learning_model import FederatedLearningModel
import numpy as np

from swarm.swarm_task.swarm_task_bundle import SwarmTaskBundle
from swarm.machine_learning.federated_learning.tasks.federated_learning_model_aggregation_task import FederatedLearningModelAggregationTask

class FederatedLearningModelTrainingTask(SwarmTask):
    def __init__(self, extra_params):
        super().__init__(extra_params)
        self.model_id = extra_params[0]
        self.data_threshold = extra_params[1]

        self.pushed_new_model = False

        self.local_data = []
        self.local_targets = []

        self.local_model = None

    def setup(self, executor_interface, execution_group_info):
        super().setup(executor_interface, execution_group_info)

        global_model = self.executor_interface.read_from_swarm_memory("models/" + str(self.model_id) + "/current_model")
        self.local_model = global_model.__class__()
        if issubclass(global_model.__class__, FederatedLearningModel):
            self.local_model.set_from_model(global_model)

    def get_data_point(self):
        raise Exception("The get_data_point method must be implemented by the child class.")

    def is_complete(self):
        return self.pushed_new_model

    def execute_task(self):
        new_data_point, new_target = self.get_data_point()
        self.add_data_point(new_data_point, new_target)

    def get_task_output(self):
        return self.local_model

    def add_data_point(self, new_data, new_target):
        # Training task shall specify how to get data and any processing that needs to be done
        self.local_data.append(new_data)
        self.local_targets.append(new_target)

        if len(self.local_data) % self.data_threshold == 0:
            self.retrain_model()

    def retrain_model(self):
        # Training specification will be based on the model
        train_percent = 0.70
        validation_percent = 0.15
        test_percent = 0.15

        train_start_ind = 0
        validation_start_ind = int(len(self.local_data) * train_percent)
        test_start_ind = int(len(self.local_data) * train_percent + len(self.local_data) * validation_percent)

        train_data = self.local_data[train_start_ind:validation_start_ind]
        train_targets = self.local_targets[train_start_ind:validation_start_ind]

        validation_data = self.local_data[validation_start_ind:test_start_ind]
        validation_targets = self.local_targets[validation_start_ind:test_start_ind]

        test_data = self.local_data[test_start_ind:len(self.local_data)]
        test_targets = self.local_targets[test_start_ind:len(self.local_targets)]

        self.local_model.train(np.array(train_data), np.array(train_targets))

        score = self.local_model.get_score(test_data, test_targets)
        if score > -1.0:
            self.save_retrained_local_model(self.local_model, validation_data, validation_targets)

    def save_retrained_local_model(self, model, new_validation_data, new_validation_targets):
        validation_info = self.executor_interface.read_from_swarm_memory("models/" + str(self.model_id) + "/validation_info")
        global_validation_data = validation_info["validation_data"]
        global_validation_targets = validation_info["validation_targets"]

        new_validation_data = global_validation_data + new_validation_data
        new_validation_targets = global_validation_targets + new_validation_targets
        self.executor_interface.write_to_swarm_memory("models/" + str(self.model_id) + "/validation_info/validation_data", new_validation_data)
        self.executor_interface.write_to_swarm_memory("models/" + str(self.model_id) + "/validation_info/validation_targets", new_validation_targets)
        self.executor_interface.write_to_swarm_memory("models/" + str(self.model_id) + "/local_models/" + str(self.get_id()), model)

        self.pushed_new_model = True

    def get_priority_score(self):
        model = self.executor_interface.read_from_swarm_memory("models/" + str(self.model_id) + "/current_model")
        if model is None:
            return 1
        
        if not model.get_has_been_fitted():
            return 1
        
        validation_info = self.executor_interface.read_from_swarm_memory("models/" + str(self.model_id) + "/validation_info")
        validation_data = validation_info["validation_data"]
        validation_targets = validation_info["validation_targets"]
        score = model.get_score(validation_data, validation_targets)
        return ((score * -1) + 1) / 0.5

