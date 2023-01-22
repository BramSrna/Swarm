from swarm.swarm_task import SwarmTask
from federated_learning.federated_learning_model import FederatedLearningModel
import numpy

class FederatedLearningClientTask(SwarmTask):
    def __init__(self):
        super().__init__()

        self.local_model = FederatedLearningModel()

        self.local_data = []
        self.local_targets = []

        self.data_threshold = 100

        self.node_list = []

    def is_task_complete(self):
        return len(self.node_list) > self.data_threshold

    def execute_task(self):
        n_samples, n_features = 10, 5
        curr_x = numpy.random.randn(1, n_features)[0]
        curr_y = numpy.random.randn(1)[0]

        self.add_data_point(curr_x, curr_y)

    def get_task_output(self):
        return self.local_model

    def add_data_point(self, new_data, new_target):
        self.local_data.append(new_data)
        self.local_targets.append(new_target)

        if len(self.local_data) % self.data_threshold == 0:
            self.retrain_model()

    def run_score(self, test_data, test_targets):
        return self.local_model.get_score(test_data, test_targets)

    def get_local_model(self):
        return self.local_model

    def retrain_model(self):
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

        self.local_model.train(numpy.array(train_data), numpy.array(train_targets))

        score = self.local_model.get_score(test_data, test_targets)
        if score > -1.0:
            self.send_validated_model(self.local_model, validation_data, validation_targets)

    def send_validated_model(self, model, validation_data, validation_targets):
        for node in self.node_list:
            node.receive_candidate_model(model, validation_data, validation_targets)

    def notify_new_global_model(self, new_block):
        self.local_model.set_from_block(new_block)