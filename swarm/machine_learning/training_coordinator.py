from swarm.swarm_task.swarm_task_bundle import SwarmTaskBundle
from swarm.machine_learning.federated_learning.tasks.federated_learning_model_aggregation_task import FederatedLearningModelAggregationTask

class TrainingCoordinator(object):
    def __init__(self, executor_interface):
        self.executor_interface = executor_interface

        self.models = {}

    def check_for_aggregation_threshold(self, path, snapshot):
        model_id = path.split("/")[1]
        print(model_id, snapshot)
        if len(snapshot) >= self.models[model_id]:
            aggregation_task_bundle = SwarmTaskBundle()
            aggregation_task_bundle.add_task(FederatedLearningModelAggregationTask, 1, [model_id])
            self.executor_interface.receive_task_bundle(aggregation_task_bundle)
    
    def initalize_federated_learning_model(self, model_class, training_task, data_threshold, aggregation_threshold):
        new_model = model_class()
        model_id = new_model.get_id()
        self.executor_interface.write_to_swarm_memory("models/" + str(model_id) + "/current_model", new_model)
        self.executor_interface.write_to_swarm_memory("models/" + str(model_id) + "/local_models", {})
        self.executor_interface.write_to_swarm_memory("models/" + str(model_id) + "/validation_info/validation_data", [])
        self.executor_interface.write_to_swarm_memory("models/" + str(model_id) + "/validation_info/validation_targets", [])
        self.executor_interface.add_path_watcher("models/" + str(model_id) + "/local_models", self.check_for_aggregation_threshold)
        self.models[str(model_id)] = aggregation_threshold
        for _ in range(aggregation_threshold):
            training_task_bundle = SwarmTaskBundle()
            training_task_bundle.add_task(training_task, 1, [model_id, data_threshold])
            self.executor_interface.receive_task_bundle(training_task_bundle)
        return new_model