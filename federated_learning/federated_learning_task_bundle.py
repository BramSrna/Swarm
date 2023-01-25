from federated_learning.federated_learning_aggregator_task import FederatedLearningAggregatorTask
from federated_learning.federated_learning_client_task import FederatedLearningClientTask
from federated_learning.federated_learning_node_task import FederatedLearningNodeTask
from swarm.swarm_task_bundle import SwarmTaskBundle


def get_federated_learning_task_bundle():
    fd_task_bundle = SwarmTaskBundle()
    fd_task_bundle.add_task(FederatedLearningAggregatorTask, 3)
    fd_task_bundle.add_task(FederatedLearningClientTask, 3)
    fd_task_bundle.add_task(FederatedLearningNodeTask, 3)
    return fd_task_bundle
