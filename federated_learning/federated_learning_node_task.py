from swarm.swarm_task import SwarmTask
from federated_learning.federated_learning_model import FederatedLearningModel


class FederatedLearningNodeTask(SwarmTask):
    def __init__(self):
        super().__init__()

        self.node_list = []
        self.aggregator_list = []
        self.client_list = []

        self.blockchain = []
        self.download_blockchain()

    def is_task_complete(self):
        task_complete = True
        for client in self.execution_group_info["FederatedLearningClientTask"]:
            task_complete = task_complete and client.is_task_complete()
        return task_complete

    def execute_task(self):
        pass

    def get_task_output(self):
        pass

    def add_node(self, new_node):
        self.node_list.append(new_node)

    def add_aggregator(self, new_aggregator):
        self.aggregator_list.append(new_aggregator)

    def download_blockchain(self):
        if len(self.node_list) > 0:
            # TODO: Download blocks from connected nodes
            # Validate blocks as they come in
            # Keep track of latest block
            self.node_list[0].request_blockchain_transfer(self)

    def get_curr_blockchain_model(self):
        curr_model = FederatedLearningModel()
        if len(self.blockchain) > 0:
            curr_model.set_from_block(self.blockchain[len(self.blockchain) - 1])
        return curr_model

    def receive_candidate_model(self, new_model, new_validation_data, new_validation_targets):
        test_score = new_model.get_score(new_validation_data, new_validation_targets)
        if test_score > -100:
            for aggregator in self.aggregator_list:
                aggregator.receive_valid_model(new_model, new_validation_data, new_validation_targets)

    def request_blockchain_transfer(self, requester_bot):
        for block in self.blockchain:
            requester_bot.notify_new_block(block, False)

    def notify_new_block(self, new_block, notify_clients):
        if new_block.run_proof_of_validation():
            self.blockchain.append(new_block)
            if notify_clients:
                for client in self.client_list:
                    client.notify_new_global_model(new_block)
