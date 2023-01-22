from swarm.swarm_bot import SwarmBot
from swarm.swarm_manager import SwarmManager

if __name__ == "__main__":
    manager = SwarmManager()

    num_client_bots = 3
    num_node_bots = 3
    num_aggregator_bots = 3

    for _ in range(num_client_bots + num_node_bots + num_aggregator_bots):
        new_bot = SwarmBot()
        SwarmBot.startup()
        manager.add_network_node(new_bot)

    for _ in range(num_client_bots):
        manager.receive_task(FederatedLearningClientTask)

    for _ in range(num_node_bots):
        manager.receive_task(FederatedLearningNodeTask)

    for _ in range(num_aggregator_bots):
        manager.receive_task(FederatedLearningAggregatorTask)