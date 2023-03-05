from network_manager.network_node.propagation_strategy.propagation_strategy import PropagationStrategy
from network_manager.network_node.message_wrapper.message_wrapper import MessageWrapper


class FullyConnectedSwarmPropagation(PropagationStrategy):
    def __init__(self, owner_network_node):
        super().__init__(owner_network_node)

    def determine_prop_targets(self, message: MessageWrapper) -> list:
        if message is None:
            return self.network_node.get_message_channels().keys()
        return []

    def track_message_propagation(self, message: MessageWrapper) -> None:
        pass
