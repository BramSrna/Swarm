import threading
import random

from network_manager.network_manager import NetworkManager
from network_manager.network_connectivity_level import NetworkConnectivityLevel
from network_manager.network_node.network_node import NetworkNode
from swarm.swarm_bot import SwarmBot
from swarm.swarm_task.swarm_task_message_types import SwarmTaskMessageTypes

"""
SwarmManager

Helper class for managing swarms. Provides helpers
for distributing tasks to the swarm and monitor the idle
states of the bots in the swarm.
"""


class SwarmManager(NetworkManager, SwarmBot):
    def __init__(self, network_connectivity_level: NetworkConnectivityLevel):
        """
        __init__

        Creates a new SwarmManager object

        @param network_connectivity_level [NetworkConnectivityLevel] The connectivity level to use for the swarm

        @return [SwarmManager] The newly created SwarmManager
        """
        NetworkManager.__init__(self, network_connectivity_level)
        SwarmBot.__init__(self)

        self.set_task_executor_status(False)

        self.task_bundle_queue = []

        self.task_tracker = {}
        self.task_locks = {}

        self.assign_msg_handler(
            str(SwarmTaskMessageTypes.TASK_OUTPUT),
            self.swarm_manager_handle_task_output_message
        )

    def teardown(self) -> None:
        self.unassign_msg_handler(
            str(SwarmTaskMessageTypes.TASK_OUTPUT),
            self.swarm_manager_handle_task_output_message
        )
        NetworkManager.teardown(self)
        SwarmBot.teardown(self)

    def add_network_node(self, new_node: NetworkNode) -> None:
        """
        add_network_node
        Overrides the NetworkManager add_network_node method.
        Adds the given node to the manager and then checks if
        any tasks can be executed.
        @param new_node [NetworkNode] The node to add to the network
        @return None
        """
        NetworkManager.add_network_node(self, new_node)
        new_node.connect_to_network_node(self)

    def receive_task_bundle(self, new_task_bundle):
        if new_task_bundle.get_req_num_bots() > len(self.network_nodes):
            return None

        receiver_bot_id = random.choice(list(self.network_nodes.keys()))
        self.network_nodes[receiver_bot_id].receive_task_bundle(new_task_bundle, listener_bot_id=self.get_id())

        bundle_id = new_task_bundle.get_id()

        self.task_locks[bundle_id] = {
            "LOCK": threading.Condition(),
            "TASK_OUTPUT": None
        }

        with self.task_locks[bundle_id]["LOCK"]:
            check = self.task_locks[bundle_id]["LOCK"].wait(timeout=10)
            if check:
                return self.task_locks.pop(bundle_id)["TASK_OUTPUT"]
            else:
                raise Exception("ERROR: Task was not completed within time limit. Bundle ID: {}".format(bundle_id))

    def swarm_manager_handle_task_output_message(self, message):
        msg_payload = message.get_message_payload()
        task_output = msg_payload["TASK_OUTPUT"]
        bundle_id = msg_payload["TASK_BUNDLE_ID"]

        self.task_locks[bundle_id]["TASK_OUTPUT"] = task_output

        with self.task_locks[bundle_id]["LOCK"]:
            self.task_locks[bundle_id]["LOCK"].notify_all()
