from network_manager.network_manager import NetworkManager
from network_manager.network_connectivity_level import NetworkConnectivityLevel
from network_manager.network_node.network_node import NetworkNode
from swarm.swarm_task import SwarmTask

"""
SwarmManager

Helper class for managing swarms. Provides helpers
for distributing tasks to the swarm and monitor the idle
states of the bots in the swarm.
"""


class SwarmManager(NetworkManager):
    def __init__(self, network_connectivity_level: NetworkConnectivityLevel):
        """
        __init__

        Creates a new SwarmManager object

        @param network_connectivity_level [NetworkConnectivityLevel] The connectivity level to use for the swarm

        @return [SwarmManager] The newly created SwarmManager
        """
        NetworkManager.__init__(self, network_connectivity_level)

        self.task_queue = []
        self.idle_bots = []

    def get_idle_bots(self):
        """
        get_idle_bots

        Returns the list of idle bots

        @param None

        @return [list] The IDs of the idle bots
        """
        return self.idle_bots

    def add_network_node(self, new_node: NetworkNode) -> None:
        """
        add_network_node

        Overrides the NetworkManager add_network_node method.
        Adds the given node to the manager and then checks if
        any tasks can be executed.

        @param new_node [NetworkNode] The node to add to the network

        @return None
        """
        super().add_network_node(new_node)
        self.idle_bots.append(new_node.get_id())
        self.check_for_available_task_executors()

    def receive_task(self, new_task: SwarmTask):
        """
        receive_task

        Adds the given task to the manager's task queue.

        @param new_task [SwarmTask] The task to add to the queue

        @return None
        """
        self.task_queue.append(new_task)
        self.check_for_available_task_executors()

    def notify_idle_state(self, bot_id: str, bot_idle: bool) -> None:
        """
        notify_idle_state

        Overrides the NetworkNodeIdleListenerInterface's notify_idle_state method.
        If the bot is now idle, it is added to the manager's list of idle bots.
        Otherwise, it is removed from the list.

        @param bot_id [str] The ID of the bot that changed in state
        @param node_idle [bool] True if the node is now idle. False if the node is busy.

        @return None
        """
        super().notify_idle_state(bot_id, bot_idle)
        if bot_idle:
            if bot_id not in self.idle_bots:
                self.idle_bots.append(bot_id)
        else:
            if bot_id in self.idle_bots:
                self.idle_bots.remove(bot_id)

        self.check_for_available_task_executors()

    def check_for_available_task_executors(self):
        """
        check_for_available_task_executors

        Checks if there are enough idle bots to execute
        any of the tasks currently in the task queue. If
        a task can be executed, then an execution group is formed
        for the task and the bots in the group receive the task.

        @param None

        @return None
        """
        i = 0
        while i < len(self.task_queue):
            task = self.task_queue[i]
            req_num_bots = task.get_req_num_bots()
            if req_num_bots <= len(self.idle_bots):
                break
            i += 1

        if (i < len(self.task_queue)):
            next_task = self.task_queue.pop(i)

            self.setup_execution_group(next_task)
            self.check_for_available_task_executors()

    def setup_execution_group(self, task_to_execute: SwarmTask):
        """
        setup_execution_group

        Sets up an execution group to execute the given task.

        @param task_to_execute [SwarmTask] The task to execute

        @return None
        """
        req_num_bots = task_to_execute.get_req_num_bots()
        bots_to_execute = self.idle_bots[:req_num_bots]
        self.idle_bots = self.idle_bots[req_num_bots:]

        for root_bot_id in bots_to_execute:
            for leaf_bot_id in bots_to_execute:
                if root_bot_id != leaf_bot_id:
                    if not self.network_nodes[root_bot_id].is_connected_to(leaf_bot_id):
                        self.network_nodes[root_bot_id].connect_to_network_node(self.network_nodes[leaf_bot_id])

        for bot_id in bots_to_execute:
            self.network_nodes[bot_id].receive_task(task_to_execute)
