from network_manager.network_manager import NetworkManager
from network_manager.network_connectivity_level import NetworkConnectivityLevel


class SwarmManager(NetworkManager):
    def __init__(self, network_connectivity_level: NetworkConnectivityLevel):
        NetworkManager.__init__(self, network_connectivity_level)

        self.task_queue = []
        self.idle_bots = []

    def get_idle_bots(self):
        return self.idle_bots

    def add_network_node(self, new_node) -> None:
        super().add_network_node(new_node)
        self.idle_bots.append(new_node.get_id())
        self.check_for_available_task_executors()

    def receive_task(self, new_task):
        self.task_queue.append(new_task)
        self.check_for_available_task_executors()

    def notify_idle_state(self, bot_id, bot_idle: bool) -> None:
        super().notify_idle_state(bot_id, bot_idle)
        if bot_idle:
            if bot_id not in self.idle_bots:
                self.idle_bots.append(bot_id)
        else:
            if bot_id in self.idle_bots:
                self.idle_bots.remove(bot_id)

        self.check_for_available_task_executors()

    def check_for_available_task_executors(self):
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

    def setup_execution_group(self, task_to_execute):
        req_num_bots = task_to_execute.get_req_num_bots()
        bots_to_execute = self.idle_bots[:req_num_bots]
        self.idle_bots = self.idle_bots[req_num_bots:]

        for bot_id in bots_to_execute:
            self.network_nodes[bot_id].receive_task(task_to_execute)
