class ExecutorInterface(object):
    def __init__(self, swarm_bot):
        self.swarm_bot = swarm_bot

    def get_id(self):
        return self.swarm_bot.get_id()

    def send_propagation_message(self, message_type, message_payload):
        return self.swarm_bot.send_propagation_message(message_type, message_payload)

    def send_directed_message(self, target_node_id, message_type, message_payload):
        return self.swarm_bot.send_directed_message(target_node_id, message_type, message_payload)

    def send_sync_directed_message(self, target_bot_id, message_type, message_payload):
        return self.swarm_bot.send_sync_directed_message(target_bot_id, message_type, message_payload)

    def respond_to_message(self, message, message_payload):
        return self.swarm_bot.respond_to_message(message, message_payload)

    def get_id_with_shortest_path_from_list(self, list_of_ids):
        return self.swarm_bot.get_id_with_shortest_path_from_list(list_of_ids)

    def assign_msg_handler(self, msg_type: str, handler: object):
        return self.swarm_bot.assign_msg_handler(msg_type, handler)

    def get_known_bot_ids(self):
        return self.swarm_bot.get_known_bot_ids()

    def add_path_watcher(self, path_to_watch, method_to_call):
        return self.swarm_bot.add_path_watcher(path_to_watch, method_to_call)

    def _notify_process_state(self, process_running):
        return self.swarm_bot._notify_process_state(process_running)
    
    def write_to_swarm_memory(self, path_to_write, value_to_write):
        return self.swarm_bot.write_to_swarm_memory(path_to_write, value_to_write)

    def read_from_swarm_memory(self, path_to_read):
        return self.swarm_bot.read_from_swarm_memory(path_to_read)

    def delete_from_swarm_memory(self, path_to_delete):
        return self.swarm_bot.delete_from_swarm_memory(path_to_delete)

    def unassign_msg_handler(self, msg_type, handler):
        return self.swarm_bot.unassign_msg_handler(msg_type, handler)
    
    def receive_task_bundle(self, new_task_bundle, listener_bot_id=None):
        return self.swarm_bot.receive_task_bundle(new_task_bundle, listener_bot_id=listener_bot_id)
