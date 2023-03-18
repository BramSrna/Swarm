from swarm.swarm_memory.local_swarm_memory import LocalSwarmMemory
from swarm.message_types import MessageTypes


class SwarmMemoryInterface(object):
    def __init__(self, executor_interface):
        self.executor_interface = executor_interface
        self.local_swarm_memory = LocalSwarmMemory(executor_interface.get_id())

    def write_to_swarm_memory(self, path_to_write, value_to_write):
        self.local_swarm_memory.write(path_to_write, value_to_write)
        self.executor_interface.send_propagation_message(
            MessageTypes.SWARM_MEMORY_OBJECT_LOCATION,
            {"PATH": path_to_write, "LOCATION_ID": self.executor_interface.get_id()}
        )

    def read_from_swarm_memory(self, path_to_read):
        bots_with_obj = self.local_swarm_memory.get_key_holder_ids(path_to_read)
        print(bots_with_obj)
        final_value = None
        for bot_id in bots_with_obj:
            curr_value = None
            if bot_id == self.executor_interface.get_id():
                curr_value = self.local_swarm_memory.read(path_to_read)
            else:
                response = self.executor_interface.send_sync_directed_message(
                    bot_id,
                    MessageTypes.REQUEST_SWARM_MEMORY_READ,
                    {"PATH_TO_READ": path_to_read}
                )
                curr_value = response.get_message_payload()["OBJECT_VALUE"]

            if isinstance(final_value, dict):
                final_value = self.merge_dicts(final_value, curr_value)
            else:
                final_value = curr_value
        return final_value

    def update_swarm_memory(self, path_to_update, new_value):
        self.write_to_swarm_memory(path_to_update, new_value)

    def pop_from_swarm_memory(self, path_to_pop):
        bots_with_obj = self.local_swarm_memory.get_key_holder_ids(path_to_pop)
        print(bots_with_obj)
        final_value = self.read_from_swarm_memory(path_to_pop)
        self.local_swarm_memory.delete(path_to_pop)
        self.executor_interface.send_propagation_message(
            MessageTypes.DELETE_FROM_SWARM_MEMORY,
            {"PATH_TO_DELETE": path_to_pop}
        )
        return final_value

    def handle_swarm_memory_object_location_message(self, message):
        msg_payload = message.get_message_payload()
        path = msg_payload["PATH"]
        location_id = msg_payload["LOCATION_ID"]
        self.local_swarm_memory.update_data_holder(path, location_id)

    def handle_request_swarm_memory_read_message(self, message):
        msg_payload = message.get_message_payload()
        path_to_read = msg_payload["PATH_TO_READ"]

        if self.local_swarm_memory.has_path(path_to_read):
            object_value = self.local_swarm_memory.read(path_to_read)
            self.executor_interface.respond_to_message(
                message,
                {
                    "OBJECT_VALUE": object_value
                }
            )

    def handle_pop_from_swarm_memory_message(self, message):
        msg_payload = message.get_message_payload()
        key_to_pop = msg_payload["KEY_TO_POP"]

        if self.local_swarm_memory.has_path(key_to_pop):
            object_info = self.local_swarm_memory.read(key_to_pop)

            self.executor_interface.respond_to_message(
                message,
                {"OBJECT_VALUE": object_info}
            )

            self.local_swarm_memory.delete(key_to_pop)

    def handle_delete_from_swarm_memory_message(self, message):
        msg_payload = message.get_message_payload()
        path_to_delete = msg_payload["PATH_TO_DELETE"]

        self.local_swarm_memory.delete(path_to_delete)

    def merge_dicts(self, a, b, path=None):
        if path is None:
            path = []
        for key in b:
            if key in a:
                if isinstance(a[key], dict) and isinstance(b[key], dict):
                    self.merge_dicts(a[key], b[key], path + [str(key)])
                elif a[key] == b[key]:
                    pass
                else:
                    raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
            else:
                a[key] = b[key]
        return a
