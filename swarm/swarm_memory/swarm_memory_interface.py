from swarm.swarm_memory.local_swarm_memory import LocalSwarmMemory
from swarm.message_types import MessageTypes


class SwarmMemoryInterface(object):
    def __init__(self, executor_interface):
        self.executor_interface = executor_interface
        self.local_swarm_memory = LocalSwarmMemory(executor_interface.get_id())

    def write_to_swarm_memory(self, key_to_write, value_to_write, data_type):
        self.local_swarm_memory.write(key_to_write, value_to_write, data_type)
        self.executor_interface.send_propagation_message(
            MessageTypes.SWARM_MEMORY_OBJECT_LOCATION,
            {"OBJECT_ID": key_to_write, "LOCATION_ID": self.executor_interface.get_id(), "DATA_TYPE": data_type}
        )

    def read_from_swarm_memory(self, key_to_read):
        bot_with_obj = self.local_swarm_memory.get_data_holder_id(key_to_read)
        value = None
        if bot_with_obj == self.executor_interface.get_id():
            value = self.local_swarm_memory.read(key_to_read)
        elif bot_with_obj is not None:
            response = self.executor_interface.send_directed_message(
                bot_with_obj,
                MessageTypes.REQUEST_SWARM_MEMORY_READ,
                {"KEY_TO_READ": key_to_read},
                True
            )

            value = response.get_message_payload()["OBJECT_VALUE"]
        else:
            # else in this case means that the there is no key with the given value in the swarm memory
            pass
        return value

    def pop_from_swarm_memory(self, key_to_pop):
        bot_with_obj = self.local_swarm_memory.get_data_holder_id(key_to_pop)
        value = None
        if bot_with_obj == self.executor_interface.get_id():
            value = self.local_swarm_memory.read(key_to_pop)
            self.local_swarm_memory.delete(key_to_pop)
            self.executor_interface.send_propagation_message(
                MessageTypes.DELETE_FROM_SWARM_MEMORY,
                {"KEY_TO_DELETE": key_to_pop}
            )
        elif bot_with_obj is not None:
            self.local_swarm_memory.delete(key_to_pop)
            response = self.executor_interface.send_directed_message(
                bot_with_obj,
                MessageTypes.POP_FROM_SWARM_MEMORY,
                {"KEY_TO_POP": key_to_pop},
                True
            )
            value = response.get_message_payload()["OBJECT_VALUE"]
            self.executor_interface.send_propagation_message(
                MessageTypes.DELETE_FROM_SWARM_MEMORY,
                {"KEY_TO_DELETE": key_to_pop}
            )
        else:
            # else in this case means that the there is no key with the given value in the swarm memory
            pass

        return value

    def get_ids_of_contents_of_type(self, type_to_get):
        return self.local_swarm_memory.get_ids_of_contents_of_type(type_to_get)

    def handle_swarm_memory_object_location_message(self, message):
        msg_payload = message.get_message_payload()
        object_id = msg_payload["OBJECT_ID"]
        location_id = msg_payload["LOCATION_ID"]
        data_type = msg_payload["DATA_TYPE"]
        self.local_swarm_memory.update_data_holder(object_id, location_id, data_type)

    def handle_request_swarm_memory_read_message(self, message):
        msg_payload = message.get_message_payload()
        object_key = msg_payload["KEY_TO_READ"]

        object_value = self.local_swarm_memory.read(object_key)

        self.executor_interface.respond_to_message(message, {"OBJECT_VALUE": object_value})

    def handle_pop_from_swarm_memory_message(self, message):
        msg_payload = message.get_message_payload()
        key_to_pop = msg_payload["KEY_TO_POP"]

        object_value = self.local_swarm_memory.read(key_to_pop)

        self.executor_interface.respond_to_message(message, {"OBJECT_VALUE": object_value})

        self.local_swarm_memory.delete(key_to_pop)

    def handle_delete_from_swarm_memory_message(self, message):
        msg_payload = message.get_message_payload()
        key_to_delete = msg_payload["KEY_TO_DELETE"]

        self.local_swarm_memory.delete(key_to_delete)
