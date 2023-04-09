import time
import collections

from swarm.swarm_memory.local_swarm_memory import LocalSwarmMemory
from swarm.message_types import MessageTypes


class SwarmMemoryInterface(object):
    def __init__(self, executor_interface, swarm_memory_optimization_operation_threshold, key_count_threshold):
        self.executor_interface = executor_interface
        self.swarm_memory_optimization_operation_threshold = swarm_memory_optimization_operation_threshold

        self.local_swarm_memory = LocalSwarmMemory(key_count_threshold)

        self.data_to_holder_id_map = {}
        self.local_usage_stats = {}

        self.executor_interface.assign_msg_handler(str(MessageTypes.REQUEST_SWARM_MEMORY_READ), self.handle_request_swarm_memory_read_message)
        self.executor_interface.assign_msg_handler(str(MessageTypes.SWARM_MEMORY_OBJECT_LOCATION), self.handle_swarm_memory_object_location_message)
        self.executor_interface.assign_msg_handler(str(MessageTypes.UPDATE_SWARM_MEMORY_VALUE), self.handle_update_swarm_memory_value_message)
        self.executor_interface.assign_msg_handler(str(MessageTypes.DELETE_FROM_SWARM_MEMORY), self.handle_delete_from_swarm_memory_message)
        self.executor_interface.assign_msg_handler(str(MessageTypes.REQUEST_NEW_HOLDER), self.handle_request_new_holder_message)
        self.executor_interface.assign_msg_handler(str(MessageTypes.TRANSFER_SWARM_MEMORY_CONTENTS), self.handle_transfer_swarm_memory_contents_message)
        self.executor_interface.assign_msg_handler(str(MessageTypes.REMOVE_SWARM_MEMORY_OBJECT_LOCATION), self.handle_remove_swarm_memory_object_location_message)
        self.executor_interface.assign_msg_handler(str(MessageTypes.BOT_TEARDOWN), self.handle_bot_teardown)

    def get_local_contents(self):
        return self.local_swarm_memory.get_contents()

    def get_swarm_memory_usage_stats(self):
        return self.local_usage_stats

    def get_data_to_holder_id_map(self):
        return self.data_to_holder_id_map

    def create_swarm_memory_entry(self, path_to_create, value):
        self.update_swarm_memory(path_to_create, value)

        if self.local_swarm_memory.is_full():
            lru_path = min(self.local_usage_stats, key=lambda k: self.local_usage_stats[k]["TIME_OF_LAST_ACCESS"])
            self.executor_interface.send_propagation_message(
                MessageTypes.REQUEST_NEW_HOLDER,
                {"PATH": lru_path, "VALUE": self.__run_local_read(lru_path, self.executor_interface.get_id())}
            )
            self.__run_local_delete(lru_path)

        self.__run_local_create(path_to_create, value)

    def read_from_swarm_memory(self, path_to_read):
        bots_with_obj = self.__get_key_holder_ids(path_to_read)
        final_value = None
        for bot_id in bots_with_obj:
            curr_value = None
            if bot_id == self.executor_interface.get_id():
                curr_value = self.__run_local_read(path_to_read, self.executor_interface.get_id())
            else:
                response = self.executor_interface.send_sync_directed_message(
                    bot_id,
                    MessageTypes.REQUEST_SWARM_MEMORY_READ,
                    {"PATH_TO_READ": path_to_read}
                )
                curr_value = response.get_message_payload()["OBJECT_VALUE"]

            if isinstance(final_value, dict):
                final_value = self.__merge_dicts(final_value, curr_value)
            else:
                final_value = curr_value
        return final_value

    def update_swarm_memory(self, path_to_update, new_value):
        update_ids = self.__get_key_holder_ids(path_to_update)
        for curr_id in update_ids:
            if curr_id == self.executor_interface.get_id():
                self.__run_local_update(path_to_update, new_value, self.executor_interface.get_id())
            else:
                self.executor_interface.send_directed_message(
                    curr_id,
                    MessageTypes.UPDATE_SWARM_MEMORY_VALUE,
                    {"PATH_TO_UPDATE": path_to_update, "NEW_VALUE": new_value}
                )

    def delete_from_swarm_memory(self, path_to_delete):
        self.__run_local_delete(path_to_delete)
        self.__delete_from_data_holder_map(path_to_delete)
        self.executor_interface.send_propagation_message(
            MessageTypes.DELETE_FROM_SWARM_MEMORY,
            {"PATH_TO_DELETE": path_to_delete}
        )

    def handle_swarm_memory_object_location_message(self, message):
        msg_payload = message.get_message_payload()
        path = msg_payload["PATH"]
        location_ids = msg_payload["LOCATION_IDS"]
        for id in location_ids:
            self.add_data_holder(path, id)

    def handle_request_swarm_memory_read_message(self, message):
        msg_payload = message.get_message_payload()
        path_to_read = msg_payload["PATH_TO_READ"]

        if self.local_swarm_memory.has_path(path_to_read):
            object_value = self.__run_local_read(path_to_read, msg_payload["ORIGINAL_SENDER_ID"])
            self.executor_interface.respond_to_message(
                message,
                {
                    "OBJECT_VALUE": object_value
                }
            )

    def handle_update_swarm_memory_value_message(self, message):
        message_payload = message.get_message_payload()
        path_to_update = message_payload["PATH_TO_UPDATE"]
        new_value = message_payload["NEW_VALUE"]

        if self.local_swarm_memory.has_path(path_to_update):
            self.__run_local_update(path_to_update, new_value, message_payload["ORIGINAL_SENDER_ID"])

    def handle_delete_from_swarm_memory_message(self, message):
        msg_payload = message.get_message_payload()
        path_to_delete = msg_payload["PATH_TO_DELETE"]

        print(path_to_delete + "\n")

        self.__delete_from_data_holder_map(path_to_delete)

        if self.local_swarm_memory.has_path(path_to_delete):
            print("DELETING FROM MEM")
            self.__run_local_delete(path_to_delete)

    def handle_request_new_holder_message(self, message):
        payload = message.get_message_payload()
        path = payload["PATH"]
        value = payload["VALUE"]

        if not self.local_swarm_memory.is_full():
            self.__run_local_create(path, value)
        self.__remove_data_holder(path, payload["ORIGINAL_SENDER_ID"])

    def handle_transfer_swarm_memory_contents_message(self, message):
        message_payload = message.get_message_payload()
        contents = self.__flatten(message_payload["SWARM_MEMORY_CONTENTS"])
        for path, value in contents.items():
            self.create_swarm_memory_entry(path, value)

    def handle_remove_swarm_memory_object_location_message(self, message):
        payload = message.get_message_payload()
        path = payload["PATH"]
        id_to_remove = payload["ID_TO_REMOVE"]
        self.__remove_data_holder(path, id_to_remove)

    def handle_bot_teardown(self, message):
        bot_to_remove = message.get_message_payload()["BOT_ID"]

        for path in self.local_usage_stats:
            if bot_to_remove in self.local_usage_stats[path]["ACCESSES"]:
                self.local_usage_stats[path]["ACCESSES"].pop(bot_to_remove)

        flat_data_to_holder_id_map = self.__flatten(self.data_to_holder_id_map)
        for path, _ in flat_data_to_holder_id_map.items():
            self.__remove_data_holder(path, bot_to_remove)

    def add_data_holder(self, path, id_to_add):
        path_components = path.split("/")
        curr_dict = self.data_to_holder_id_map
        for i in range(len(path_components)):
            key = path_components[i]
            if i == len(path_components) - 1:
                if key not in curr_dict:
                    curr_dict[key] = []
                curr_dict[key].append(id_to_add)
            else:
                if key not in curr_dict:
                    curr_dict[key] = {}
                if (not isinstance(curr_dict[key], dict)):
                    curr_dict[key] = {}
                curr_dict = curr_dict[key]

    def __remove_data_holder(self, path, id_to_remove):
        path_components = path.split("/")
        curr_dict = self.data_to_holder_id_map
        for i in range(len(path_components)):
            key = path_components[i]
            if i == len(path_components) - 1:
                if key in curr_dict:
                    if id_to_remove in curr_dict[key]:
                        curr_dict[key].remove(id_to_remove)
                    if len(curr_dict[key]) == 0:
                        curr_dict.pop(key)
            else:
                if key not in curr_dict:
                    return False
                curr_dict = curr_dict[key]

    def __delete_from_data_holder_map(self, path):
        path_components = path.split("/")
        curr_dict = self.data_to_holder_id_map
        for i in range(len(path_components)):
            key = path_components[i]
            if i == len(path_components) - 1:
                if key in curr_dict:
                    curr_dict.pop(key)
            else:
                if key not in curr_dict:
                    return False
                curr_dict = curr_dict[key]

    def __get_key_holder_ids(self, path):
        path_components = path.split("/")
        curr_dict = self.data_to_holder_id_map
        for i in range(len(path_components)):
            key = path_components[i]
            if key not in curr_dict:
                return []
            curr_dict = curr_dict[key]
        if isinstance(curr_dict, dict):
            return list(set(self.__get_nested_dict_values(curr_dict)))
        else:
            return curr_dict

    def __get_nested_dict_values(self, dict_to_parse):
        final_ids = []
        for value in dict_to_parse.values():
            if isinstance(value, dict):
                final_ids += self.__get_nested_dict_values(value)
            else:
                final_ids += value
        return final_ids

    def __merge_dicts(self, a, b, path=None):
        if path is None:
            path = []
        for key in b:
            if key in a:
                if isinstance(a[key], dict) and isinstance(b[key], dict):
                    self.__merge_dicts(a[key], b[key], path + [str(key)])
                elif a[key] == b[key]:
                    pass
                else:
                    raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
            else:
                a[key] = b[key]
        return a

    def __run_local_create(self, path_to_create, value):
        self.local_swarm_memory.create(path_to_create, value)
        self.local_usage_stats[path_to_create] = {
            "ACCESSES": {},
            "TIME_OF_LAST_ACCESS": float(time.time())
        }
        self.add_data_holder(path_to_create, self.executor_interface.get_id())
        self.executor_interface.send_propagation_message(
            MessageTypes.SWARM_MEMORY_OBJECT_LOCATION,
            {"PATH": path_to_create, "LOCATION_IDS": [self.executor_interface.get_id()]}
        )

    def __run_local_read(self, path_to_read, reader_id):
        curr_value = self.local_swarm_memory.read(path_to_read)
        self.__add_access(path_to_read, reader_id)
        return curr_value

    def __run_local_update(self, path_to_update, new_value, updater_id):
        self.local_swarm_memory.update(path_to_update, new_value)
        self.__add_access(path_to_update, updater_id)

    def __run_local_delete(self, path_to_delete):
        self.local_swarm_memory.delete(path_to_delete)
        if path_to_delete in self.local_usage_stats:
            self.local_usage_stats.pop(path_to_delete)
        self.__remove_data_holder(path_to_delete, self.executor_interface.get_id())

    def __add_access(self, path, bot_id):
        sub_paths = self.local_swarm_memory.get_child_paths_from_parent_path(path)
        for path in sub_paths:
            if bot_id not in self.local_usage_stats[path]["ACCESSES"]:
                self.local_usage_stats[path]["ACCESSES"][bot_id] = 0
            self.local_usage_stats[path]["ACCESSES"][bot_id] += 1
            self.local_usage_stats[path]["TIME_OF_LAST_ACCESS"] = float(time.time())

            access_dict = self.local_usage_stats[path]["ACCESSES"]
            if sum(access_dict.values()) == self.swarm_memory_optimization_operation_threshold:
                print("HERE")
                ids_with_max_reads = [k for k, v in access_dict.items() if v == max(access_dict.values())]
                for curr_id in ids_with_max_reads:
                    if curr_id != self.executor_interface.get_id():
                        self.executor_interface.send_directed_message(
                            curr_id,
                            MessageTypes.TRANSFER_SWARM_MEMORY_CONTENTS,
                            {"SWARM_MEMORY_CONTENTS": {path: self.__run_local_read(path, self.executor_interface.get_id())}}
                        )
                if self.executor_interface.get_id() not in ids_with_max_reads:
                    self.__run_local_delete(path)
                    self.executor_interface.send_propagation_message(
                        MessageTypes.REMOVE_SWARM_MEMORY_OBJECT_LOCATION,
                        {"PATH": path, "ID_TO_REMOVE": self.executor_interface.get_id()}
                    )
                else:
                    self.local_usage_stats[path]["ACCESSES"] = {}

    def __flatten(self, dictionary, parent_key=False, separator='/'):
        items = []
        for key, value in dictionary.items():
            new_key = str(parent_key) + separator + key if parent_key else key
            if isinstance(value, collections.abc.MutableMapping):
                items.extend(self.__flatten(value, new_key, separator).items())
            else:
                items.append((new_key, value))
        return dict(items)
