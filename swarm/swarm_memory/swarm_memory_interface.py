import random
import collections
import time

from network_manager.network_node.network_node_message_types import NetworkNodeMessageTypes
from swarm.swarm_memory.local_swarm_memory_entry import LocalSwarmMemoryEntry
from swarm.swarm_memory.swarm_memory_message_types import SwarmMemoryMessageTypes


class SwarmMemoryInterface(object):
    def __init__(self, executor_interface, swarm_memory_optimization_operation_threshold, key_count_threshold):
        self.executor_interface = executor_interface
        self.swarm_memory_optimization_operation_threshold = swarm_memory_optimization_operation_threshold
        self.key_count_threshold = key_count_threshold

        self.swarm_memory_contents = {}
        self.local_usage_stats = {}
        self.path_watchers = {}
        self.deleted_items = {}

        self.executor_interface.assign_msg_handler(
            str(NetworkNodeMessageTypes.REQUEST_CONNECTION),
            self.swarm_memory_interface_handle_request_connection_message
        )
        self.executor_interface.assign_msg_handler(
            str(NetworkNodeMessageTypes.BOT_TEARDOWN),
            self.swarm_memory_interface_handle_bot_teardown_message
        )

        self.executor_interface.assign_msg_handler(
            str(SwarmMemoryMessageTypes.SYNC_SWARM_MEMORY),
            self.swarm_memory_interface_handle_sync_swarm_memory_message
        )
        self.executor_interface.assign_msg_handler(
            str(SwarmMemoryMessageTypes.NEW_HOLDER_ID),
            self.swarm_memory_interface_handle_new_holder_id_message
        )
        self.executor_interface.assign_msg_handler(
            str(SwarmMemoryMessageTypes.DELETE_FROM_SWARM_MEMORY),
            self.swarm_memory_interface_handle_delete_from_swarm_memory_message
        )
        self.executor_interface.assign_msg_handler(
            str(SwarmMemoryMessageTypes.REQUEST_NEW_HOLDER),
            self.swarm_memory_interface_handle_request_new_holder_message
        )
        self.executor_interface.assign_msg_handler(
            str(SwarmMemoryMessageTypes.REQUEST_READ),
            self.swarm_memory_interface_handle_request_read_message
        )

    def teardown(self):
        known_bots = self.executor_interface.get_known_bot_ids()
        if len(known_bots) <= 0:
            return None

        all_paths = self._flatten(self.swarm_memory_contents)
        for path in all_paths:
            if self._has_path_saved_locally(path):
                self.executor_interface.send_directed_message(
                    random.choice(self.executor_interface.get_known_bot_ids()),
                    SwarmMemoryMessageTypes.REQUEST_NEW_HOLDER,
                    {"PATH": path, "BLOCKCHAIN": self._get_obj_at_path(path).get_change_blocks()}
                )
            self._get_obj_at_path(path).teardown()

        self.executor_interface.unassign_msg_handler(
            str(NetworkNodeMessageTypes.REQUEST_CONNECTION),
            self.swarm_memory_interface_handle_request_connection_message
        )
        self.executor_interface.unassign_msg_handler(
            str(NetworkNodeMessageTypes.BOT_TEARDOWN),
            self.swarm_memory_interface_handle_bot_teardown_message
        )

        self.executor_interface.unassign_msg_handler(
            str(SwarmMemoryMessageTypes.SYNC_SWARM_MEMORY),
            self.swarm_memory_interface_handle_sync_swarm_memory_message
        )
        self.executor_interface.unassign_msg_handler(
            str(SwarmMemoryMessageTypes.NEW_HOLDER_ID),
            self.swarm_memory_interface_handle_new_holder_id_message
        )
        self.executor_interface.unassign_msg_handler(
            str(SwarmMemoryMessageTypes.DELETE_FROM_SWARM_MEMORY),
            self.swarm_memory_interface_handle_delete_from_swarm_memory_message
        )
        self.executor_interface.unassign_msg_handler(
            str(SwarmMemoryMessageTypes.REQUEST_NEW_HOLDER),
            self.swarm_memory_interface_handle_request_new_holder_message
        )
        self.executor_interface.unassign_msg_handler(
            str(SwarmMemoryMessageTypes.REQUEST_READ),
            self.swarm_memory_interface_handle_request_read_message
        )

    def write(self, path_to_write, value):
        if (not self.path_exists_in_memory(path_to_write)) or \
                (not isinstance(self._get_obj_at_path(path_to_write), LocalSwarmMemoryEntry)):
            self._initialize_path_in_memory(path_to_write)

        if not self._has_path_saved_locally(path_to_write):
            if self._local_memory_is_full():
                self._make_space_in_memory()
            self._set_save_path_locally(path_to_write, True)

        self._get_obj_at_path(path_to_write).create_change_block(value)

        self._check_path_watchers(path_to_write)

    def read(self, path_to_read):
        if not self.path_exists_in_memory(path_to_read):
            return None

        return self._unwrap(self._get_obj_at_path(path_to_read))

    def delete(self, path_to_delete):
        self.deleted_items[path_to_delete] = time.time()

        self._run_local_delete(path_to_delete)

        self.executor_interface.send_propagation_message(
            SwarmMemoryMessageTypes.DELETE_FROM_SWARM_MEMORY,
            {"PATH_TO_DELETE": path_to_delete, "TIME_ISSUED": self.deleted_items[path_to_delete]}
        )

        if path_to_delete in self.local_usage_stats:
            self.local_usage_stats.pop(path_to_delete)

        self._check_path_watchers(path_to_delete)

    def path_exists_in_memory(self, path_to_check):
        return self._get_obj_at_path(path_to_check) is not None

    def add_path_watcher(self, path_to_watch, method_to_call):
        if path_to_watch not in self.path_watchers:
            self.path_watchers[path_to_watch] = []
        self.path_watchers[path_to_watch].append(method_to_call)

    def get_local_swarm_memory_contents(self):
        local_contents = {}
        all_paths = self._flatten(self.swarm_memory_contents)
        for path in all_paths:
            if self._has_path_saved_locally(path):
                local_contents[path] = self._get_obj_at_path(path).get_inner_value()
        return local_contents

    def swarm_memory_interface_handle_request_connection_message(self, message):
        new_id = message.get_sender_id()
        self.executor_interface.send_directed_message(
            new_id,
            SwarmMemoryMessageTypes.SYNC_SWARM_MEMORY,
            {"SWARM_MEMORY_REF": self._get_data_to_holder_id_map(self.swarm_memory_contents)}
        )

    def swarm_memory_interface_handle_sync_swarm_memory_message(self, message):
        message_payload = message.get_message_payload()
        swarm_memory_ref = message_payload["SWARM_MEMORY_REF"]
        flattend_ref = self._flatten(swarm_memory_ref)
        for path, holders in flattend_ref.items():
            if (not self.path_exists_in_memory(path)) or (not isinstance(self._get_obj_at_path(path), LocalSwarmMemoryEntry)):
                self._initialize_path_in_memory(path)
            entry = self._get_obj_at_path(path)
            for holder in holders:
                entry.add_holder_id(holder.get_holder_id(), holder.get_time_added())
            self._check_path_watchers(path)

        final_swarm_memory_ref = self._flatten(self._get_data_to_holder_id_map(self.swarm_memory_contents))
        for path, holders in final_swarm_memory_ref.items():
            for holder in holders:
                self.executor_interface.send_propagation_message(
                    SwarmMemoryMessageTypes.NEW_HOLDER_ID,
                    {"PATH": path, "HOLDER_ID": holder.get_holder_id(), "TIME_ISSUED": holder.get_time_added()}
                )

    def swarm_memory_interface_handle_new_holder_id_message(self, message):
        message_payload = message.get_message_payload()
        path = message_payload["PATH"]
        holder_id = message_payload["HOLDER_ID"]
        time_issued = message_payload["TIME_ISSUED"]

        if (path not in self.deleted_items) or (time_issued > self.deleted_items[path]):
            if (not self.path_exists_in_memory(path)) or \
                    (not isinstance(self._get_obj_at_path(path), LocalSwarmMemoryEntry)):
                self._initialize_path_in_memory(path)
            self._get_obj_at_path(path).add_holder_id(holder_id, time_issued)
            self._check_path_watchers(path)

    def swarm_memory_interface_handle_delete_from_swarm_memory_message(self, message):
        message_payload = message.get_message_payload()
        path_to_delete = message_payload["PATH_TO_DELETE"]
        time_issued = message_payload["TIME_ISSUED"]

        self.deleted_items[path_to_delete] = time_issued
        
        self._run_local_delete(path_to_delete)

        if path_to_delete in self.local_usage_stats:
            self.local_usage_stats.pop(path_to_delete)

        self._check_path_watchers(path_to_delete)

    def swarm_memory_interface_handle_request_new_holder_message(self, message):
        message_payload = message.get_message_payload()
        path_to_write = message_payload["PATH"]
        blockchain = message_payload["BLOCKCHAIN"]

        if (not self.path_exists_in_memory(path_to_write)) or \
                (not isinstance(self._get_obj_at_path(path_to_write), LocalSwarmMemoryEntry)):
            self._initialize_path_in_memory(path_to_write)

        if (not self._local_memory_is_full()) and (not self._has_path_saved_locally(path_to_write)):
            self._set_save_path_locally(path_to_write, True)

            local_entry = self._get_obj_at_path(path_to_write)
            for block in blockchain:
                local_entry.add_change_block(block)

            self._check_path_watchers(path_to_write)

    def swarm_memory_interface_handle_bot_teardown_message(self, message):
        bot_to_remove = message.get_message_payload()["BOT_ID"]

        for path in self.local_usage_stats:
            if bot_to_remove in self.local_usage_stats[path]["ACCESSES"]:
                self.local_usage_stats[path]["ACCESSES"].pop(bot_to_remove)

        final_swarm_memory_ref = self._flatten(self.swarm_memory_contents)
        for path in final_swarm_memory_ref:
            self._get_obj_at_path(path).remove_holder_id(bot_to_remove)

    def swarm_memory_interface_handle_request_read_message(self, message):
        message_payload = message.get_message_payload()
        path = message_payload["PATH"]
        bot_id = message_payload["ORIGINAL_SENDER_ID"]
        self._add_access(path, bot_id)
        obj_at_path = self._get_obj_at_path(path)
        if (obj_at_path is None) or isinstance(obj_at_path, dict):
            self.executor_interface.respond_to_message(
                message,
                {"INNER_VALUE": None}
            )

    def _add_access(self, path, bot_id):
        if self._has_path_saved_locally(path):
            time_issued = time.time()
            if bot_id not in self.local_usage_stats[path]["ACCESSES"]:
                self.local_usage_stats[path]["ACCESSES"][bot_id] = 0
            self.local_usage_stats[path]["ACCESSES"][bot_id] += 1
            self.local_usage_stats[path]["TIME_OF_LAST_ACCESS"] = float(time_issued)

            access_dict = self.local_usage_stats[path]["ACCESSES"]
            if sum(access_dict.values()) == self.swarm_memory_optimization_operation_threshold:
                ids_with_max_reads = [k for k, v in access_dict.items() if v == max(access_dict.values())]
                for curr_id in ids_with_max_reads:
                    if curr_id != self.executor_interface.get_id():
                        self.executor_interface.send_directed_message(
                            curr_id,
                            SwarmMemoryMessageTypes.REQUEST_NEW_HOLDER,
                            {"PATH": path, "BLOCKCHAIN": self._get_obj_at_path(path).get_change_blocks()}
                        )
                self.local_usage_stats[path]["ACCESSES"] = {}

    def _get_data_to_holder_id_map(self, map_to_convert):
        if isinstance(map_to_convert, dict):
            ret_dict = {}
            for key, value in map_to_convert.items():
                if isinstance(value, dict):
                    ret_dict[key] = self._get_data_to_holder_id_map(value)
                else:
                    ret_dict[key] = value.get_holders()
            return ret_dict
        else:
            return map_to_convert.get_holders()

    def _check_path_watchers(self, path_to_check):
        for path in self.path_watchers:
            if path_to_check.startswith(path):
                obj_at_path = self._get_obj_at_path(path)
                snapshot = None
                if isinstance(obj_at_path, dict):
                    snapshot = list(obj_at_path.keys())
                else:
                    snapshot = obj_at_path.read()
                for handler in self.path_watchers[path]:
                    handler(path_to_check, snapshot)

    def _flatten(self, dictionary, parent_key=False, separator='/'):
        items = []
        for key, value in dictionary.items():
            new_key = str(parent_key) + separator + key if parent_key else key
            if isinstance(value, collections.abc.MutableMapping):
                items.extend(self._flatten(value, new_key, separator).items())
            else:
                items.append((new_key, value))
        return dict(items)

    def _get_obj_at_path(self, path):
        path_components = path.split("/")
        curr_dict = self.swarm_memory_contents
        for i in range(len(path_components)):
            key = path_components[i]
            if (not isinstance(curr_dict, dict)) or (key not in curr_dict):
                return None
            curr_dict = curr_dict[key]
        return curr_dict

    def _unwrap(self, value_to_unwrap):
        if isinstance(value_to_unwrap, dict):
            ret_dict = {}
            keys = list(value_to_unwrap.keys())
            for key in keys:
                if key in value_to_unwrap:
                    value = value_to_unwrap[key]
                    if isinstance(value, dict):
                        ret_dict[key] = self._unwrap(value)
                    else:
                        ret_dict[key] = value.read()
                        if value.is_saved_locally():
                            self._add_access(value.get_path(), self.executor_interface.get_id())
            return ret_dict
        else:
            if value_to_unwrap.is_saved_locally():
                self._add_access(value_to_unwrap.get_path(), self.executor_interface.get_id())
            return value_to_unwrap.read()

    def _set_save_path_locally(self, path, new_save_state):
        if not self.path_exists_in_memory(path):
            return False

        if new_save_state:
            self.local_usage_stats[path] = {
                "ACCESSES": {},
                "TIME_OF_LAST_ACCESS": float(time.time())
            }
        else:
            if path in self.local_usage_stats:
                self.local_usage_stats.pop(path)

        self._get_obj_at_path(path).set_saved_locally(new_save_state)

    def _get_usage_percentage(self):
        def count(dict_to_count):
            final_count = 0
            for key, value in dict_to_count.items():
                if isinstance(value, dict):
                    final_count += count(value)
                else:
                    if value.is_saved_locally():
                        final_count += 1
            return final_count

        return (float(count(self.swarm_memory_contents)) / float(self.swarm_memory_optimization_operation_threshold)) * 100

    def _make_space_in_memory(self):
        lru_path = min(self.local_usage_stats, key=lambda k: self.local_usage_stats[k]["TIME_OF_LAST_ACCESS"])
        self.executor_interface.send_propagation_message(
            SwarmMemoryMessageTypes.REQUEST_NEW_HOLDER,
            {"PATH": lru_path, "BLOCKCHAIN": self._get_obj_at_path(lru_path).get_change_blocks()}
        )
        self._set_save_path_locally(lru_path, False)

    def _local_memory_is_full(self):
        return self._get_usage_percentage() == 100

    def _has_path_saved_locally(self, path_to_check):
        if not self.path_exists_in_memory(path_to_check):
            return False

        return self._get_obj_at_path(path_to_check).is_saved_locally()

    def _initialize_path_in_memory(self, path_to_create):
        path_components = path_to_create.split("/")
        curr_dict = self.swarm_memory_contents
        for i in range(len(path_components)):
            key = path_components[i]
            if i == len(path_components) - 1:
                curr_dict[key] = LocalSwarmMemoryEntry(self.executor_interface, path_to_create)
            else:
                if key not in curr_dict:
                    curr_dict[key] = {}
                if (not isinstance(curr_dict[key], dict)):
                    curr_path = ""
                    for j in range(i + 1):
                        if curr_path != "":
                            curr_path += "/"
                        curr_path += path_components[j]
                    if self._has_path_saved_locally(curr_path):
                        self._set_save_path_locally(curr_path, False)
                    curr_dict[key].teardown()
                    curr_dict[key] = {}
                curr_dict = curr_dict[key]

    def _run_local_delete(self, path_to_delete):
        if self.path_exists_in_memory(path_to_delete):
            path_components = path_to_delete.split("/")
            curr_dict = self.swarm_memory_contents
            for i in range(len(path_components) - 1):
                key = path_components[i]
                curr_dict = curr_dict[key]

            if isinstance(curr_dict[path_components[-1]], LocalSwarmMemoryEntry):
                curr_dict[path_components[-1]].teardown()
            curr_dict.pop(path_components[-1])

