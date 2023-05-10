import time
import functools

from swarm.swarm_memory.swarm_memory_message_types import SwarmMemoryMessageTypes
from swarm.swarm_memory.swarm_memory_update_block import UpdateBlock


class HolderInfo(object):
    def __init__(self, holder_id, time_added):
        self.holder_id = holder_id
        self.time_added = time_added

    def get_holder_id(self):
        return self.holder_id

    def get_time_added(self):
        return self.time_added


class LocalSwarmMemoryEntry(object):
    def __init__(self, executor_interface, path):
        self.executor_interface = executor_interface
        self.path = path

        self.inner_value = None
        self.change_blocks = []
        self.future_blocks = []
        self.holders = []

        self.saved_locally = False

        self.reset_state()

        self.executor_interface.assign_msg_handler(
            str(SwarmMemoryMessageTypes.REMOVE_HOLDER_ID),
            self.local_swarm_memory_entry_handle_remove_holder_id_message
        )
        self.executor_interface.assign_msg_handler(
            str(SwarmMemoryMessageTypes.NEW_HOLDER_ID),
            self.local_swarm_memory_entry_handle_new_holder_id_message
        )
        self.executor_interface.assign_msg_handler(
            str(SwarmMemoryMessageTypes.NEW_CHANGE_BLOCK),
            self.local_swarm_memory_entry_handle_new_change_block_message
        )
        self.executor_interface.assign_msg_handler(
            str(SwarmMemoryMessageTypes.REQUEST_BLOCKCHAIN_TRANSFER),
            self.local_swarm_memory_entry_handle_request_blockchain_transfer_message
        )
        self.executor_interface.assign_msg_handler(
            str(SwarmMemoryMessageTypes.REQUEST_READ),
            self.local_swarm_memory_entry_handle_request_read_message
        )

    def get_curr_state_counter(self):
        return len(self.change_blocks)

    def reprocess_chain(self):
        self.reset_state()
        for potential_block_list in self.change_blocks:
            sorted_potential_block_list = sorted(potential_block_list, key=functools.cmp_to_key(self.compare_change_blocks))
            for block in sorted_potential_block_list:
                self.process_change_block(block)

    def add_change_block(self, new_change_block):
        if not self.is_valid_block(new_change_block):
            return False

        added_to_end = False
        expected_state = new_change_block.get_expected_state()
        if expected_state > self.get_curr_state_counter():
            if new_change_block not in self.future_blocks:
                self.future_blocks.append(new_change_block)
            return True
        elif expected_state == self.get_curr_state_counter():
            added_to_end = True
            self.change_blocks.append([])

        if new_change_block not in self.change_blocks[expected_state]:
            self.change_blocks[expected_state].append(new_change_block)

            if added_to_end:
                self.process_change_block(new_change_block)
            else:
                self.reprocess_chain()

        for block in self.future_blocks:
            if new_change_block.get_expected_state() <= self.get_curr_state_counter():
                self.future_blocks.remove(block)
                self.add_change_block(block)

        return True

    def reset_state(self):
        self.inner_value = None

    def process_change_block(self, new_block):
        self.inner_value = new_block.get_new_value()

    def is_valid_block(self, block_to_check):
        if block_to_check.get_expected_state() < 0:
            return False
        if not isinstance(block_to_check, UpdateBlock):
            return False
        return True

    def get_change_blocks(self):
        commited_block_list = []
        for potential_block_list in self.change_blocks:
            sorted_potential_block_list = sorted(potential_block_list, key=functools.cmp_to_key(self.compare_change_blocks))
            commited_block_list += sorted_potential_block_list
        return commited_block_list

    def compare_change_blocks(self, block_1, block_2):
        if block_1.get_time_issued() < block_2.get_time_issued():
            return -1
        elif block_1.get_time_issued() > block_2.get_time_issued():
            return 1
        else:
            return 0

    def get_time_path_last_updated(self, path_to_check):
        time_last_updated = None
        for block in self.change_blocks:
            if path_to_check == block.get_path():
                block_time = block.get_time_issued()
                if (time_last_updated is None) or (block_time > time_last_updated):
                    time_last_updated = block_time
        return time_last_updated

    def sync_from_swarm_shared_object(self, swarm_shared_object):
        second_chain = swarm_shared_object.get_change_blocks()
        for block in second_chain:
            self.add_change_block(block)

    def create_change_block(self, new_value):
        new_block = UpdateBlock(self.get_curr_state_counter(), time.time(), new_value)

        self.add_change_block(new_block)

        for holder in self.holders:
            holder_id = holder.get_holder_id()
            if holder_id != self.executor_interface.get_id():
                self.executor_interface.send_directed_message(
                    holder_id,
                    SwarmMemoryMessageTypes.NEW_CHANGE_BLOCK,
                    {"PATH": self.path, "NEW_BLOCK": new_block}
                )

    def is_saved_locally(self):
        return self.saved_locally

    def set_saved_locally(self, new_saved_state):
        if self.saved_locally == new_saved_state:
            return True

        self.saved_locally = new_saved_state
        if not self.saved_locally:
            self.executor_interface.send_propagation_message(
                SwarmMemoryMessageTypes.REMOVE_HOLDER_ID,
                {"PATH": self.path, "HOLDER_ID": self.executor_interface.get_id()}
            )

            self.inner_value = None
            self.change_blocks = []
            self.future_blocks = []
            self.remove_holder_id(self.executor_interface.get_id())
        else:
            time_added = time.time()
            self.sync_with_other_holders()
            self.executor_interface.send_propagation_message(
                SwarmMemoryMessageTypes.NEW_HOLDER_ID,
                {"PATH": self.path, "HOLDER_ID": self.executor_interface.get_id(), "TIME_ISSUED": time_added}
            )
            self.add_holder_id(self.executor_interface.get_id(), time_added)

    def read(self):
        if self.saved_locally:
            self.sync_with_other_holders()
            return self.inner_value
        else:
            response = None
            while (response is None) and (len(self.holders) > 0):
                holder_ids = [holder.get_holder_id() for holder in self.holders]
                id_to_ask = self.executor_interface.get_id_with_shortest_path_from_list(holder_ids)
                response = self.executor_interface.send_sync_directed_message(
                    id_to_ask,
                    SwarmMemoryMessageTypes.REQUEST_READ,
                    {"PATH": self.path}
                )
                if response is None:
                    self.remove_holder_id(id_to_ask)
            if response is None:
                return None
            else:
                return response.get_message_payload()["INNER_VALUE"]

    def get_holders(self):
        return self.holders

    def add_holder_id(self, new_holder_id, time_added):
        if (new_holder_id not in [holder.get_holder_id() for holder in self.holders]) and \
                ((new_holder_id != self.executor_interface.get_id()) or (self.saved_locally)):
            self.holders.append(HolderInfo(new_holder_id, time_added))

    def remove_holder_id(self, holder_id):
        if ((holder_id != self.executor_interface.get_id()) or (not self.saved_locally)):
            target = None
            for holder in self.holders:
                if holder.get_holder_id() == holder_id:
                    target = holder
                    break
            if target is not None:
                self.holders.remove(target)

    def get_time_holder_added(self, id):
        for holder in self.holders:
            if holder.get_holder_id() == id:
                return holder.get_time_added()
        return None

    def local_swarm_memory_entry_handle_remove_holder_id_message(self, message):
        message_payload = message.get_message_payload()
        path = message_payload["PATH"]
        holder_id = message_payload["HOLDER_ID"]

        if path == self.path:
            self.remove_holder_id(holder_id)

    def local_swarm_memory_entry_handle_new_holder_id_message(self, message):
        message_payload = message.get_message_payload()
        path = message_payload["PATH"]
        holder_id = message_payload["HOLDER_ID"]
        time_issued = message_payload["TIME_ISSUED"]

        if path == self.path:
            self.add_holder_id(holder_id, time_issued)

    def local_swarm_memory_entry_handle_new_change_block_message(self, message):
        message_payload = message.get_message_payload()
        path = message_payload["PATH"]
        new_block = message_payload["NEW_BLOCK"]

        if (self.saved_locally) and (path == self.path):
            self.add_change_block(new_block)

    def local_swarm_memory_entry_handle_request_blockchain_transfer_message(self, message):
        message_payload = message.get_message_payload()
        path = message_payload["PATH"]

        if (path == self.path):
            ordered_chain = []
            if (self.saved_locally):
                ordered_chain = self.get_change_blocks()

            self.executor_interface.respond_to_message(
                message,
                {"BLOCKCHAIN": ordered_chain}
            )

    def local_swarm_memory_entry_handle_request_read_message(self, message):
        message_payload = message.get_message_payload()
        path = message_payload["PATH"]

        if (path == self.path):
            local_value = None
            if (self.saved_locally):
                local_value = self.read()

            self.executor_interface.respond_to_message(
                message,
                {"INNER_VALUE": local_value}
            )

    def sync_with_other_holders(self):
        if self.saved_locally:
            for holder in self.holders:
                bot_id = holder.get_holder_id()
                if bot_id != self.executor_interface.get_id():
                    response = self.executor_interface.send_sync_directed_message(
                        bot_id,
                        SwarmMemoryMessageTypes.REQUEST_BLOCKCHAIN_TRANSFER,
                        {"PATH": self.path}
                    )
                    if response is None:
                        self.remove_holder_id(bot_id)
                    else:
                        blockchain = response.get_message_payload()["BLOCKCHAIN"]
                        for block in blockchain:
                            self.add_change_block(block)

    def get_inner_value(self):
        return self.inner_value

    def get_path(self):
        return self.path

    def teardown(self):
        self.executor_interface.unassign_msg_handler(
            str(SwarmMemoryMessageTypes.REMOVE_HOLDER_ID),
            self.local_swarm_memory_entry_handle_remove_holder_id_message
        )
        self.executor_interface.unassign_msg_handler(
            str(SwarmMemoryMessageTypes.NEW_HOLDER_ID),
            self.local_swarm_memory_entry_handle_new_holder_id_message
        )
        self.executor_interface.unassign_msg_handler(
            str(SwarmMemoryMessageTypes.NEW_CHANGE_BLOCK),
            self.local_swarm_memory_entry_handle_new_change_block_message
        )
        self.executor_interface.unassign_msg_handler(
            str(SwarmMemoryMessageTypes.REQUEST_BLOCKCHAIN_TRANSFER),
            self.local_swarm_memory_entry_handle_request_blockchain_transfer_message
        )
        self.executor_interface.unassign_msg_handler(
            str(SwarmMemoryMessageTypes.REQUEST_READ),
            self.local_swarm_memory_entry_handle_request_read_message
        )
