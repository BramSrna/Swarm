class LocalSwarmMemoryEntry(object):
    def __init__(self, inner_value):
        self.change_blocks = []
        self.future_blocks = []
        
        self.reset_state()

        self.inner_value = inner_value

    def get_inner_value(self):
        return self.inner_value

    def get_curr_state_counter(self):
        return len(self.change_blocks)
    
    def reprocess_chain(self):
        self.reset_state()
        for potential_block_list in self.change_blocks:
            sorted_potential_block_list = sorted(potential_block_list, key=cmp_to_key(self.compare_change_blocks))
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
        self.contents = {}

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
            sorted_potential_block_list = sorted(potential_block_list, key=cmp_to_key(self.compare_change_blocks))
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

    def create_change_block(self, change_block):
        if not self.is_valid_block(change_block):
            return False
        
        self.add_change_block(change_block)