import logging
import unittest

from network_manager_test.network_node_test_class import NetworkNodeTestClass
from swarm.swarm_bot import SwarmBot
from swarm.swarm_memory.swarm_shared_object import SwarmSharedObject
from swarm.swarm_memory.swarm_shared_object_change import SwarmSharedObjectChange

class SimpleIntegerObj(SwarmSharedObject):
    def __init__(self):
        super().__init__()

    def get_value(self):
        return self.value

    def process_change_block(self, new_block):
        self.value += new_block.get_value()
    
    def is_valid_block(self, block_to_check):
        if not super().is_valid_block(block_to_check):
            return False
        if not isinstance(block_to_check, SimpleIntegerObjChange):
            return False
        return True
    
    def reset_state(self):
        self.value = 10

    def compare_change_blocks(self, block_1, block_2):
        if block_1.get_value() < block_2.get_value():
            return -1
        elif block_1.get_value() > block_2.get_value():
            return 1
        else:
            return None

class SimpleIntegerObjChange(SwarmSharedObjectChange):
    def __init__(self, curr_state, change_val):
        super().__init__(curr_state)
        self.change_val = change_val

    def get_value(self):
        return self.change_val

class TestSwarmSharedObject(NetworkNodeTestClass):
    def test_swarm_shared_object_will_iterate_its_state_when_a_new_change_is_received(self):
        test_obj = SimpleIntegerObj()
        self.assertEqual(0, test_obj.get_curr_state_counter())
        test_obj.add_change_block(SimpleIntegerObjChange(0, 1))
        self.assertEqual(1, test_obj.get_curr_state_counter())

    def test_swarm_shared_object_will_process_changes_when_they_are_received(self):
        test_obj = SimpleIntegerObj()
        self.assertEqual(10, test_obj.get_value())
        test_obj.add_change_block(SimpleIntegerObjChange(0, 1))
        self.assertEqual(11, test_obj.get_value())
        test_obj.add_change_block(SimpleIntegerObjChange(1, -7))
        self.assertEqual(4, test_obj.get_value())

    def test_swarm_shared_object_will_not_commit_change_block_until_the_correct_state_is_reached(self):
        test_obj = SimpleIntegerObj()
        self.assertEqual(10, test_obj.get_value())
        test_obj.add_change_block(SimpleIntegerObjChange(0, 1))
        self.assertEqual(11, test_obj.get_value())
        test_obj.add_change_block(SimpleIntegerObjChange(1, -7))
        self.assertEqual(4, test_obj.get_value())
        test_obj.add_change_block(SimpleIntegerObjChange(3, 5))
        self.assertEqual(4, test_obj.get_value())
        test_obj.add_change_block(SimpleIntegerObjChange(2, 4))
        self.assertEqual(13, test_obj.get_value())
        test_obj.add_change_block(SimpleIntegerObjChange(4, 12))
        self.assertEqual(25, test_obj.get_value())

    def test_swarm_shared_object_will_add_chain_block_to_proper_spot_in_chain(self):
        test_obj = SimpleIntegerObj()
        expected_val = 10
        expected_chain = []

        i = 0
        while i < 10:
            self.assertEqual(expected_val, test_obj.get_value())
            new_block = SimpleIntegerObjChange(i, i)
            test_obj.add_change_block(new_block)
            expected_chain.append(new_block)
            expected_val += i
            i += 1

        self.assertEqual(expected_val, test_obj.get_value())

        self.assertEqual(expected_chain, test_obj.get_change_blocks())

        new_block = SimpleIntegerObjChange(0, -3)
        test_obj.add_change_block(new_block)

        expected_chain = [new_block] + expected_chain
        expected_val += -3

        self.assertEqual(expected_val, test_obj.get_value())
        self.assertEqual(expected_chain, test_obj.get_change_blocks())

    def test_one_swarm_shared_object_can_sync_based_on_the_chain_from_another_swarm_shared_object(self):
        test_obj = SimpleIntegerObj()
        self.assertEqual(10, test_obj.get_value())
        test_obj.add_change_block(SimpleIntegerObjChange(0, 1))
        self.assertEqual(11, test_obj.get_value())
        test_obj.add_change_block(SimpleIntegerObjChange(1, -7))
        self.assertEqual(4, test_obj.get_value())
        test_obj.add_change_block(SimpleIntegerObjChange(3, 5))
        self.assertEqual(4, test_obj.get_value())
        test_obj.add_change_block(SimpleIntegerObjChange(2, 4))
        self.assertEqual(13, test_obj.get_value())
        test_obj.add_change_block(SimpleIntegerObjChange(4, 12))
        self.assertEqual(25, test_obj.get_value())

        second_test_obj = SimpleIntegerObj()
        self.assertEqual(10, second_test_obj.get_value())
        second_test_obj.sync_from_swarm_shared_object(test_obj)
        self.assertEqual(test_obj.get_value(), second_test_obj.get_value())
        self.assertEqual(test_obj.get_change_blocks(), second_test_obj.get_change_blocks())


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
