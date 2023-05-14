import random

from swarm_bot_test.test_swarm_task.simulator.simulator import Simulator


class ShortestDistanceSimulator(Simulator):
    def __init__(self, num_columns=10, num_rows=10):
        super()

        self.num_columns = num_columns
        self.num_rows = num_rows

        start_loc = self.generate_random_location()
        end_loc = self.generate_random_location()
        while end_loc == start_loc:
            end_loc = self.generate_random_location()

        self.curr_col, self.curr_row = start_loc
        self.end_col, self.end_row = end_loc

        self.has_loc_been_visited = False
        self.traversed_path = [start_loc]

    def generate_random_location(self):
        col = random.randint(0, self.num_columns - 1)
        row = random.randint(0, self.num_rows - 1)
        return [col, row]
    
    def get_traversed_path(self):
        return self.traversed_path

    def update_has_loc_been_visisted(self):
        if (self.curr_col == self.end_col) and (self.curr_row == self.end_row):
            self.has_loc_been_visited = True
        self.traversed_path.append(self.get_current_simulator_state())

    def get_current_simulator_state(self):
        return [self.curr_col, self.curr_row]

    def move_up(self):
        if not self.get_possible_actions(self.get_current_simulator_state())[0]:
            return False
        
        self.curr_row += 1
        self.update_has_loc_been_visisted()

    def move_right(self):
        if not self.get_possible_actions(self.get_current_simulator_state())[1]:
            return False
        
        self.curr_col += 1
        self.update_has_loc_been_visisted()

    def move_down(self):
        if not self.get_possible_actions(self.get_current_simulator_state())[2]:
            return False
        
        self.curr_row -= 1
        self.update_has_loc_been_visisted()

    def move_left(self):
        if not self.get_possible_actions(self.get_current_simulator_state())[3]:
            return False
        
        self.curr_col -= 1
        self.update_has_loc_been_visisted()

    def check_for_finish(self):
        return self.has_loc_been_visited
    
    def get_possible_actions(self, simulator_state):
        col, row = simulator_state

        # up, right, down, left
        possible_actions = [1, 1, 1, 1]

        if row >= self.num_rows - 1:
            possible_actions[0] = 0
        if col >= self.num_columns - 1:
            possible_actions[1] = 0
        if row <= 0:
            possible_actions[2] = 0
        if col <= 0:
            possible_actions[3] = 0

        return possible_actions
    
    def get_possible_actions_as_methods(self, simulator_state):
        bin_possible_actions = self.get_possible_actions(simulator_state)
        method_possible_actions = []
        if bin_possible_actions[0]:
            method_possible_actions.append(self.move_up)
        if bin_possible_actions[1]:
            method_possible_actions.append(self.move_right)
        if bin_possible_actions[2]:
            method_possible_actions.append(self.move_down)
        if bin_possible_actions[3]:
            method_possible_actions.append(self.move_left)
        return method_possible_actions
    
    def get_possible_states(self):
        possible_states = []
        for col in range(self.num_columns - 1):
            for row in range(self.num_rows - 1):
                possible_states.append([col, row])
        return possible_states
