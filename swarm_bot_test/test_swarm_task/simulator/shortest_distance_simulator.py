import random

from swarm_bot_test.test_swarm_task.simulator.simulator import Simulator


class ShortestDistanceSimulator(Simulator):
    def __init__(self, x_squares=10, y_squares=10):
        super()

        self.x_squares = x_squares
        self.y_squares = y_squares

        self.start_location = self.get_loc()
        self.end_location = self.get_loc()

        self.curr_location = self.start_location
        self.has_loc_been_visited = False

        self.reset()

    def get_loc(self):
        x = random.randint(0, self.x_squares - 1)
        y = random.randint(0, self.y_squares - 1)
        return [x, y]

    def reset(self):
        self.curr_location = self.start_location
        self.has_loc_been_visited = False

    def to_situation(self):
        return {
            "curr_location": self.curr_location
        }

    def update_has_loc_been_visisted(self):
        if (self.curr_location == self.end_location):
            self.has_loc_been_visited = True

    def move_up(self):
        loc = self.curr_location
        x = loc[0]
        y = loc[1]

        if (y > 0):
            self.curr_location = [x, y - 1]
            self.update_has_loc_been_visisted()
            return True

        return False

    def move_down(self):
        loc = self.curr_location
        x = loc[0]
        y = loc[1]

        if (y < self.y_squares - 1):
            self.curr_location = [x, y + 1]
            self.update_has_loc_been_visisted()
            return True

        return False

    def move_left(self):
        loc = self.curr_location
        x = loc[0]
        y = loc[1]

        if (x > 0):
            self.curr_location = [x - 1, y]
            self.update_has_loc_been_visisted()
            return True

        return False

    def move_right(self):
        loc = self.curr_location
        x = loc[0]
        y = loc[1]

        if (x < self.x_squares - 1):
            self.curr_location = [x + 1, y]
            self.update_has_loc_been_visisted()
            return True

        return False

    def get_possible_options(self):
        possible_options = [
            self.move_up,
            self.move_down,
            self.move_left,
            self.move_right
        ]

        return possible_options

    def check_for_finish(self):
        return self.has_loc_been_visited

    def get_curr_location(self):
        return self.curr_location
