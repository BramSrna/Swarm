import random

from swarm_bot_test.test_swarm_task.simulator.simulator import Simulator


class TravellingSalesmanSimulator(Simulator):
    def __init__(self, x_squares=10, y_squares=10, num_salesman=10):
        super()

        self.sale_man_id = 1

        self.x_squares = x_squares
        self.y_squares = y_squares

        curr_map = []

        for y in range(y_squares):
            curr_row = []

            for x in range(x_squares):
                curr_row.append(None)

            curr_map.append(curr_row)

        self.sim_map = curr_map

        self.locs_to_visit = []

        self.gen_locations(num_salesman)

        self.original_salesman_loc = self.gen_salesman_loc()

        self.locs_remaining = self.locs_to_visit

    def gen_salesman_loc(self):
        x = random.randint(0, self.x_squares - 1)
        y = random.randint(0, self.y_squares - 1)
        self.salesman_loc = [x, y]
        return self.salesman_loc

    def get_salesman_loc(self):
        return(self.salesman_loc)

    def gen_locations(self, num_locations=10):
        for i in range(num_locations):
            loc_occupied = True

            while loc_occupied:
                x = random.randint(0, self.x_squares - 1)
                y = random.randint(0, self.y_squares - 1)

                loc_occupied = self.is_salesman_at_loc(x, y)

            self.add_saleman_at_loc(x, y)

    def add_saleman_at_loc(self, x, y):
        self.sim_map[y][x] = self.sale_man_id
        self.locs_to_visit.append((y, x))

    def is_salesman_at_loc(self, x, y):
        return self.sim_map[y][x] == self.sale_man_id

    def get_locs_to_visit(self):
        return self.locs_to_visit

    def get_locs_remaining(self):
        return self.locs_remaining

    def to_situation(self):
        return {
            "salesman_loc": self.get_salesman_loc(),
            "locs_remaining": self.get_locs_remaining()
        }

    def check_for_finish(self):
        return len(self.get_locs_remaining()) == 0

    def compare_situations(self, ref_situation):
        locs_remaining_equal = (self.to_situation["locs_remaining"] == ref_situation["locs_remaining"])
        salesman_loc_equal = (self.to_situation["salesman_loc"] == ref_situation["salesman_loc"])
        return (locs_remaining_equal and salesman_loc_equal)

    def update_locs_remaining(self):
        loc = self.salesman_loc
        x = loc[0]
        y = loc[1]

        if ((x, y) in self.locs_remaining):
            self.locs_remaining.remove((x, y))

        print("Curr position: " + str((x, y)) + " Locations Remaining: " + str(self.locs_remaining))

    def move_up(self):
        loc = self.salesman_loc
        x = loc[0]
        y = loc[1]

        if (y > 0):
            self.salesman_loc = [x, y - 1]
            self.update_locs_remaining()
            return True

        return False

    def move_down(self):
        loc = self.salesman_loc
        x = loc[0]
        y = loc[1]

        if (y < self.y_squares - 1):
            self.salesman_loc = [x, y + 1]
            self.update_locs_remaining()
            return True

        return False

    def move_left(self):
        loc = self.salesman_loc
        x = loc[0]
        y = loc[1]

        if (x > 0):
            self.salesman_loc = [x - 1, y]
            self.update_locs_remaining()
            return True

        return False

    def move_right(self):
        loc = self.salesman_loc
        x = loc[0]
        y = loc[1]

        if (x < self.x_squares - 1):
            self.salesman_loc = [x + 1, y]
            self.update_locs_remaining()
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

    def reset(self):
        self.salesman_loc = self.original_salesman_loc

    def __str__(self):
        retStr = "["

        for y in range(self.y_squares):
            if y > 0:
                retStr += "\n"

            retStr += "["

            for x in range(self.x_squares):
                if x > 0:
                    retStr += ", "

                retStr += str(self.sim_map[y][x])

            retStr += "]"

        retStr += "]"

        return retStr
