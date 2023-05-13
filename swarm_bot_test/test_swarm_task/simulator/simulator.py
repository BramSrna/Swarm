class Simulator(object):
    def __init__(self):
        pass

    def to_situation(self):
        raise NotImplementedError("The method to_situation() needs to be implemented by the child class.")

    def compare_situations(self, ref_situation):
        raise NotImplementedError("The method compare_situations(ref_situation) needs to be implemented by the child class.")

    def __str__(self):
        raise NotImplementedError("The method __str__() needs to be implemented by the child class.")

    def reset(self):
        raise NotImplementedError("The method reset() needs to be implemented by the child class.")

    def get_possible_options(self):
        raise NotImplementedError("The method get_possible_options() needs to be implemented by the child class.")
