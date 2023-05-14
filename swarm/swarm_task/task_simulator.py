from sklearn.tree import DecisionTreeClassifier

class TaskSimulator(object):
    def __init__(self):
        self.data_points = []
        self.needs_retraining = False
        self.simulator = None

    def add_data_point(self, state, possible_actions):
        self.data_points.append((state, possible_actions))
        self.needs_retraining = True

    def get_possible_actions(self, simulator_state):
        if self.needs_retraining:
            self.train_model()
        return self.simulator.predict([simulator_state])
    
    def train_model(self):
        training_data = []
        training_values = []

        for data_point in self.data_points:
            training_data.append(data_point[0])
            training_values.append(data_point[1])

        self.simulator = DecisionTreeClassifier()
        self.simulator = self.simulator.fit(training_data, training_values)