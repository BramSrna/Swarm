from sklearn.tree import DecisionTreeClassifier
from tensorflow.keras import models, layers, utils, backend
import numpy as np

class TaskSimulatorState(object):
    def __init__(self, state_info):
        self.state_info = state_info
        self.transition_tree = {}

    def get_state_info(self):
        return self.state_info
    
    def add_possible_action_info(self, possible_action_info):
        for action in possible_action_info:
            if action not in self.transition_tree:
                self.transition_tree[str(action)] = None

    def add_state_transition_info(self, action, new_state):
        if action not in self.transition_tree:
            self.transition_tree[str(action)] = new_state

    def get_transition_tree(self):
        return self.transition_tree
    
    def get_possible_actions(self):
        possible_actions = list(self.transition_tree.keys())
        return possible_actions


class TaskSimulator(object):
    def __init__(self):
        self.known_states = []
        self.all_possible_actions = []
        self.previous_predictions = {}
        self.action_predictor_model = None
        self.state_transition_predictor_model = None

        self.reset_models()

    def save_execution_flow_info(self, original_state_info, original_state_possible_actions, new_state_info, new_state_possible_actions, action_executed):
        saved_original_state = None
        if original_state_info is not None:
            saved_original_state = self.get_known_state(original_state_info)
            if saved_original_state is None:
                saved_original_state = TaskSimulatorState(original_state_info)
                self.known_states.append(saved_original_state)

        saved_new_state = self.get_known_state(new_state_info)
        if saved_new_state is None:
            saved_new_state = TaskSimulatorState(new_state_info)
            self.known_states.append(saved_new_state)

        if saved_original_state is not None:
            saved_original_state.add_possible_action_info(original_state_possible_actions)
            saved_original_state.add_state_transition_info(action_executed, saved_new_state)
        saved_new_state.add_possible_action_info(new_state_possible_actions)

        for action in original_state_possible_actions:
            if str(action) not in self.all_possible_actions:
                self.all_possible_actions.append(str(action))

        for action in new_state_possible_actions:
            if str(action) not in self.all_possible_actions:
                self.all_possible_actions.append(str(action))

        self.reset_models()

    def is_ready_for_use(self):
        if len(self.known_states) == 0:
            return False
        
        unknown_transitions = 0
        total_transitions = 0
        for state in self.known_states:
            for _, new_state in state.get_transition_tree().items():
                if new_state is None:
                    unknown_transitions += 1
            total_transitions += 1
        return (float(unknown_transitions) / float(total_transitions)) > 0.9 
    
    def get_start_state(self):
        return self.known_states[0].get_state_info()
    
    def get_end_state(self):
        for state in self.known_states:
            possible_actions = list(state.get_transition_tree().keys())
            if len(possible_actions) == 0:
                return state.get_state_info()
        return None
            
    def get_known_state(self, state_info):
        for state in self.known_states:
            if state.get_state_info() == state_info:
                return state
        return None
    
    def get_all_known_states(self):
        ret_arr = []
        for state in self.known_states:
            ret_arr.append(state.get_state_info())
        return ret_arr
    
    def construct_state_prediction(self, state_info):
        if str(state_info) in self.previous_predictions:
            return self.previous_predictions[str(state_info)]
        
        predicted_state = TaskSimulatorState(state_info)

        if self.action_predictor_model is None:
            self.train_action_predictor_model()

        actions = self.action_model_array_to_method_array(self.action_predictor_model.predict([state_info])[0])
        predicted_state.add_possible_action_info(actions)

        if self.state_transition_predictor_model is None:
            self.train_state_transition_predictor_model()

        for action in actions:
            predict_input = state_info + self.action_method_array_to_model_array([action])
            new_state_info = self.state_transition_predictor_model.predict([predict_input])
            predicted_state.add_state_transition_info(action, TaskSimulatorState(new_state_info))

        self.previous_predictions[str(state_info)] = predicted_state
        return predicted_state
    
    def train_action_predictor_model(self):
        training_data = []
        training_targets = []

        for state in self.known_states:
            state_info = state.get_state_info()
            training_data.append(state_info)

            curr_possible_actions = state.get_possible_actions()
            training_targets.append(self.action_method_array_to_model_array(curr_possible_actions))

        self.action_predictor_model = DecisionTreeClassifier()
        self.action_predictor_model = self.action_predictor_model.fit(training_data, training_targets)
    
    def train_state_transition_predictor_model(self):
        # https://towardsdatascience.com/deep-learning-with-python-neural-networks-complete-tutorial-6b53c0b06af0
        base_input_size = len(self.known_states[0].get_state_info()) + len(self.all_possible_actions)
        output_size = len(self.known_states[0].get_state_info())

        # Define the model
        self.state_transition_predictor_model = models.Sequential(name="ActionPredictorModel", layers=[
            layers.Dense(name="h1", input_dim=base_input_size, units=int(round((base_input_size + 1) / 2)), activation='relu'),
            layers.Dropout(name="drop1", rate=0.2),
            
            layers.Dense(name="h2", units=int(round((base_input_size + 1) / 4)), activation='relu'),
            layers.Dropout(name="drop2", rate=0.2),
            
            layers.Dense(name="output", units=output_size, activation='sigmoid')
        ])
        self.state_transition_predictor_model.summary()

        # Compile the model
        self.state_transition_predictor_model.compile(optimizer='adam', loss='mean_absolute_error', metrics=['accuracy'])

        # Prepare the data
        training_data = []
        training_targets = []

        for state in self.known_states:
            state_info = state.get_state_info()
            transition_tree = state.get_transition_tree()
            for action, new_state in transition_tree.items():
                if new_state is not None:
                    training_data.append(state_info + self.action_method_array_to_model_array([str(action)]))
                    training_targets.append(new_state.get_state_info())

        # Train the model
        self.state_transition_predictor_model.fit(x=np.array(training_data), y=np.array(training_targets), batch_size=32, epochs=100, shuffle=True, validation_split=0.3)
    
    def get_state_info_after_action(self, state_info, action):
        saved_state = self.get_known_state(state_info)
        if saved_state is None:
            return None
        transition_tree = saved_state.get_transition_tree()
        if str(action) in transition_tree:
            return transition_tree[str(action)].get_state_info()
        else:
            return state_info 

    def get_possible_actions_for_state(self, state_info):
        saved_state = self.get_known_state(state_info)
        if saved_state is not None:
            return self.action_method_array_to_model_array(saved_state.get_possible_actions())
        else:
            return None
    
    def action_method_array_to_model_array(self, method_array):
        model_array = []
        for action in self.all_possible_actions:
            if action in method_array:
                model_array.append(1)
            else:
                model_array.append(0)
        return model_array

    def action_model_array_to_method_array(self, model_array):
        method_array = []
        for i in range(len(model_array)):
            if model_array[i] == 1:
                method_array.append(self.all_possible_actions[i])
        return method_array
    
    def reset_models(self):
        self.previous_predictions = {}
        self.action_predictor_model = None
        self.state_transition_predictor_model = None


