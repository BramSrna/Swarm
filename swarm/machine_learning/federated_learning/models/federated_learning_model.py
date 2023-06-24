class FederatedLearningModel(object):
    def __init__(self):
        self.id = id(self)
    
    def get_id(self):
        return self.id

    def set_from_model(self, ref_model):
        raise Exception("ERROR: The set_from_model method must be implemented by the child class.")

    def train(self, data, targets):
        raise Exception("ERROR: The train method must be implemented by the child class.")

    def get_score(self, test_data, test_targets):
        raise Exception("ERROR: The get_score method must be implemented by the child class.")

    def aggregate_models(self, model_list):
        raise Exception("ERROR: The aggregate_models method must be implemented by the child class.")