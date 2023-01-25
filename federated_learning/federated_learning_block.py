class FederatedLearningBlock(object):
    def __init__(self, coef, intercept, validation_data, validation_targets):
        self.coef = coef
        self.intercept = intercept
        self.validation_data = validation_data
        self.validation_targets = validation_targets

    def run_proof_of_validation(self):
        from src.model import Model
        check_model = Model()
        check_model.set_from_block(self)
        score = check_model.get_score(self.validation_data, self.validation_targets)
        return score > -1.0

    def set_validation_data(self, new_validation_data):
        self.validation_data = new_validation_data

    def set_validation_targets(self, new_validation_targets):
        self.validation_targets = new_validation_targets

    def get_coef(self):
        return self.coef

    def get_intercept(self):
        return self.intercept
