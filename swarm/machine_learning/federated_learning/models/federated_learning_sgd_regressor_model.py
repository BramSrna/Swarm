from sklearn import linear_model
import copy
from swarm.machine_learning.federated_learning.models.federated_learning_model import FederatedLearningModel

# https://scikit-learn.org/0.15/modules/generated/sklearn.linear_model.SGDRegressor.html#sklearn.linear_model.SGDRegressor.partial_fit
# https://scikit-learn.org/0.15/modules/scaling_strategies.html

class FederatedLearningSGDRegressorModel(FederatedLearningModel):
    def __init__(self):
        self.id = id(self)
        self.current_model = linear_model.SGDRegressor()
        self.has_been_fitted = False

    def get_id(self):
        return self.id

    def get_score(self, test_data, test_targets):
        return self.current_model.score(test_data, test_targets)

    def train(self, data, targets):
        self.current_model.partial_fit(data, targets)
        self.has_been_fitted = True

    def get_has_been_fitted(self):
        return self.has_been_fitted

    def __eq__(self, other_model):
        if other_model is None:
            return False
        
        my_coef = self.get_coef()
        other_coef = other_model.get_coef()

        if (my_coef is None) and (other_coef is not None):
            return False
        elif (my_coef is not None) and (other_coef is None):
            return False
        
        my_intercept = self.get_intercept()
        other_intercept = other_model.get_intercept()

        if (my_intercept is None) and (other_intercept is not None):
            return False
        elif (my_intercept is not None) and (other_intercept is None):
            return False

        return (my_coef == other_coef) and (my_intercept == other_intercept)

    def set_from_model(self, ref_model):
        self.current_model.coef_ = copy.deepcopy(ref_model.get_coef())
        self.current_model.intercept_ = copy.deepcopy(ref_model.get_intercept())

    def get_coef(self):
        if hasattr(self.current_model, "coef_"):
            return self.current_model.coef_
        return None

    def get_intercept(self):
        if hasattr(self.current_model, "intercept_"):
            return self.current_model.intercept_
        return None

    def __str__(self):
        return "COEF: " + str(self.get_coef()) + ", INTERCEPT: " + str(self.get_intercept())

    def set_coef(self, new_coef):
        self.current_model.coef_ = new_coef

    def set_intercept(self, new_intercept):
        self.current_model.intercept_ = new_intercept

    def aggregate_models(self, model_list):
        new_coef = 0
        new_intercept = 0
        num_fitted_models = 0

        for model in model_list:
            if model.get_has_been_fitted():
                new_coef += model.get_coef()
                new_intercept += model.get_intercept()
                num_fitted_models += 1

        if num_fitted_models == 0:
            print("HERE")
            return None
        
        new_coef /= num_fitted_models
        new_intercept /= num_fitted_models

        aggregated_model = FederatedLearningSGDRegressorModel()
        aggregated_model.set_coef(new_coef)
        aggregated_model.set_intercept(new_intercept)

        return aggregated_model