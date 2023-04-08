from sklearn import linear_model
from federated_learning_task_template.federated_learning_block import FederatedLearningBlock
import copy

# https://scikit-learn.org/0.15/modules/generated/sklearn.linear_model.SGDRegressor.html#sklearn.linear_model.SGDRegressor.partial_fit
# https://scikit-learn.org/0.15/modules/scaling_strategies.html


class FederatedLearningModel(object):
    def __init__(self):
        self.current_model = linear_model.SGDRegressor()
        self.current_model.coef_ = None
        self.current_model.intercept_ = None

    def get_score(self, test_data, test_targets):
        return self.current_model.score(test_data, test_targets)

    def train(self, data, targets):
        self.current_model.partial_fit(data, targets)

    def coefficients_equal(self, coef_1, coef_2):
        if (coef_1 is None) and (coef_2 is not None):
            return False
        elif (coef_1 is not None) and (coef_2 is None):
            return False
        elif (coef_1 is not None) and (coef_2 is not None):
            if coef_1.__class__.__name__ != coef_2.__class__.__name__:
                return False

            if len(coef_1) != len(coef_2):
                return False

            for i in range(len(coef_1)):
                if coef_1[i] != coef_2[i]:
                    return False

        return True

    def intercepts_equal(self, intercept_1, intercept_2):
        if (intercept_1 is None) and (intercept_2 is not None):
            return False
        elif (intercept_1 is not None) and (intercept_2 is None):
            return False
        elif (intercept_1 is not None) and (intercept_2 is not None):
            if intercept_1.__class__.__name__ != intercept_2.__class__.__name__:
                return False

            if isinstance(intercept_1, float):
                if intercept_1 != intercept_2:
                    return False
            else:
                if len(intercept_1) != len(intercept_2):
                    return False

                for i in range(len(intercept_1)):
                    if intercept_1[i] != intercept_2[i]:
                        return False

        return True

    def __eq__(self, other_model):
        if other_model is None:
            return False

        curr_coef = self.current_model.coef_
        other_ceof = other_model.get_coef()

        curr_intercept = self.current_model.intercept_
        other_intercept = other_model.get_intercept()

        return self.coefficients_equal(curr_coef, other_ceof) and self.intercepts_equal(curr_intercept, other_intercept)

    def to_block(self):
        return FederatedLearningBlock(self.current_model.coef_, self.current_model.intercept_, None, None)

    def set_from_task_output(self, task_output):
        self.current_model.coef_ = copy.deepcopy(task_output["COEF"])
        self.current_model.intercept_ = copy.deepcopy(task_output["INTERCEPT"])

    def set_from_block(self, block):
        self.current_model.coef_ = copy.deepcopy(block.get_coef())
        self.current_model.intercept_ = copy.deepcopy(block.get_intercept())

    def set_from_model(self, ref_model):
        self.current_model.coef_ = copy.deepcopy(ref_model.get_coef())
        self.current_model.intercept_ = copy.deepcopy(ref_model.get_intercept())

    def get_coef(self):
        return self.current_model.coef_

    def get_intercept(self):
        return self.current_model.intercept_

    def __str__(self):
        return "COEF: " + str(self.current_model.coef_) + ", INTERCEPT: " + str(self.current_model.intercept_)

    def set_coef(self, new_coef):
        self.current_model.coef_ = new_coef

    def set_intercept(self, new_intercept):
        self.current_model.intercept_ = new_intercept

    def aggregate_models(self, model_list):
        new_coef = 0
        new_intercept = 0
        for model in model_list:
            new_coef += model.get_coef()
            new_intercept += model.get_intercept()
        new_coef /= len(model_list)
        new_intercept /= len(model_list)

        aggregated_model = FederatedLearningModel()
        aggregated_model.set_coef(new_coef)
        aggregated_model.set_intercept(new_intercept)

        return aggregated_model
