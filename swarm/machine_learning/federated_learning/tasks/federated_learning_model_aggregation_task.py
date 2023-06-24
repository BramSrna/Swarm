from swarm.swarm_task.swarm_task import SwarmTask

class FederatedLearningModelAggregationTask(SwarmTask):
    def __init__(self, extra_params):
        super().__init__(extra_params)
        self.model_id = extra_params[0]

        self.pushed_aggregated_model = False

        self.aggregated_model = None

    def is_complete(self):
        return self.pushed_aggregated_model

    def execute_task(self):
        local_models = list(self.executor_interface.read_from_swarm_memory("models/" + str(self.model_id) + "/local_models").values())
        self.executor_interface.write_to_swarm_memory("models/" + str(self.model_id) + "/local_models", {})
        self.aggregated_model = self.aggregate_models(local_models)
        if self.aggregated_model != None:
            self.executor_interface.write_to_swarm_memory("models/" + str(self.model_id) + "/current_model", self.aggregated_model)
            self.pushed_aggregated_model = True

    def aggregate_models(self, models):
        if len(models) == 0:
            return None
                
        aggregated_model = models[0].aggregate_models(models)

        return aggregated_model
    
    def get_task_output(self):
        return self.aggregated_model

    def get_priority_score(self):
        return 1