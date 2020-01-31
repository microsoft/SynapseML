from azureml.core.compute import ComputeTarget, AmlCompute
from azureml.core.runconfig import RunConfiguration
from azureml.core import Experiment, Workspace
from azureml.core.environment import Environment
from azureml.core import Datastore
from azureml.core import ScriptRunConfig
from azureml.train.estimator import Estimator
import os 
from azureml.core.run import Run


def get_logger():
    try:
        return Run.get_context()
    except Exception:
        return LocalLogger()

run_logger = get_logger()
run_logger.log("Here is a sample log test", "Bananas bananas.")

ws = Workspace(
    subscription_id="ce1dee05-8cf6-4ad6-990a-9c80868800ba",
    resource_group="extern2020",
    workspace_name="exten-amls"
)
exp = Experiment(workspace=ws, name='new_experiment')
compute_target = ComputeTarget(workspace=ws, name='cpu-cluster')

estimator = Estimator(
    source_directory = "",
    entry_script = "sklearn_linear_regression.py",
    script_params = {},
    conda_dependencies_file = "myenv.yml",
    compute_target = compute_target,
    use_docker = True,
   	custom_docker_image = "mcr.microsoft.com/azureml/base:intelmpi2018.3-ubuntu16.04"
)

run = exp.submit(estimator)
run.wait_for_completion(show_output = True)
run.register_model(
    model_name="my_c00l_classifier",
    model_path="outputs/my_c00l_classifier.joblib"
)