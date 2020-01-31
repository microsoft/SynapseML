import numpy as np
from joblib import dump
import os
from azureml.core import Experiment, Workspace
from azureml.train.estimator import Estimator

# X = np.array([[1, 1], [1, 2], [2, 2], [2, 3]])
# y = np.dot(X, np.array([1, 2])) + 3
# reg = LinearRegression().fit(X, y)
# print('The scikit-learn version is {}.'.format(sklearn.__version__))

# outputdir = "outputs/"
# os.makedirs(outputdir, exist_ok=True)

reg = {"dog": 1, "cat": 1230491408}
# reg = lambda wfe: 10
dump(reg, "my_c00l_classifier.joblib") 

from azureml.core import Model


ws = Workspace(
    subscription_id="ce1dee05-8cf6-4ad6-990a-9c80868800ba",
    resource_group="extern2020",
    workspace_name="exten-amls"
)

# classification_model = Model.register(workspace=ws,
#                        model_name='stupid_model',
#                        model_path= 'my_c00l_classifier.joblib', # local path
#                        description='A reall dumb model')
classification_model = Model(ws, name="stupid_model")

################################################################

from azureml.core.conda_dependencies import CondaDependencies

Add the dependencies for your model
myenv = CondaDependencies()
myenv.add_conda_package("scikit-learn")

# Save the environment config as a .yml file
env_file = 'myenv.yml'
with open(env_file,"w") as f:
    f.write(myenv.serialize_to_string())
print("Saved dependency info in", env_file)

################################################################

from azureml.core.model import InferenceConfig

classifier_inference_config = InferenceConfig(runtime= "python",
                                              entry_script="score.py",
                                              conda_file="myenv.yml")


################################################################

from azureml.core.webservice import AksWebservice
from azureml.core.compute import AksCompute, ComputeTarget

service_name = 'mydumbservice5'
cluster_name = 'sparkml-test'
print("Deploying new service: {}".format(service_name))
production_cluster = AksCompute(ws, cluster_name)
classifier_deploy_config = AksWebservice.deploy_configuration(cpu_cores = 1,
                                                              memory_gb = 1)

################################################################


from azureml.core.model import Model


from azureml.core import diagnostic_log
with diagnostic_log('my.log'):

  classification_model = Model.register(workspace=ws,
                       model_name='stupid_model',
                       model_path= 'my_c00l_classifier.joblib', # local path
                       description='A reall dumb model')
  # service = Model.register(workspace=ws,
  #                        name = service_name,
  #                        models = [classification_model],
  #                        inference_config = classifier_inference_config,
  #                        deployment_config = classifier_deploy_config,
  #                        deployment_target = production_cluster,
  #                        overwrite=True)
  # service.wait_for_deployment(show_output = True)
  # print(service.get_logs())
