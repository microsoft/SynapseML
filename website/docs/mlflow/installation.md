---
title: mlflow Installation
description: install mlflow on different environments
---

## Installation

Install MLflow from PyPI via `pip install mlflow`

MLflow requires `conda` to be on the `PATH` for the projects feature.

Learn more about MLflow on their [GitHub page](https://github.com/mlflow/mlflow).


### Install mlflow on Databricks

If you're using Databricks, install mlflow with this command:
```
# run this so that mlflow is installed on workers besides driver
%pip install mlflow
```

### Install mlflow on Synapse
mlflow is pre-installed on Synapse. To log model with mlflow, you'll need to create an Azure Machine Learning workspace and linking it with your Synapse workspace.

#### Create Azure Machine Learning Workspace

Please follow this document to create [AML workspace](https://learn.microsoft.com/en-us/azure/machine-learning/quickstart-create-resources#create-the-workspace), compute instance and compute clusters are not required.

#### Create an Azure ML Linked Service

<img src="https://mmlspark.blob.core.windows.net/graphics/Documentation/ml_linked_service_1.png" width="600" />

- In the Synapse workspace, go to **Manage** -> **External connections** -> **Linked services**, click **+ New**
- Select the workspace you want to log the model in and create the linked service. The name of the linked service will be used later.

#### Auth Synapse Workspace
<img src="https://mmlspark.blob.core.windows.net/graphics/Documentation/ml_linked_service_2.png" width="600" />

- Go to the **Azure Machine Learning workspace** resource -> **access control (IAM)** -> **Role assignment**, click **+ Add**, choose **Add role assignment**
- Choose **contributor**, click next
- In members blade, choose **Managed identity**, click **+ select members**. Under **managed identity**, choose Synapse workspace. Under **Select**, choose the workspace you will run your experiment on. Click Select, Review + assign.
#### Use mlflow in Synapse
You can add the code below to set up connection
```python

#AML workspace authentication using linked service
from notebookutils.mssparkutils import azureML
linked_service_name = "YourLinkedServiceName"
ws = azureML.getWorkspace(linked_service_name)
mlflow.set_tracking_uri(ws.get_mlflow_tracking_uri())

#Set MLflow experiment. 
experiment_name = "synapse-mlflow-experiment"
mlflow.set_experiment(experiment_name) 
```
