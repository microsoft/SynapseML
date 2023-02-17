---
title: Mlflow Installation
description: install Mlflow on different environments
---

## Installation

Install MLflow from PyPI via `pip install mlflow`

MLflow requires `conda` to be on the `PATH` for the projects feature.

Learn more about MLflow on their [GitHub page](https://github.com/mlflow/mlflow).


### Install Mlflow on Databricks

If you're using Databricks, install Mlflow with this command:
```
# run this so that Mlflow is installed on workers besides driver
%pip install mlflow
```

### Install Mlflow on Synapse
To log model with Mlflow, you need to create an Azure Machine Learning workspace and link it with your Synapse workspace.

#### Create Azure Machine Learning Workspace

Follow this document to create [AML workspace](https://learn.microsoft.com/en-us/azure/machine-learning/quickstart-create-resources#create-the-workspace). You don't need to create compute instance and compute clusters.

#### Create an Azure ML Linked Service

<img src="https://mmlspark.blob.core.windows.net/graphics/Documentation/ml_linked_service_1.png" width="600" />

- In the Synapse workspace, go to **Manage** -> **External connections** -> **Linked services**, select **+ New**
- Select the workspace you want to log the model in and create the linked service. You need the **name of the linked service** to set up connection.

#### Auth Synapse Workspace
<img src="https://mmlspark.blob.core.windows.net/graphics/Documentation/ml_linked_service_2.png" width="600" />

- Go to the **Azure Machine Learning workspace** resource -> **access control (IAM)** -> **Role assignment**, select **+ Add**, choose **Add role assignment**
- Choose **contributor**, select next
- In members page, choose **Managed identity**, select  **+ select members**. Under **managed identity**, choose Synapse workspace. Under **Select**, choose the workspace you run your experiment on. Click **Select**, **Review + assign**.


#### Use Mlflow in Synapse
Set up connection
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

#### Alternative (Don't need Linked Service)
Once you create an AML workspace, you can obtain the MLflow tracking URL directly. The AML start page is where you can locate the MLflow tracking URL.
<img src="https://mmlspark.blob.core.windows.net/graphics/Documentation/mlflow_tracking_url.png" width="600" />
You can set it tracking url with
```python
mlflow.set_tracking_uri("your mlflow tracking url")
```
