# Log Model with MLflow on Synapse Analytics


## Prerequisites:

To log model with mlflow on Synapse Analytics for the first time, you will need to 

 - Create an AML workspace
 - Create a Service Principle with AML
 - Create a Key Vault to manage secrets (recommended, optional)


### Create AML resources

Please follow this document to create [AML workspace](https://learn.microsoft.com/en-us/azure/machine-learning/quickstart-create-resources#create-the-workspace), compute instance and compute clusters are not required.


### Create a Service Principle with AML

- Install Azure CLI: You will need to install [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli) to create a service principal.

- Find the subscription id: You will need your subscription id to create a service principal, which can be found [here](https://portal.azure.com/#view/Microsoft_Azure_Billing/SubscriptionsBlade)

<img src="https://mmlspark.blob.core.windows.net/graphics/Documentation/subscription_id.png" width="600" style="float: center;"/>

- Create a service principal: Please follow this document to create a [service principal](https://learn.microsoft.com/en-us/azure/machine-learning/how-to-setup-authentication?tabs=sdk#configure-a-service-principal), you will need the subscription id from the step above. Follow the step 1 and 2 under configure a service principal, and take a note of your output JSON. The result will be similar to
```json

{
    "clientId": "your-client-id",
    "clientSecret": "your-client-secret",
    "subscriptionId": "your-sub-id",
    "tenantId": "your-tenant-id",
    "activeDirectoryEndpointUrl": "https://login.microsoftonline.com",
    "resourceManagerEndpointUrl": "https://management.azure.com",
    "activeDirectoryGraphResourceId": "https://graph.windows.net",
    "sqlManagementEndpointUrl": "https://management.core.windows.net:5555",
    "galleryEndpointUrl": "https://gallery.azure.com/",
    "managementEndpointUrl": "https://management.core.windows.net"
}

```

### Install authentication package

The code below install package for this session. If you want to install library on your cluster, please follow this [document](https://learn.microsoft.com/en-us/azure/machine-learning/how-to-use-mlflow-azure-synapse?tabs=cli%2Cmlflow#install-libraries).


```python
import subprocessimport 
import sys
subprocess.check_call([sys.executable, "-m", "pip", "install", "azure.ai.ml"])
```

### Configure authentication


```python
# Please manage the secrets with Azure KeyVault. The following code example is for quick test not for production.
import os
os.environ["AZURE_TENANT_ID"] = "<tenantId> in the service principle json above"
os.environ["AZURE_CLIENT_ID"] = "<clientId> in the service principle json above"
os.environ["AZURE_CLIENT_SECRET"] = "<clientSecret> in the service principle json above"
```

You can download the workspace configuration file by:

- Navigate to [Azure ML studio](https://ml.azure.com/home)
- Click the workspace you want to log your experiment with
- Click on the upper-right corner of the page -> Download config file.

<img src="https://mmlspark.blob.core.windows.net/graphics/Documentation/getamlconfigfile.png" width="400" style="float: center;"/>

the AML config file will contain the following information

```json
{
    "subscription_id": "your subscription_id",
    "resource_group": "your resource_group",
    "workspace_name": "your workspace_name"
}
```


```python

from azure.ai.ml import MLClient
from azure.identity import DefaultAzureCredential

#Enter details of your AzureML workspace
subscription_id = "subscription_id in the AML config file above"
resource_group = "resource_group in the AML config file above"
workspace_name = "workspace_name in the AML config file above"

ml_client = MLClient(credential=DefaultAzureCredential(),
                        subscription_id=subscription_id, 
                        resource_group_name=resource_group,
                        workspace_name=workspace_name)

mlflow_tracking_uri = ml_client.workspaces.get(ml_client.workspace_name).mlflow_tracking_uri
mlflow.set_tracking_uri(mlflow_tracking_uri)
```

## Logging SynapseML LightGBMClassifier model with MLflow


```python
from pyspark.sql import SparkSession

# Bootstrap Spark Session
spark = SparkSession.builder.getOrCreate()

from synapse.ml.core.platform import *
df = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(
        "wasbs://publicwasb@mmlspark.blob.core.windows.net/company_bankruptcy_prediction_data.csv"
    )
)
train, test = df.randomSplit([0.85, 0.15], seed=1)
from pyspark.ml.feature import VectorAssembler

feature_cols = df.columns[1:]
featurizer = VectorAssembler(inputCols=feature_cols, outputCol="features")
train_data = featurizer.transform(train)["Bankrupt?", "features"]
test_data = featurizer.transform(test)["Bankrupt?", "features"]


# MLFlow experiment
artifact_path = "lightGBM"
experiment_name = "lightGBM_experiment"
model_name = "lightGBM-model"
model_version = 1
model = LightGBMClassifier(
    objective="binary", featuresCol="features", labelCol="Bankrupt?", isUnbalance=True
)
from synapse.ml.lightgbm import LightGBMClassifier

with mlflow.start_run():
    fit_model = model.fit(train_data)
    mlflow.spark.log_model(
        fit_model, artifact_path=artifact_path, registered_model_name=model_name
    )

```

You can find the log in your Azure Machine Learning Studio, go to your Azure Machine Learning workspace and click Launch studio.

<img src="https://mmlspark.blob.core.windows.net/graphics/Documentation/mlflow_log.png" width="800" style="float: center;"/>


```python
# load model
model_uri = f"models:/{model_name}/{model_version}"
model2 = mlflow.spark.load_model(model_uri)
```

## Next Steps:
You may want to check [Track Azure Synapse Analytics ML experiments with MLflow and Azure Machine Learning](https://learn.microsoft.com/en-us/azure/machine-learning/how-to-use-mlflow-azure-synapse?tabs=python%2Cmlflow) for deploying the model and more information.
