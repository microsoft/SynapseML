---
title: HyperOpt
hide_title: true
status: stable
---
# Hyperparameter tuning: SynapseML with Hyperopt

[SynapseML](https://github.com/microsoft/SynapseML) is an open-source library that simplifies the creation of massively scalable machine learning (ML) pipelines. SynapseML provides simple, composable, and distributed APIs for a wide variety of different machine learning tasks such as text analytics, vision, anomaly detection, and many others.

[Hyperopt](https://github.com/hyperopt/hyperopt), on the other hand, is a Python library for serial and parallel optimization over complex search spaces, including real-valued, discrete, and conditional dimensions.

This guide showcases the process of tuning a distributed algorithm in Spark with SynapseML and Hyperopt.

The use case of this guide is for distributed machine learning in Python that requires hyperparameter tuning. It provides a demo on how to tune hyperparameters for a machine learning workflow in SynapseML and can be used as a reference to tune other distributed machine learning algorithms from Spark MLlib or other libraries.

The guide includes two sections:
* Running distributed training with SynapseML without hyperparameter tuning.
* Using Hyperopt to tune hyperparameters in the distributed training workflow.
## Prerequisites
 - If you are running it on Synapse, you'll need to [create an AML workspace and set up linked Service](../../../Use%20with%20MLFlow/Overview/).

## Requirements
 - Install HyperOpt


```python
%pip install hyperopt mlflow
```

## MLflow autologging

To track model training and tuning with MLflow, you could enable MLflow autologging by running `mlflow.pyspark.ml.autolog()`.


```python
from synapse.ml.core.platform import *

if running_on_synapse_internal():
    experiment_name = "hyperopt-synapseml"
elif running_on_synapse():
    experiment_name = "hyperopt-synapseml"
else:
    experiment_name = "/Shared/hyperopt-synapseml"
```


```python
import mlflow

mlflow.__version__
```


```python
# Set pyspark autologging logModelAllowlist to include SynapseML models
spark.sparkContext._conf.set(
    "spark.mlflow.pysparkml.autolog.logModelAllowlistFile",
    "https://mmlspark.blob.core.windows.net/publicwasb/log_model_allowlist.txt",
)
```


```python
# enable autologging
mlflow.pyspark.ml.autolog()
```

### Set experiment name for tracking


```python
# Set MLflow experiment.

if running_on_synapse():
    from notebookutils.mssparkutils import azureML

    linked_service = "AzureMLService1"  # use your linked service name
    ws = azureML.getWorkspace(linked_service)
    mlflow.set_tracking_uri(ws.get_mlflow_tracking_uri())
mlflow.set_experiment(experiment_name)
```

## Part 1. Run distributed training using MLlib

This section shows a simple example of distributed training using SynapseML. For more information and examples, visit the official [website](https://microsoft.github.io/SynapseML/)

## Prepare Dataset
We use [*California Housing* dataset](https://scikit-learn.org/stable/datasets/real_world.html#california-housing-dataset). 
The data was derived from the 1990 U.S. census. It consists of 20640 entries with 8 features. 
We use `sklearn.datasets` module to download it easily, then split the set into training and testing by 75/25.


```python
import numpy as np
import pandas as pd
from sklearn.datasets import fetch_california_housing
import time
```


```python
try:
    california = fetch_california_housing()
except EOFError:
    print("Encountered EOFError while downloading, retrying once...")
    time.sleep(5)
    california = fetch_california_housing()

feature_cols = ["f" + str(i) for i in range(california.data.shape[1])]
header = ["target"] + feature_cols
df = spark.createDataFrame(
    pd.DataFrame(
        data=np.column_stack((california.target, california.data)), columns=header
    )
).repartition(1)

print("Dataframe has {} rows".format(df.count()))
display(df)
```

Following is the summary of the data set.


```python
display(df.summary().toPandas())
```

### Create a function to train a model

In this section, you define a function to train a gradient boosting model with SynapseML LightgbmRegressor.  Wrapping the training code in a function is important for passing the function to Hyperopt for tuning later.

We evaluate the prediction result by using `synapse.ml.train.ComputeModelStatistics` which returns four metrics:
* [MSE (Mean Squared Error)](https://en.wikipedia.org/wiki/Mean_squared_error)
* [RMSE (Root Mean Squared Error)](https://en.wikipedia.org/wiki/Root-mean-square_deviation) = sqrt(MSE)
* [R Squared](https://en.wikipedia.org/wiki/Coefficient_of_determination)
* [MAE (Mean Absolute Error)](https://en.wikipedia.org/wiki/Mean_absolute_error)


```python
from pyspark.ml.feature import VectorAssembler

# Convert features into a single vector column
featurizer = VectorAssembler(inputCols=feature_cols, outputCol="features")
data = featurizer.transform(df)["target", "features"]

train_data, test_data = data.randomSplit([0.75, 0.25], seed=42)
train_data, validation_data = train_data.randomSplit([0.85, 0.15], seed=42)

display(train_data)

# Using one partition since the training dataset is very small
repartitioned_data = train_data.repartition(1).cache()
```


```python
from synapse.ml.lightgbm import LightGBMRegressor
from synapse.ml.train import ComputeModelStatistics


def train_tree(alpha, learningRate, numLeaves, numIterations):
    """
    This train() function:
     - takes hyperparameters as inputs (for tuning later)
     - returns the F1 score on the validation dataset

    Wrapping code as a function makes it easier to reuse the code later with Hyperopt.
    """
    # Use MLflow to track training.
    # Specify "nested=True" since this single model will be logged as a child run of Hyperopt's run.
    with mlflow.start_run(nested=True):

        lgr = LightGBMRegressor(
            objective="quantile",
            alpha=alpha,
            learningRate=learningRate,
            numLeaves=numLeaves,
            labelCol="target",
            numIterations=numIterations,
        )

        model = lgr.fit(repartitioned_data)

        cms = ComputeModelStatistics(
            evaluationMetric="regression", labelCol="target", scoresCol="prediction"
        )

        # Define an evaluation metric and evaluate the model on the test dataset.
        predictions = model.transform(test_data)
        metrics = cms.transform(predictions).collect()[0].asDict()

        # log metrics with mlflow
        mlflow.log_metric("MSE", metrics["mean_squared_error"])
        mlflow.log_metric("RMSE", metrics["root_mean_squared_error"])
        mlflow.log_metric("R^2", metrics["R^2"])
        mlflow.log_metric("MAE", metrics["mean_absolute_error"])

    return model, metrics["R^2"]
```

Run the training function to make sure it works.
It's a good idea to make sure training code runs before adding in tuning.


```python
initial_model, val_metric = train_tree(
    alpha=0.2, learningRate=0.3, numLeaves=31, numIterations=50
)
print(
    f"The trained decision tree achieved a R^2 of {val_metric} on the validation data"
)
```

## Part 2. Use Hyperopt to tune hyperparameters

In the second section, the Hyperopt workflow is created by:
* Define a function to minimize
* Define a search space over hyperparameters
* Specifying the search algorithm and using `fmin()` for tuning the model.

For more information about the Hyperopt APIs, see the [Hyperopt documentation](http://hyperopt.github.io/hyperopt/).

### Define a function to minimize

* Input: hyperparameters
* Internally: Reuse the training function defined above.
* Output: loss


```python
from hyperopt import fmin, tpe, hp, Trials, STATUS_OK


def train_with_hyperopt(params):
    """
    An example train method that calls into MLlib.
    This method is passed to hyperopt.fmin().

    :param params: hyperparameters as a dict. Its structure is consistent with how search space is defined. See below.
    :return: dict with fields 'loss' (scalar loss) and 'status' (success/failure status of run)
    """
    # For integer parameters, make sure to convert them to int type if Hyperopt is searching over a continuous range of values.
    alpha = params["alpha"]
    learningRate = params["learningRate"]
    numLeaves = int(params["numLeaves"])
    numIterations = int(params["numIterations"])

    model, r_squared = train_tree(alpha, learningRate, numLeaves, numIterations)

    # Hyperopt expects you to return a loss (for which lower is better), so take the negative of the R^2 (for which higher is better).
    loss = -r_squared

    return {"loss": loss, "status": STATUS_OK}
```

### Define the search space over hyperparameters

This example tunes four hyperparameters: `alpha`, `learningRate`, `numLeaves` and `numIterations`. See the [Hyperopt documentation](https://github.com/hyperopt/hyperopt/wiki/FMin#21-parameter-expressions) for details on defining a search space and parameter expressions.


```python
space = {
    "alpha": hp.uniform("alpha", 0, 1),
    "learningRate": hp.uniform("learningRate", 0, 1),
    "numLeaves": hp.uniformint("numLeaves", 30, 50),
    "numIterations": hp.uniformint("numIterations", 20, 100),
}
```

### Tune the model using Hyperopt `fmin()`

For tuning the model with Hyperopt's `fmin()`, the following steps are taken:
- Setting `max_evals` to the maximum number of points in the hyperparameter space to be tested.
- Specifying the search algorithm, either `hyperopt.tpe.suggest` or `hyperopt.rand.suggest`.
  - `hyperopt.tpe.suggest`: Tree of Parzen Estimators, a Bayesian approach which iteratively and adaptively selects new hyperparameter settings to explore based on previous results
  - `hyperopt.rand.suggest`: Random search, a non-adaptive approach that randomly samples the search space

**Important:**  
When using Hyperopt with SynapseML and other distributed training algorithms, do not pass a `trials` argument to `fmin()`. When you do not include the `trials` argument, Hyperopt uses the default `Trials` class, which runs on the cluster driver. Hyperopt needs to evaluate each trial on the driver node so that each trial can initiate distributed training jobs.  

Do not use the `SparkTrials` class with SynapseML. `SparkTrials` is designed to distribute trials for algorithms that are not themselves distributed. SynapseML uses distributed computing already and is not compatible with `SparkTrials`.


```python
algo = tpe.suggest

with mlflow.start_run():
    best_params = fmin(fn=train_with_hyperopt, space=space, algo=algo, max_evals=8)
```


```python
# Print out the parameters that produced the best model
best_params
```

### Retrain the model on the full training dataset

For tuning, this workflow split the training dataset into training and validation subsets. Now, retrain the model using the "best" hyperparameters on the full training dataset.


```python
best_alpha = best_params["alpha"]
best_learningRate = best_params["learningRate"]
best_numIterations = int(best_params["numIterations"])
best_numLeaves = int(best_params["numLeaves"])

final_model, val_r_squared = train_tree(
    best_alpha, best_learningRate, best_numIterations, best_numLeaves
)
```

Use the test dataset to compare evaluation metrics for the initial and "best" models.


```python
# Define an evaluation metric and evaluate the model on the test dataset.
cms = ComputeModelStatistics(
    evaluationMetric="regression", labelCol="target", scoresCol="prediction"
)

initial_model_predictions = initial_model.transform(test_data)
initial_model_test_metric = (
    cms.transform(initial_model_predictions).collect()[0].asDict()["R^2"]
)

final_model_predictions = final_model.transform(test_data)
final_model_test_metric = (
    cms.transform(final_model_predictions).collect()[0].asDict()["R^2"]
)

print(
    f"On the test data, the initial (untuned) model achieved R^2 {initial_model_test_metric}, and the final (tuned) model achieved {final_model_test_metric}."
)
```
