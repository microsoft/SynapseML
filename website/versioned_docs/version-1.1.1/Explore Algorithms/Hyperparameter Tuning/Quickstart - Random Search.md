---
title: Quickstart - Random Search
hide_title: true
status: stable
---
# HyperParameterTuning - Fighting Breast Cancer

This tutorial shows how SynapseML can be used to identify the best combination of hyperparameters for your chosen classifiers, ultimately resulting in more accurate and reliable models. In order to demonstrate this, we'll show how to perform distributed randomized grid search hyperparameter tuning to build a model to identify breast cancer. 

## 1 - Set up dependencies
Start by importing pandas and setting up our Spark session.

Next, read the data and split it into tuning and test sets.


```python
data = spark.read.parquet(
    "wasbs://publicwasb@mmlspark.blob.core.windows.net/BreastCancer.parquet"
).cache()
tune, test = data.randomSplit([0.80, 0.20])
tune.limit(10).toPandas()
```

Define the models to be used.


```python
from synapse.ml.automl import TuneHyperparameters
from synapse.ml.train import TrainClassifier
from pyspark.ml.classification import (
    LogisticRegression,
    RandomForestClassifier,
    GBTClassifier,
)

logReg = LogisticRegression()
randForest = RandomForestClassifier()
gbt = GBTClassifier()
smlmodels = [logReg, randForest, gbt]
mmlmodels = [TrainClassifier(model=model, labelCol="Label") for model in smlmodels]
```

## 2 - Find the best model using AutoML

Import SynapseML's AutoML classes from `synapse.ml.automl`.
Specify the hyperparameters using the `HyperparamBuilder`. Add either `DiscreteHyperParam` or `RangeHyperParam` hyperparameters. `TuneHyperparameters` will randomly choose values from a uniform distribution:


```python
from synapse.ml.automl import *

paramBuilder = (
    HyperparamBuilder()
    .addHyperparam(logReg, logReg.regParam, RangeHyperParam(0.1, 0.3))
    .addHyperparam(randForest, randForest.numTrees, DiscreteHyperParam([5, 10]))
    .addHyperparam(randForest, randForest.maxDepth, DiscreteHyperParam([3, 5]))
    .addHyperparam(gbt, gbt.maxBins, RangeHyperParam(8, 16))
    .addHyperparam(gbt, gbt.maxDepth, DiscreteHyperParam([3, 5]))
)
searchSpace = paramBuilder.build()
# The search space is a list of params to tuples of estimator and hyperparam
print(searchSpace)
randomSpace = RandomSpace(searchSpace)
```

Next, run TuneHyperparameters to get the best model.


```python
bestModel = TuneHyperparameters(
    evaluationMetric="accuracy",
    models=mmlmodels,
    numFolds=2,
    numRuns=len(mmlmodels) * 2,
    parallelism=1,
    paramSpace=randomSpace.space(),
    seed=0,
).fit(tune)
```

## 3 - Evaluate the model
We can view the best model's parameters and retrieve the underlying best model pipeline


```python
print(bestModel.getBestModelInfo())
print(bestModel.getBestModel())
```

We can score against the test set and view metrics.


```python
from synapse.ml.train import ComputeModelStatistics

prediction = bestModel.transform(test)
metrics = ComputeModelStatistics().transform(prediction)
metrics.limit(10).toPandas()
```
