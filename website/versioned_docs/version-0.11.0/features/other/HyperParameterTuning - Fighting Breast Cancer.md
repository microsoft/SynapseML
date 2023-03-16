---
title: HyperParameterTuning - Fighting Breast Cancer
hide_title: true
status: stable
---
## HyperParameterTuning - Fighting Breast Cancer

We can do distributed randomized grid search hyperparameter tuning with SynapseML.

First, we import the packages


```python
import pandas as pd
from pyspark.sql import SparkSession

# Bootstrap Spark Session
spark = SparkSession.builder.getOrCreate()
```

Now let's read the data and split it to tuning and test sets:


```python
data = spark.read.parquet(
    "wasbs://publicwasb@mmlspark.blob.core.windows.net/BreastCancer.parquet"
).cache()
tune, test = data.randomSplit([0.80, 0.20])
tune.limit(10).toPandas()
```

Next, define the models that will be tuned:


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

We can specify the hyperparameters using the HyperparamBuilder.
We can add either DiscreteHyperParam or RangeHyperParam hyperparameters.
TuneHyperparameters will randomly choose values from a uniform distribution.


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
