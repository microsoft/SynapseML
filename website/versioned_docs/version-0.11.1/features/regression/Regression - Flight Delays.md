---
title: Regression - Flight Delays
hide_title: true
status: stable
---
## Regression - Flight Delays

In this example, we run a linear regression on the *Flight Delay* dataset to predict the delay times.

We demonstrate how to use the `TrainRegressor` and the `ComputePerInstanceStatistics` APIs.

First, import the packages.


```python
from pyspark.sql import SparkSession

# Bootstrap Spark Session
spark = SparkSession.builder.getOrCreate()
```


```python
import numpy as np
import pandas as pd
import synapse.ml
```

Next, import the CSV dataset.


```python
flightDelay = spark.read.parquet(
    "wasbs://publicwasb@mmlspark.blob.core.windows.net/On_Time_Performance_2012_9.parquet"
)
# print some basic info
print("records read: " + str(flightDelay.count()))
print("Schema: ")
flightDelay.printSchema()
flightDelay.limit(10).toPandas()
```

Split the dataset into train and test sets.


```python
train, test = flightDelay.randomSplit([0.75, 0.25])
```

Train a regressor on dataset with `l-bfgs`.


```python
from synapse.ml.train import TrainRegressor, TrainedRegressorModel
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import StringIndexer

# Convert columns to categorical
catCols = ["Carrier", "DepTimeBlk", "ArrTimeBlk"]
trainCat = train
testCat = test
for catCol in catCols:
    simodel = StringIndexer(inputCol=catCol, outputCol=catCol + "Tmp").fit(train)
    trainCat = (
        simodel.transform(trainCat)
        .drop(catCol)
        .withColumnRenamed(catCol + "Tmp", catCol)
    )
    testCat = (
        simodel.transform(testCat)
        .drop(catCol)
        .withColumnRenamed(catCol + "Tmp", catCol)
    )
lr = LinearRegression().setRegParam(0.1).setElasticNetParam(0.3)
model = TrainRegressor(model=lr, labelCol="ArrDelay").fit(trainCat)
```

Save, load, or Score the regressor on the test data.


```python
from synapse.ml.core.platform import *

if running_on_synapse():
    model_name = "/models/flightDelayModel.mml"
elif running_on_synapse_internal():
    model_name = "Files/models/flightDelayModel.mml"
elif running_on_databricks():
    model_name = "dbfs:/flightDelayModel.mml"
else:
    model_name = "/tmp/flightDelayModel.mml"

model.write().overwrite().save(model_name)
flightDelayModel = TrainedRegressorModel.load(model_name)

scoredData = flightDelayModel.transform(testCat)
scoredData.limit(10).toPandas()
```

Compute model metrics against the entire scored dataset


```python
from synapse.ml.train import ComputeModelStatistics

metrics = ComputeModelStatistics().transform(scoredData)
metrics.toPandas()
```

Finally, compute and show per-instance statistics, demonstrating the usage
of `ComputePerInstanceStatistics`.


```python
from synapse.ml.train import ComputePerInstanceStatistics

evalPerInstance = ComputePerInstanceStatistics().transform(scoredData)
evalPerInstance.select("ArrDelay", "prediction", "L1_loss", "L2_loss").limit(
    10
).toPandas()
```
