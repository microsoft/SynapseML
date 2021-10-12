---
title: Regression - Flight Delays with DataCleaning
hide_title: true
status: stable
---
## Regression -  Flight Delays with DataCleaning

This example notebook is similar to
[Regression - Flight Delays](https://github.com/microsoft/SynapseML/blob/master/notebooks/Regression%20-%20Flight%20Delays.ipynb).
In this example, we will demonstrate the use of `DataConversion()` in two
ways.  First, to convert the data type of several columns after the dataset
has been read in to the Spark DataFrame instead of specifying the data types
as the file is read in.  Second, to convert columns to categorical columns
instead of iterating over the columns and applying the `StringIndexer`.

This sample demonstrates how to use the following APIs:
- [`TrainRegressor`
  ](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc4/pyspark/mmlspark.train.html?#module-mmlspark.train.TrainRegressor)
- [`ComputePerInstanceStatistics`
  ](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc4/pyspark/mmlspark.train.html?#module-mmlspark.train.ComputePerInstanceStatistics)
- [`DataConversion`
  ](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc4/pyspark/mmlspark.featurize.html?#module-mmlspark.featurize.DataConversion)

First, import the pandas package


```python
import os
if os.environ.get("AZURE_SERVICE", None) == "Microsoft.ProjectArcadia":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
```


```python
import pandas as pd
```

Next, import the CSV dataset: retrieve the file if needed, save it locally,
read the data into a pandas dataframe via `read_csv()`, then convert it to
a Spark dataframe.

Print the schema of the dataframe, and note the columns that are `long`.


```python
flightDelay = spark.read.parquet("wasbs://publicwasb@mmlspark.blob.core.windows.net/On_Time_Performance_2012_9.parquet")
# print some basic info
print("records read: " + str(flightDelay.count()))
print("Schema: ")
flightDelay.printSchema()
flightDelay.limit(10).toPandas()
```

Use the `DataConversion` transform API to convert the columns listed to
double.

The `DataConversion` API accepts the following types for the `convertTo`
parameter:
* `boolean`
* `byte`
* `short`
* `integer`
* `long`
* `float`
* `double`
* `string`
* `toCategorical`
* `clearCategorical`
* `date` -- converts a string or long to a date of the format
  "yyyy-MM-dd HH:mm:ss" unless another format is specified by
the `dateTimeFormat` parameter.

Again, print the schema and note that the columns are now `double`
instead of long.


```python
from mmlspark.featurize import DataConversion
flightDelay = DataConversion(cols=["Quarter","Month","DayofMonth","DayOfWeek",
                                   "OriginAirportID","DestAirportID",
                                   "CRSDepTime","CRSArrTime"],
                             convertTo="double") \
                  .transform(flightDelay)
flightDelay.printSchema()
flightDelay.limit(10).toPandas()
```

Split the datasest into train and test sets.


```python
train, test = flightDelay.randomSplit([0.75, 0.25])
```

Create a regressor model and train it on the dataset.

First, use `DataConversion` to convert the columns `Carrier`, `DepTimeBlk`,
and `ArrTimeBlk` to categorical data.  Recall that in Notebook 102, this
was accomplished by iterating over the columns and converting the strings
to index values using the `StringIndexer` API.  The `DataConversion` API
simplifies the task by allowing you to specify all columns that will have
the same end type in a single command.

Create a LinearRegression model using the Limited-memory BFGS solver
(`l-bfgs`), an `ElasticNet` mixing parameter of `0.3`, and a `Regularization`
of `0.1`.

Train the model with the `TrainRegressor` API fit on the training dataset.


```python
from mmlspark.train import TrainRegressor, TrainedRegressorModel
from pyspark.ml.regression import LinearRegression

trainCat = DataConversion(cols=["Carrier","DepTimeBlk","ArrTimeBlk"],
                          convertTo="toCategorical") \
               .transform(train)
testCat  = DataConversion(cols=["Carrier","DepTimeBlk","ArrTimeBlk"],
                          convertTo="toCategorical") \
               .transform(test)
lr = LinearRegression().setRegParam(0.1) \
                       .setElasticNetParam(0.3)
model = TrainRegressor(model=lr, labelCol="ArrDelay").fit(trainCat)
```

Score the regressor on the test data.


```python
scoredData = model.transform(testCat)
scoredData.limit(10).toPandas()
```

Compute model metrics against the entire scored dataset


```python
from mmlspark.train import ComputeModelStatistics
metrics = ComputeModelStatistics().transform(scoredData)
metrics.toPandas()
```

Finally, compute and show statistics on individual predictions in the test
dataset, demonstrating the usage of `ComputePerInstanceStatistics`


```python
from mmlspark.train import ComputePerInstanceStatistics
evalPerInstance = ComputePerInstanceStatistics().transform(scoredData)
evalPerInstance.select("ArrDelay", "Scores", "L1_loss", "L2_loss") \
               .limit(10).toPandas()
```
