#!/usr/bin/env python
# coding: utf-8

# ## Regression -  Flight Delays with DataCleaning
# 
# This example notebook is similar to
# [Notebook 102](102 - Regression Example with Flight Delay Dataset.ipynb).
# In this example, we will demonstrate the use of `DataConversion()` in two
# ways.  First, to convert the data type of several columns after the dataset
# has been read in to the Spark DataFrame instead of specifying the data types
# as the file is read in.  Second, to convert columns to categorical columns
# instead of iterating over the columns and applying the `StringIndexer`.
# 
# This sample demonstrates how to use the following APIs:
# - [`TrainRegressor`
#   ](http://mmlspark.azureedge.net/docs/pyspark/TrainRegressor.html)
# - [`ComputePerInstanceStatistics`
#   ](http://mmlspark.azureedge.net/docs/pyspark/ComputePerInstanceStatistics.html)
# - [`DataConversion`
#   ](http://mmlspark.azureedge.net/docs/pyspark/DataConversion.html)
# 
# First, import the pandas package

# In[ ]:


import os
if os.environ.get("AZURE_SERVICE", None) == "Microsoft.ProjectArcadia":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()


# In[ ]:


import pandas as pd


# Next, import the CSV dataset: retrieve the file if needed, save it locally,
# read the data into a pandas dataframe via `read_csv()`, then convert it to
# a Spark dataframe.
# 
# Print the schema of the dataframe, and note the columns that are `long`.

# In[ ]:


flightDelay = spark.read.parquet("wasbs://publicwasb@mmlspark.blob.core.windows.net/On_Time_Performance_2012_9.parquet")
# print some basic info
print("records read: " + str(flightDelay.count()))
print("Schema: ")
flightDelay.printSchema()
flightDelay.limit(10).toPandas()


# Use the `DataConversion` transform API to convert the columns listed to
# double.
# 
# The `DataConversion` API accepts the following types for the `convertTo`
# parameter:
# * `boolean`
# * `byte`
# * `short`
# * `integer`
# * `long`
# * `float`
# * `double`
# * `string`
# * `toCategorical`
# * `clearCategorical`
# * `date` -- converts a string or long to a date of the format
#   "yyyy-MM-dd HH:mm:ss" unless another format is specified by
# the `dateTimeFormat` parameter.
# 
# Again, print the schema and note that the columns are now `double`
# instead of long.

# In[ ]:


from mmlspark.featurize import DataConversion
flightDelay = DataConversion(cols=["Quarter","Month","DayofMonth","DayOfWeek",
                                   "OriginAirportID","DestAirportID",
                                   "CRSDepTime","CRSArrTime"],
                             convertTo="double") \
                  .transform(flightDelay)
flightDelay.printSchema()
flightDelay.limit(10).toPandas()


# Split the datasest into train and test sets.

# In[ ]:


train, test = flightDelay.randomSplit([0.75, 0.25])


# Create a regressor model and train it on the dataset.
# 
# First, use `DataConversion` to convert the columns `Carrier`, `DepTimeBlk`,
# and `ArrTimeBlk` to categorical data.  Recall that in Notebook 102, this
# was accomplished by iterating over the columns and converting the strings
# to index values using the `StringIndexer` API.  The `DataConversion` API
# simplifies the task by allowing you to specify all columns that will have
# the same end type in a single command.
# 
# Create a LinearRegression model using the Limited-memory BFGS solver
# (`l-bfgs`), an `ElasticNet` mixing parameter of `0.3`, and a `Regularization`
# of `0.1`.
# 
# Train the model with the `TrainRegressor` API fit on the training dataset.

# In[ ]:


from mmlspark.train import TrainRegressor, TrainedRegressorModel
from pyspark.ml.regression import LinearRegression

trainCat = DataConversion(cols=["Carrier","DepTimeBlk","ArrTimeBlk"],
                          convertTo="toCategorical") \
               .transform(train)
testCat  = DataConversion(cols=["Carrier","DepTimeBlk","ArrTimeBlk"],
                          convertTo="toCategorical") \
               .transform(test)
lr = LinearRegression().setRegParam(0.1)                        .setElasticNetParam(0.3)
model = TrainRegressor(model=lr, labelCol="ArrDelay").fit(trainCat)


# Score the regressor on the test data.

# In[ ]:


scoredData = model.transform(testCat)
scoredData.limit(10).toPandas()


# Compute model metrics against the entire scored dataset

# In[ ]:


from mmlspark.train import ComputeModelStatistics
metrics = ComputeModelStatistics().transform(scoredData)
metrics.toPandas()


# Finally, compute and show statistics on individual predictions in the test
# dataset, demonstrating the usage of `ComputePerInstanceStatistics`

# In[ ]:


from mmlspark.train import ComputePerInstanceStatistics
evalPerInstance = ComputePerInstanceStatistics().transform(scoredData)
evalPerInstance.select("ArrDelay", "Scores", "L1_loss", "L2_loss")                .limit(10).toPandas()

