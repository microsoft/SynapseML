#!/usr/bin/env python
# coding: utf-8

# ## 106 - Quantile Regression with VowpalWabbit
# 
# We will demonstrate how to use the VowpalWabbit quantile regressor with
# TrainRegressor and ComputeModelStatistics on the Triazines dataset.
# 
# 
# This sample demonstrates how to use the following APIs:
# - [`TrainRegressor`
#   ](http://mmlspark.azureedge.net/docs/pyspark/TrainRegressor.html)
# - [`VowpalWabbitRegressor`
#   ](http://mmlspark.azureedge.net/docs/pyspark/VowpalWabbitRegressor.html)
# - [`ComputeModelStatistics`
#   ](http://mmlspark.azureedge.net/docs/pyspark/ComputeModelStatistics.html)

# In[ ]:


triazines = spark.read.format("libsvm")    .load("wasbs://publicwasb@mmlspark.blob.core.windows.net/triazines.scale.svmlight")


# In[ ]:


# print some basic info
print("records read: " + str(triazines.count()))
print("Schema: ")
triazines.printSchema()
triazines.limit(10).toPandas()


# Split the dataset into train and test

# In[ ]:


train, test = triazines.randomSplit([0.85, 0.15], seed=1)


# Train the quantile regressor on the training data.
# 
# Note: have a look at stderr for the task to see VW's output
# 
# Full command line argument docs can be found [here](https://github.com/VowpalWabbit/vowpal_wabbit/wiki/Command-Line-Arguments).
# 
# Learning rate, numPasses and power_t are exposed to support grid search.

# In[ ]:


from mmlspark.vw import VowpalWabbitRegressor
model = (VowpalWabbitRegressor(numPasses=20, args="--holdout_off --loss_function quantile -q :: -l 0.1")
            .fit(train))


# Score the regressor on the test data.

# In[ ]:


scoredData = model.transform(test)
scoredData.limit(10).toPandas()


# Compute metrics using ComputeModelStatistics

# In[ ]:


from mmlspark.train import ComputeModelStatistics
metrics = ComputeModelStatistics(evaluationMetric='regression',
                                 labelCol='label',
                                 scoresCol='prediction') \
            .transform(scoredData)
metrics.toPandas()

