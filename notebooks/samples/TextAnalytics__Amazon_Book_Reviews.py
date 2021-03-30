#!/usr/bin/env python
# coding: utf-8

# ## 201 - Engineering Text Features Using `mmlspark` Modules and Spark SQL
# 
# Again, try to predict Amazon book ratings greater than 3 out of 5, this time using
# the `TextFeaturizer` module which is a composition of several text analytics APIs that
# are native to Spark.

# In[ ]:


import pandas as pd


# In[ ]:


data = spark.read.parquet("wasbs://publicwasb@mmlspark.blob.core.windows.net/BookReviewsFromAmazon10K.parquet")
data.limit(10).toPandas()


# Use `TextFeaturizer` to generate our features column.  We remove stop words, and use TF-IDF
# to generate 2²⁰ sparse features.

# In[ ]:


from mmlspark.featurize.text import TextFeaturizer
textFeaturizer = TextFeaturizer()   .setInputCol("text").setOutputCol("features")   .setUseStopWordsRemover(True).setUseIDF(True).setMinDocFreq(5).setNumFeatures(1 << 16).fit(data)


# In[ ]:


processedData = textFeaturizer.transform(data)
processedData.limit(5).toPandas()


# Change the label so that we can predict whether the rating is greater than 3 using a binary
# classifier.

# In[ ]:


processedData = processedData.withColumn("label", processedData["rating"] > 3)                              .select(["features", "label"])
processedData.limit(5).toPandas()


# Train several Logistic Regression models with different regularizations.

# In[ ]:


train, test, validation = processedData.randomSplit([0.60, 0.20, 0.20])
from pyspark.ml.classification import LogisticRegression

lrHyperParams = [0.05, 0.1, 0.2, 0.4]
logisticRegressions = [LogisticRegression(regParam = hyperParam) for hyperParam in lrHyperParams]

from mmlspark.train import TrainClassifier
lrmodels = [TrainClassifier(model=lrm, labelCol="label").fit(train) for lrm in logisticRegressions]


# Find the model with the best AUC on the test set.

# In[ ]:


from mmlspark.automl import FindBestModel, BestModel
bestModel = FindBestModel(evaluationMetric="AUC", models=lrmodels).fit(test)
bestModel.getEvaluationResults().show()
bestModel.getBestModelMetrics().show()
bestModel.getAllModelMetrics().show()


# Use the optimized `ComputeModelStatistics` API to find the model accuracy.

# In[ ]:


from mmlspark.train import ComputeModelStatistics
predictions = bestModel.transform(validation)
metrics = ComputeModelStatistics().transform(predictions)
print("Best model's accuracy on validation set = "
      + "{0:.2f}%".format(metrics.first()["accuracy"] * 100))

