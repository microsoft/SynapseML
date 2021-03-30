#!/usr/bin/env python
# coding: utf-8

# ## 101 - Training and Evaluating Classifiers with `mmlspark`
# 
# In this example, we try to predict incomes from the *Adult Census* dataset.
# 
# First, we import the packages (use `help(mmlspark)` to view contents),

# In[ ]:


import numpy as np
import pandas as pd


# Now let's read the data and split it to train and test sets:

# In[ ]:


data = spark.read.parquet("wasbs://publicwasb@mmlspark.blob.core.windows.net/AdultCensusIncome.parquet")
data = data.select(["education", "marital-status", "hours-per-week", "income"])
train, test = data.randomSplit([0.75, 0.25], seed=123)
train.limit(10).toPandas()


# `TrainClassifier` can be used to initialize and fit a model, it wraps SparkML classifiers.
# You can use `help(mmlspark.TrainClassifier)` to view the different parameters.
# 
# Note that it implicitly converts the data into the format expected by the algorithm: tokenize
# and hash strings, one-hot encodes categorical variables, assembles the features into a vector
# and so on.  The parameter `numFeatures` controls the number of hashed features.

# In[ ]:


from mmlspark.train import TrainClassifier
from pyspark.ml.classification import LogisticRegression
model = TrainClassifier(model=LogisticRegression(), labelCol="income", numFeatures=256).fit(train)


# After the model is trained, we score it against the test dataset and view metrics.

# In[ ]:


from mmlspark.train import ComputeModelStatistics, TrainedClassifierModel
prediction = model.transform(test)
metrics = ComputeModelStatistics().transform(prediction)
metrics.limit(10).toPandas()


# Finally, we save the model so it can be used in a scoring program.

# In[ ]:


model.write().overwrite().save("AdultCensus.mml")

