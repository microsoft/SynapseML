#!/usr/bin/env python
# coding: utf-8

# # Classification - Adult Census using Vowpal Wabbit in MMLSpark
# 
# In this example, we predict incomes from the *Adult Census* dataset using Vowpal Wabbit (VW) classifier in MMLSpark.
# First, we read the data and split it into train and test sets as in this [example](https://github.com/Azure/mmlspark/blob/master/notebooks/samples/Classification%20-%20Adult%20Census.ipynb
# ).

# In[ ]:


data = spark.read.parquet("wasbs://publicwasb@mmlspark.blob.core.windows.net/AdultCensusIncome.parquet")
data = data.select(["education", "marital-status", "hours-per-week", "income"])
train, test = data.randomSplit([0.75, 0.25], seed=123)
train.limit(10).toPandas()


# Next, we define a pipeline that includes feature engineering and training of a VW classifier. We use a featurizer provided by VW that hashes the feature names. 
# Note that VW expects classification labels being -1 or 1. Thus, the income category is mapped to this space before feeding training data into the pipeline.

# In[ ]:


from pyspark.sql.functions import when, col
from pyspark.ml import Pipeline
from mmlspark.vw import VowpalWabbitFeaturizer, VowpalWabbitClassifier

# Define classification label
train = train.withColumn("label", when(col("income").contains("<"), 0.0).otherwise(1.0)).repartition(1).cache()
print(train.count())

# Specify featurizer
vw_featurizer = VowpalWabbitFeaturizer(inputCols=["education", "marital-status", "hours-per-week"],
                                       outputCol="features")

# Define VW classification model
args = "--loss_function=logistic --quiet --holdout_off"
vw_model = VowpalWabbitClassifier(featuresCol="features",
                                  labelCol="label",
                                  args=args,
                                  numPasses=10)

# Create a pipeline
vw_pipeline = Pipeline(stages=[vw_featurizer, vw_model])


# Then, we are ready to train the model by fitting the pipeline with the training data.

# In[ ]:


# Train the model
vw_trained = vw_pipeline.fit(train)


# After the model is trained, we apply it to predict the income of each sample in the test set.

# In[ ]:


# Making predictions
test = test.withColumn("label", when(col("income").contains("<"), 0.0).otherwise(1.0))
prediction = vw_trained.transform(test)
prediction.limit(10).toPandas()


# Finally, we evaluate the model performance using `ComputeModelStatistics` function which will compute confusion matrix, accuracy, precision, recall, and AUC by default for classificaiton models.

# In[10]:


from mmlspark.train import ComputeModelStatistics
metrics = ComputeModelStatistics(evaluationMetric="classification", 
                                 labelCol="label", 
                                 scoredLabelsCol="prediction").transform(prediction)
metrics.toPandas()

