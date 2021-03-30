#!/usr/bin/env python
# coding: utf-8

# ## 303 - Transfer Learning by DNN Featurization
# 
# Classify automobile vs airplane using DNN featurization and transfer learning
# against a subset of images from CIFAR-10 dataset.

# Load DNN Model and pick one of the inner layers as feature output

# In[ ]:


from mmlspark.cntk import CNTKModel
from mmlspark.downloader import ModelDownloader
import numpy as np, os, urllib, tarfile, pickle, array
from os.path import abspath
from pyspark.sql.functions import col, udf
from pyspark.sql.types import *
modelName = "ConvNet"
modelDir = "file:" + abspath("models")
d = ModelDownloader(spark, modelDir)
model = d.downloadByName(modelName)
print(model.layerNames)
cntkModel = CNTKModel().setInputCol("images").setOutputCol("features")                        .setModelLocation(model.uri).setOutputNode("l8")


# Format raw CIFAR data into correct shape.

# In[ ]:


imagesWithLabels = spark.read.parquet("wasbs://publicwasb@mmlspark.blob.core.windows.net/CIFAR10_test.parquet")


# Select airplanes (label=0) and automobiles (label=1)

# In[ ]:


imagesWithLabels = imagesWithLabels.filter("labels<2")
imagesWithLabels.cache()


# Featurize images

# In[ ]:


featurizedImages = cntkModel.transform(imagesWithLabels).select(["features","labels"])


# Use featurized images to train a classifier

# In[ ]:


from mmlspark.train import TrainClassifier
from pyspark.ml.classification import RandomForestClassifier

train,test = featurizedImages.randomSplit([0.75,0.25])

model = TrainClassifier(model=RandomForestClassifier(),labelCol="labels").fit(train)


# Evaluate the accuracy of the model

# In[ ]:


from mmlspark.train import ComputeModelStatistics
predictions = model.transform(test)
metrics = ComputeModelStatistics(evaluationMetric="accuracy").transform(predictions)
metrics.show()

