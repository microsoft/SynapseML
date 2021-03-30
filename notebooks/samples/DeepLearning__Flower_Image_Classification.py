#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.ml import Transformer, Estimator, Pipeline
from pyspark.ml.classification import LogisticRegression
from mmlspark.downloader import ModelDownloader
import os, sys, time


# In[ ]:


model = ModelDownloader(spark, "dbfs:/models/").downloadByName("ResNet50")


# In[ ]:


# Load the images
# use flowers_and_labels.parquet on larger cluster in order to get better results
imagesWithLabels = spark.read.parquet("wasbs://publicwasb@mmlspark.blob.core.windows.net/flowers_and_labels2.parquet")     .withColumnRenamed("bytes","image").sample(.1)

imagesWithLabels.printSchema()


# ![Smiley face](https://i.imgur.com/p2KgdYL.jpg)

# In[ ]:


from mmlspark.opencv import ImageTransformer
from mmlspark.image import UnrollImage, ImageFeaturizer
from mmlspark.stages import *

# Make some featurizers
it = ImageTransformer()    .setOutputCol("scaled")    .resize(height = 60, width = 60)

ur = UnrollImage()    .setInputCol("scaled")    .setOutputCol("features")
    
dc1 = DropColumns().setCols(["scaled", "image"])

lr1 = LogisticRegression().setMaxIter(8).setFeaturesCol("features").setLabelCol("labels")

dc2 = DropColumns().setCols(["features"])

basicModel = Pipeline(stages=[it, ur, dc1, lr1, dc2])


# In[ ]:


resnet = ImageFeaturizer()    .setInputCol("image")    .setOutputCol("features")    .setModelLocation(model.uri)    .setLayerNames(model.layerNames)    .setCutOutputLayers(1)
    
dc3 = DropColumns().setCols(["image"])
    
lr2 = LogisticRegression().setMaxIter(8).setFeaturesCol("features").setLabelCol("labels")

dc4 = DropColumns().setCols(["features"])

deepModel = Pipeline(stages=[resnet, dc3, lr2, dc4])    


# ![Resnet 18](https://i.imgur.com/Mb4Dyou.png)

# ### How does it work?
# 
# ![Convolutional network weights](http://i.stack.imgur.com/Hl2H6.png)

# ### Run the experiment

# In[ ]:


def timedExperiment(model, train, test):
  start = time.time()
  result =  model.fit(train).transform(test).toPandas()
  print("Experiment took {}s".format(time.time() - start))
  return result


# In[ ]:


train, test = imagesWithLabels.randomSplit([.8,.2])
train.count(), test.count()


# In[ ]:


basicResults = timedExperiment(basicModel, train, test)


# In[ ]:


deepResults = timedExperiment(deepModel, train, test)


# ### Plot confusion matrix.

# In[ ]:


import matplotlib.pyplot as plt
from sklearn.metrics import confusion_matrix
import numpy as np

def evaluate(results, name):
    y, y_hat = results["labels"],results["prediction"]
    y = [int(l) for l in y]

    accuracy = np.mean([1. if pred==true else 0. for (pred,true) in zip(y_hat,y)])
    cm = confusion_matrix(y, y_hat)
    cm = cm.astype("float") / cm.sum(axis=1)[:, np.newaxis]

    plt.text(40, 10,"$Accuracy$ $=$ ${}\%$".format(round(accuracy*100,1)),fontsize=14)
    plt.imshow(cm, interpolation="nearest", cmap=plt.cm.Blues)
    plt.colorbar()
    plt.xlabel("$Predicted$ $label$", fontsize=18)
    plt.ylabel("$True$ $Label$", fontsize=18)
    plt.title("$Normalized$ $CM$ $for$ ${}$".format(name))

plt.figure(figsize=(12,5))
plt.subplot(1,2,1)
evaluate(deepResults,"CNTKModel + LR")
plt.subplot(1,2,2)
evaluate(basicResults,"LR")
# Note that on the larger dataset the accuracy will bump up from 44% to >90%
display(plt.show())

