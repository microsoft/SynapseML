#!/usr/bin/env python
# coding: utf-8

# ## Interpretability - Image Explainers
# 
# In this example, we use LIME and Kernel SHAP explainers to explain the ResNet50 model's multi-class output of an image.
# 
# First we import the packages and define some UDFs and a plotting function we will need later.

# In[ ]:


from mmlspark.downloader import ModelDownloader
from mmlspark.explainers import *
from mmlspark.cntk import ImageFeaturizer
from mmlspark.stages import UDFTransformer
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
import pyspark
import urllib.request
import matplotlib.pyplot as plt
import PIL, io
from PIL import Image

vec_access = udf(lambda vec, i: float(vec[i]), FloatType())
vec_slice = udf(lambda vec, indices: (vec.toArray())[indices].tolist(), ArrayType(FloatType()))
arg_top = udf(lambda vec, n: (-vec.toArray()).argsort()[:n].tolist(), ArrayType(IntegerType()))


def plot_superpixels(image_data, sp_clusters, weights):
    image_bytes = image_data
    superpixels = sp_clusters
    green_value = np.percentile(weights, 80)
    red_value = np.percentile(weights, 20)
    img = (PIL.Image.open(io.BytesIO(image_bytes))).convert("RGBA")
    image_array = np.asarray(img).copy()
    for (sp, v) in zip(superpixels, weights):
        if v > green_value:
            for (x, y) in sp:
                image_array[y, x, 1] = 255
                image_array[y, x, 3] = 200
        if v < red_value:
            for (x, y) in sp:
                image_array[y, x, 0] = 255
                image_array[y, x, 3] = 200
    plt.clf()
    plt.imshow(image_array)
    display()


# We download an image for interpretation.

# In[ ]:


test_image_url = (
    "https://mmlspark.blob.core.windows.net/publicwasb/explainers/images/david-lusvardi-dWcUncxocQY-unsplash.jpg"
)
with urllib.request.urlopen(test_image_url) as url:
    barr = url.read()

img = (PIL.Image.open(io.BytesIO(barr))).convert("RGBA")
image_array = np.asarray(img).copy()

plt.clf()
plt.imshow(image_array)
display()


# Create a dataframe from the downloaded image, and use ResNet50 model to infer the image.
# 
# The result shows 88.7% probability of "upright piano", and 9.6% probability of "cello".

# In[ ]:


image_df = spark.createDataFrame([(bytearray(barr),)], ["image"])

network = ModelDownloader(spark, "dbfs:/Models/").downloadByName("ResNet50")

model = ImageFeaturizer(inputCol="image", outputCol="probability", cutOutputLayers=0).setModel(network)

predicted = (
    model.transform(image_df)
    .withColumn("top2pred", arg_top(col("probability"), lit(2)))
    .withColumn("top2prob", vec_slice(col("probability"), col("top2pred")))
)

display(predicted.select("top2pred", "top2prob"))


# First we use the LIME image explainer to explain the model's top 2 classes' probabilities.

# In[ ]:


lime = (
    ImageLIME()
    .setModel(model)
    .setOutputCol("weights")
    .setInputCol("image")
    .setCellSize(50.0)
    .setModifier(20.0)
    .setNumSamples(500)
    .setMetricsCol("r2")
    .setTargetCol("probability")
    .setTargetClassesCol("top2pred")
    .setSamplingFraction(0.7)
)

lime_result = (
    lime.transform(predicted)
    .withColumn("weights_piano", col("weights").getItem(0))
    .withColumn("weights_cello", col("weights").getItem(1))
    .withColumn("r2_piano", vec_access("r2", lit(0)))
    .withColumn("r2_cello", vec_access("r2", lit(1)))
    .cache()
)

display(lime_result.select(col("weights_piano"), col("r2_piano"), col("weights_cello"), col("r2_cello")))
lime_row = lime_result.head()


# We plot the LIME weights for "piano" output and "cell" output.
# 
# Green area are superpixels with LIME weights above 90 percentile, and red area are superpixels with LIME weights below 10 percentile.

# In[ ]:


plot_superpixels(barr, lime_row["superpixels"]["clusters"], list(lime_row["weights_piano"]))
plot_superpixels(barr, lime_row["superpixels"]["clusters"], list(lime_row["weights_cello"]))


# Your results will look like:
# 
# <img src="https://mmlspark.blob.core.windows.net/graphics/explainers/image-lime.png"/>

# Then we use the Kernel SHAP image explainer to explain the model's top 2 classes' probabilities.

# In[ ]:


shap = (
    ImageSHAP()
    .setModel(model)
    .setOutputCol("shaps")
    .setSuperpixelCol("superpixels")
    .setInputCol("image")
    .setCellSize(50.0)
    .setModifier(20.0)
    .setNumSamples(500)
    .setMetricsCol("r2")
    .setTargetCol("probability")
    .setTargetClassesCol("top2pred")
)

shap_result = (
    shap.transform(predicted)
    .withColumn("shaps_piano", col("shaps").getItem(0))
    .withColumn("shaps_cello", col("shaps").getItem(1))
    .withColumn("r2_piano", vec_access("r2", lit(0)))
    .withColumn("r2_cello", vec_access("r2", lit(1)))
    .cache()
)

display(shap_result.select(col("shaps_piano"), col("r2_piano"), col("shaps_cello"), col("r2_cello")))
shap_row = shap_result.head()


# We plot the SHAP values for "piano" output and "cell" output.
# 
# Green area are superpixels with SHAP values above 90 percentile, and red area are superpixels with SHAP values below 10 percentile.
# 
# > Notice that we drop the base value from the SHAP output before rendering the superpixels. The base value is the model output for the background (all black) image.

# In[ ]:


plot_superpixels(barr, shap_row["superpixels"]["clusters"], list(shap_row["shaps_piano"][1:]))
plot_superpixels(barr, shap_row["superpixels"]["clusters"], list(shap_row["shaps_cello"][1:]))


# Your results will look like:
# 
# <img src="https://mmlspark.blob.core.windows.net/graphics/explainers/image-shap.png"/>
