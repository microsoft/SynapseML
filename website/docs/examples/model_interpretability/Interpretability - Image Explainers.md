---
title: Interpretability - Image Explainers
hide_title: true
status: stable
---
## Interpretability - Image Explainers

In this example, we use LIME and Kernel SHAP explainers to explain the ResNet50 model's multi-class output of an image.

First we import the packages and define some UDFs and a plotting function we will need later.


```python
from mmlspark.explainers import *

from mmlspark.onnx import ONNXModel

from mmlspark.opencv import ImageTransformer

from mmlspark.io import *

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



vec_slice = udf(lambda vec, indices: (vec.toArray())[indices].tolist(), ArrayType(FloatType()))

arg_top_k = udf(lambda vec, k: (-vec.toArray()).argsort()[:k].tolist(), ArrayType(IntegerType()))



def downloadBytes(url: str):

  with urllib.request.urlopen(url) as url:

    barr = url.read()

    return barr



def rotate_color_channel(bgr_image_array, height, width, nChannels):

  B, G, R, *_ = np.asarray(bgr_image_array).reshape(height, width, nChannels).T

  rgb_image_array = np.array((R, G, B)).T

  return rgb_image_array

    

def plot_superpixels(image_rgb_array, sp_clusters, weights, green_threshold=99):

    superpixels = sp_clusters

    green_value = np.percentile(weights, green_threshold)

    img = Image.fromarray(image_rgb_array, mode='RGB').convert("RGBA")

    image_array = np.asarray(img).copy()

    for (sp, v) in zip(superpixels, weights):

        if v > green_value:

            for (x, y) in sp:

                image_array[y, x, 1] = 255

                image_array[y, x, 3] = 200

    plt.clf()

    plt.imshow(image_array)

    display()
```

Create a dataframe for a testing image, and use the ResNet50 ONNX model to infer the image.

The result shows 39.6% probability of "violin" (889), and 38.4% probability of "upright piano" (881).


```python
from mmlspark.io import *



image_df = spark.read.image().load("wasbs://publicwasb@mmlspark.blob.core.windows.net/explainers/images/david-lusvardi-dWcUncxocQY-unsplash.jpg")

display(image_df)



# Rotate the image array from BGR into RGB channels for visualization later.

row = image_df.select("image.height", "image.width", "image.nChannels", "image.data").head()

locals().update(row.asDict())

rgb_image_array = rotate_color_channel(data, height, width, nChannels)



# Download the ONNX model

modelPayload = downloadBytes("https://mmlspark.blob.core.windows.net/publicwasb/ONNXModels/resnet50-v2-7.onnx")



featurizer = (

  ImageTransformer(inputCol="image", outputCol="features")

      .resize(224, True)

      .centerCrop(224, 224)

      .normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225], color_scale_factor = 1/255)

      .setTensorElementType(FloatType())

)



onnx = (

  ONNXModel()

      .setModelPayload(modelPayload)

      .setFeedDict({"data": "features"})

      .setFetchDict({"rawPrediction": "resnetv24_dense0_fwd"})

      .setSoftMaxDict({"rawPrediction": "probability"})

      .setMiniBatchSize(1)

)



model = Pipeline(stages=[featurizer, onnx]).fit(image_df)
```


```python
predicted = (

    model.transform(image_df)

      .withColumn("top2pred", arg_top_k(col("probability"), lit(2)))

      .withColumn("top2prob", vec_slice(col("probability"), col("top2pred")))

)



display(predicted.select("top2pred", "top2prob"))
```

First we use the LIME image explainer to explain the model's top 2 classes' probabilities.


```python
lime = (

    ImageLIME()

    .setModel(model)

    .setOutputCol("weights")

    .setInputCol("image")

    .setCellSize(150.0)

    .setModifier(50.0)

    .setNumSamples(500)

    .setTargetCol("probability")

    .setTargetClassesCol("top2pred")

    .setSamplingFraction(0.7)

)



lime_result = (

    lime.transform(predicted)

    .withColumn("weights_violin", col("weights").getItem(0))

    .withColumn("weights_piano", col("weights").getItem(1))

    .cache()

)



display(lime_result.select(col("weights_violin"), col("weights_piano")))

lime_row = lime_result.head()
```

We plot the LIME weights for "violin" output and "upright piano" output.

Green area are superpixels with LIME weights above 95 percentile.


```python
plot_superpixels(rgb_image_array, lime_row["superpixels"]["clusters"], list(lime_row["weights_violin"]), 95)

plot_superpixels(rgb_image_array, lime_row["superpixels"]["clusters"], list(lime_row["weights_piano"]), 95)
```

Your results will look like:



<img src="https://mmlspark.blob.core.windows.net/graphics/explainers/image-lime-20210811.png"/>

Then we use the Kernel SHAP image explainer to explain the model's top 2 classes' probabilities.


```python
shap = (

    ImageSHAP()

    .setModel(model)

    .setOutputCol("shaps")

    .setSuperpixelCol("superpixels")

    .setInputCol("image")

    .setCellSize(150.0)

    .setModifier(50.0)

    .setNumSamples(500)

    .setTargetCol("probability")

    .setTargetClassesCol("top2pred")

)



shap_result = (

    shap.transform(predicted)

    .withColumn("shaps_violin", col("shaps").getItem(0))

    .withColumn("shaps_piano", col("shaps").getItem(1))

    .cache()

)



display(shap_result.select(col("shaps_violin"), col("shaps_piano")))

shap_row = shap_result.head()
```

We plot the SHAP values for "piano" output and "cell" output.

Green area are superpixels with SHAP values above 95 percentile.

> Notice that we drop the base value from the SHAP output before rendering the superpixels. The base value is the model output for the background (all black) image.


```python
plot_superpixels(rgb_image_array, shap_row["superpixels"]["clusters"], list(shap_row["shaps_violin"][1:]), 95)

plot_superpixels(rgb_image_array, shap_row["superpixels"]["clusters"], list(shap_row["shaps_piano"][1:]), 95)
```

Your results will look like:



<img src="https://mmlspark.blob.core.windows.net/graphics/explainers/image-shap-20210811.png"/>
