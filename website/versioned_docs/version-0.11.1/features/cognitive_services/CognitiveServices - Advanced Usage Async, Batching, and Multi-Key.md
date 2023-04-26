---
title: CognitiveServices - Advanced Usage Async, Batching, and Multi-Key
hide_title: true
status: stable
---
# Cognitive Services Advanced Guide: Asynchrony, Batching, Multi-Key

## Step 1: Imports and Keys


```
import os
from pyspark.sql import SparkSession
from synapse.ml.core.platform import running_on_synapse, find_secret

# Bootstrap Spark Session
spark = SparkSession.builder.getOrCreate()
if running_on_synapse():
    from notebookutils.visualization import display

service_key = find_secret("cognitive-api-key")
service_loc = "eastus"
```

## Step 2: Basic Usage

Image 1             |  Image 2          |  Image 3      
:-------------------------:|:-------------------------:|:----------------------:|
!<img src="https://raw.githubusercontent.com/Azure-Samples/cognitive-services-sample-data-files/master/ComputerVision/Images/objects.jpg" width="300" />  |  <img src="https://raw.githubusercontent.com/Azure-Samples/cognitive-services-sample-data-files/master/ComputerVision/Images/dog.jpg" width="300" /> |  <img src="https://raw.githubusercontent.com/Azure-Samples/cognitive-services-sample-data-files/master/ComputerVision/Images/house.jpg" width="300" />


```
from synapse.ml.cognitive.vision import AnalyzeImage

# Create a dataframe with the image URLs
base_url = "https://raw.githubusercontent.com/Azure-Samples/cognitive-services-sample-data-files/master/ComputerVision/Images/"
image_df = spark.createDataFrame(
    [(base_url + "objects.jpg",), (base_url + "dog.jpg",), (base_url + "house.jpg",)],
    ["image"],
)

# Run the Computer Vision service. Analyze Image extracts infortmation from/about the images.
analyzer = (
    AnalyzeImage()
    .setLocation(service_loc)
    .setSubscriptionKey(service_key)
    .setVisualFeatures(
        ["Categories", "Color", "Description", "Faces", "Objects", "Tags"]
    )
    .setOutputCol("analysis_results")
    .setImageUrlCol("image")
    .setErrorCol("error")
)

image_results = analyzer.transform(image_df).cache()
```

#### First we'll look at the full response objects:


```
display(image_results)
```

#### We can select out just what we need:


```
display(image_results.select("analysis_results.description.captions.text"))
```

#### What's going on under the hood

<img src="https://mmlspark.blob.core.windows.net/graphics/Cog%20Service%20NB/cog_service.svg" width="300" />

When we call the cognitive service transformer, we start cognitive service clients on each of your spark workers.
These clients send requests to the cloud, and turn the JSON responses into Spark Struct Types so that you can access any field that the service returns.

## Step 3: Asynchronous Usage

<img src="https://mmlspark.blob.core.windows.net/graphics/Cog%20Service%20NB/async_parallelism.svg" width="700"/>

Apache Spark ordinarily parallelizes a computation to all of it's worker threads. When working with services however this parallelism doesent fully maximize throughput because workers sit idle as requests are processed on the server. The `concurrency` parameter makes sure that each worker can stay busy as they wait for requests to complete.


```
display(analyzer.setConcurrency(3).transform(image_df))
```

#### Faster without extra hardware:
<img src="https://mmlspark.blob.core.windows.net/graphics/Cog%20Service%20NB/async_relative%20(2).png" width="500" />

## Step 4: Batching


```
from synapse.ml.cognitive.text import TextSentiment

# Create a dataframe
text_df = spark.createDataFrame(
    [
        ("I am so happy today, its sunny!",),
        ("I am frustrated by this rush hour traffic",),
        ("The cognitive services on spark is pretty lit",),
    ],
    ["text"],
)

sentiment = (
    TextSentiment()
    .setTextCol("text")
    .setLocation(service_loc)
    .setSubscriptionKey(service_key)
    .setOutputCol("sentiment")
    .setErrorCol("error")
    .setBatchSize(10)
)

# Show the results of your text query
display(sentiment.transform(text_df).select("text", "sentiment.document.sentiment"))
```

## Step 5: Multi-Key


```
from synapse.ml.cognitive.text import TextSentiment
from pyspark.sql.functions import udf
import random

service_key_2 = find_secret("cognitive-api-key-2")
keys = [service_key, service_key_2]


@udf
def random_key():
    return keys[random.randint(0, len(keys) - 1)]


image_df2 = image_df.withColumn("key", random_key())

results = analyzer.setSubscriptionKeyCol("key").transform(image_df2)
```


```
display(results.select("key", "analysis_results.description.captions.text"))
```

## Learn More
- [Explore other cogntive services](https://microsoft.github.io/SynapseML/docs/features/cognitive_services/CognitiveServices%20-%20Overview/)
- [Read our paper "Large-Scale Intelligent Microservices"](https://arxiv.org/abs/2009.08044)
