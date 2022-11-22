---
title: Classification - Sentiment Analysis Quickstart
hide_title: true
status: stable
---
# A 5 minute tour of SynapseML


```python
from pyspark.sql import SparkSession
from synapse.ml.core.platform import *

spark = SparkSession.builder.getOrCreate()

from synapse.ml.core.platform import materializing_display as display
```

# Step 1: Load our Dataset


```python
train, test = (
    spark.read.parquet(
        "wasbs://publicwasb@mmlspark.blob.core.windows.net/BookReviewsFromAmazon10K.parquet"
    )
    .limit(1000)
    .cache()
    .randomSplit([0.8, 0.2])
)

display(train)
```


    StatementMeta(, , , Cancelled, )


# Step 2: Make our Model


```python
from pyspark.ml import Pipeline
from synapse.ml.featurize.text import TextFeaturizer
from synapse.ml.lightgbm import LightGBMRegressor

model = Pipeline(
    stages=[
        TextFeaturizer(inputCol="text", outputCol="features"),
        LightGBMRegressor(featuresCol="features", labelCol="rating"),
    ]
).fit(train)
```


    StatementMeta(, , , Cancelled, )


# Step 3: Predict!


```python
display(model.transform(test))
```


    StatementMeta(, , , Cancelled, )


# Alternate route: Let the Cognitive Services handle it


```python
from synapse.ml.cognitive import TextSentiment
from synapse.ml.core.platform import find_secret

model = TextSentiment(
    textCol="text",
    outputCol="sentiment",
    subscriptionKey=find_secret("cognitive-api-key"),
).setLocation("eastus")

display(model.transform(test))
```


    StatementMeta(, , , Cancelled, )

