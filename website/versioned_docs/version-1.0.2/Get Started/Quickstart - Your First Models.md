---
title: Quickstart - Your First Models
hide_title: true
status: stable
---
# Build your first SynapseML models
This tutorial provides a brief introduction to SynapseML. In particular, we use SynapseML to create two different pipelines for sentiment analysis. The first pipeline combines a text featurization stage with LightGBM regression to predict ratings based on review text from a dataset containing book reviews from Amazon. The second pipeline shows how to use prebuilt models through the Azure AI Services to solve this problem without training data.

## Load a dataset
Load your dataset and split it into train and test sets.


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

## Create the training pipeline
Create a pipeline that featurizes data using `TextFeaturizer` from the `synapse.ml.featurize.text` library and derives a rating using the `LightGBMRegressor` function.


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

## Predict the output of the test data
Call the `transform` function on the model to predict and display the output of the test data as a dataframe.


```python
display(model.transform(test))
```

## Use Azure AI Services to transform data in one step
Alternatively, for these kinds of tasks that have a prebuilt solution, you can use SynapseML's integration with Azure AI Services to transform your data in one step.


```python
from synapse.ml.services.language import AnalyzeText
from synapse.ml.core.platform import find_secret

model = AnalyzeText(
    textCol="text",
    outputCol="sentiment",
    kind="SentimentAnalysis",
    subscriptionKey=find_secret(
        secret_name="ai-services-api-key", keyvault="mmlspark-build-keys"
    ),  # Replace the call to find_secret with your key as a python string.
).setLocation("eastus")

display(model.transform(test))
```
