---
title: CognitiveServices - Analyze Text
hide_title: true
status: stable
---
# Cognitive Services - Analyze Text




```python
import os

if os.environ.get("AZURE_SERVICE", None) == "Microsoft.ProjectArcadia":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    from notebookutils.mssparkutils.credentials import getSecret

    os.environ["TEXT_API_KEY"] = getSecret("mmlspark-build-keys", "cognitive-api-key")
    from notebookutils.visualization import display

# put your service keys here
key = os.environ["TEXT_API_KEY"]
location = "eastus"
```


```python
df = spark.createDataFrame(
    data=[
        ["en", "Hello Seattle"],
        ["en", "There once was a dog who lived in London and thought she was a human"],
    ],
    schema=["language", "text"],
)
```


```python
display(df)
```


```python
from synapse.ml.cognitive import *

text_analyze = (
    TextAnalyze()
    .setLocation(location)
    .setSubscriptionKey(key)
    .setTextCol("text")
    .setOutputCol("textAnalysis")
    .setErrorCol("error")
    .setLanguageCol("language")
    # set the tasks to perform
    .setEntityRecognitionTasks([{"parameters": {"model-version": "latest"}}])
    .setKeyPhraseExtractionTasks([{"parameters": {"model-version": "latest"}}])
    # Uncomment these lines to add more tasks
    # .setEntityRecognitionPiiTasks([{"parameters": { "model-version": "latest"}}])
    # .setEntityLinkingTasks([{"parameters": { "model-version": "latest"}}])
    # .setSentimentAnalysisTasks([{"parameters": { "model-version": "latest"}}])
)

df_results = text_analyze.transform(df)
```


```python
display(df_results)
```


```python
from pyspark.sql.functions import col

# reformat and display for easier viewing
display(
    df_results.select(
        "language", "text", "error", col("textAnalysis").getItem(0)
    ).select(  # we are not batching so only have a single result
        "language", "text", "error", "textAnalysis[0].*"
    )  # explode the Text Analytics tasks into columns
)
```
