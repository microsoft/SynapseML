---
title: Quickstart - Analyze Text
hide_title: true
status: stable
---
# Cognitive Services - Analyze Text




```python
from synapse.ml.core.platform import find_secret

cognitive_key = find_secret("cognitive-api-key")
cognitive_location = "eastus"
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
    .setLocation(cognitive_location)
    .setSubscriptionKey(cognitive_key)
    .setTextCol("text")
    .setOutputCol("textAnalysis")
    .setErrorCol("error")
    .setLanguageCol("language")
    .setEntityRecognitionParams(
        {"model-version": "latest"}
    )  # Can pass parameters to each model individually
    .setIncludePii(False)  # Users can manually exclude tasks to speed up analysis
    .setIncludeEntityLinking(False)
    .setIncludeSentimentAnalysis(False)
)

df_results = text_analyze.transform(df)
```


```python
display(df_results)
```
