---
title: Quickstart - Analyze Text
hide_title: true
status: stable
---
# Azure AI Services - Analyze Text


```python
from synapse.ml.core.platform import find_secret

ai_service_key = find_secret(
    secret_name="ai-services-api-key", keyvault="mmlspark-build-keys"
)
ai_service_location = "eastus"
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
from synapse.ml.services import *

text_analyze = (
    TextAnalyze()
    .setLocation(ai_service_location)
    .setSubscriptionKey(ai_service_key)
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
