---
title: First Example
description: Build machine learning applications using Microsoft Machine Learning for Apache Spark
---

## Prerequisites

- If you don't have an Azure subscription, [create a free account before you begin](https://azure.microsoft.com/free/).
- [Azure Synapse Analytics workspace](https://docs.microsoft.com/en-us/azure/synapse-analytics/get-started-create-workspace) with an Azure Data Lake Storage Gen2 storage account configured as the default storage. You need to be the _Storage Blob Data Contributor_ of the Data Lake Storage Gen2 file system that you work with.
- Spark pool in your Azure Synapse Analytics workspace. For details, see [Create a Spark pool in Azure Synapse](https://docs.microsoft.com/en-us/azure/synapse-analytics/get-started-analyze-spark).
- Pre-configuration steps described in the tutorial [Configure Cognitive Services in Azure Synapse](https://docs.microsoft.com/en-us/azure/synapse-analytics/machine-learning/tutorial-configure-cognitive-services-synapse).

## Get started

To get started, import synapse.ml and configurate service keys.

```python
import synapse.ml
from synapse.ml.cognitive import *
from notebookutils import mssparkutils

# A general Cognitive Services key for Text Analytics and Computer Vision (or use separate keys that belong to each service)
cognitive_service_key = mssparkutils.credentials.getSecret("ADD_YOUR_KEY_VAULT_NAME", "ADD_YOUR_SERVICE_KEY","ADD_YOUR_KEY_VAULT_LINKED_SERVICE_NAME")
# A Bing Search v7 subscription key
bingsearch_service_key = mssparkutils.credentials.getSecret("ADD_YOUR_KEY_VAULT_NAME", "ADD_YOUR_BING_SEARCH_KEY","ADD_YOUR_KEY_VAULT_LINKED_SERVICE_NAME")
# An Anomaly Dectector subscription key
anomalydetector_key = mssparkutils.credentials.getSecret("ADD_YOUR_KEY_VAULT_NAME", "ADD_YOUR_ANOMALY_KEY","ADD_YOUR_KEY_VAULT_LINKED_SERVICE_NAME")


```

## Text analytics sample

The [Text Analytics](https://azure.microsoft.com/en-us/services/cognitive-services/text-analytics/) service provides several algorithms for extracting intelligent insights from text. For example, we can find the sentiment of given input text. The service will return a score between 0.0 and 1.0 where low scores indicate negative sentiment and high score indicates positive sentiment. This sample uses three simple sentences and returns the sentiment for each.

```python
from pyspark.sql.functions import col

# Create a dataframe that's tied to it's column names
df_sentences = spark.createDataFrame([
  ("I am so happy today, its sunny!", "en-US"),
  ("this is a dog", "en-US"),s
  ("I am frustrated by this rush hour traffic!", "en-US")
], ["text", "language"])

# Run the Text Analytics service with options
sentiment = (TextSentiment()
    .setTextCol("text")
    .setLocation("eastasia") # Set the location of your cognitive service
    .setSubscriptionKey(cognitive_service_key)
    .setOutputCol("sentiment")
    .setErrorCol("error")
    .setLanguageCol("language"))

# Show the results of your text query in a table format

display(sentiment.transform(df_sentences).select("text", col("sentiment")[0].getItem("sentiment").alias("sentiment")))
```

### Expected results

| text                                       | sentiment |
| ------------------------------------------ | --------- |
| I am frustrated by this rush hour traffic! | negative  |
| this is a dog                              | neutral   |
| I am so happy today, its sunny!            | positive  |
