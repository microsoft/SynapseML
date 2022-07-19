---
title: CognitiveServices - Create a Multilingual Search Engine from Forms
hide_title: true
status: stable
---
```python
import os

if os.environ.get("AZURE_SERVICE", None) == "Microsoft.ProjectArcadia":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    from notebookutils.mssparkutils.credentials import getSecret

    os.environ["VISION_API_KEY"] = getSecret("mmlspark-build-keys", "cognitive-api-key")
    os.environ["AZURE_SEARCH_KEY"] = getSecret(
        "mmlspark-build-keys", "azure-search-key"
    )
    os.environ["TRANSLATOR_KEY"] = getSecret("mmlspark-build-keys", "translator-key")
    from notebookutils.visualization import display


key = os.environ["VISION_API_KEY"]
search_key = os.environ["AZURE_SEARCH_KEY"]
translator_key = os.environ["TRANSLATOR_KEY"]

search_service = "mmlspark-azure-search"
search_index = "form-demo-index"
```


```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def blob_to_url(blob):
    [prefix, postfix] = blob.split("@")
    container = prefix.split("/")[-1]
    split_postfix = postfix.split("/")
    account = split_postfix[0]
    filepath = "/".join(split_postfix[1:])
    return "https://{}/{}/{}".format(account, container, filepath)


df2 = (
    spark.read.format("binaryFile")
    .load("wasbs://ignite2021@mmlsparkdemo.blob.core.windows.net/form_subset/*")
    .select("path")
    .limit(10)
    .select(udf(blob_to_url, StringType())("path").alias("url"))
    .cache()
)
```


```python
display(df2)
```

<embed src="https://mmlsparkdemo.blob.core.windows.net/ignite2021/form_svgs/Invoice11205.svg" width="40%"/>


```python
from synapse.ml.cognitive import AnalyzeInvoices

analyzed_df = (
    AnalyzeInvoices()
    .setSubscriptionKey(key)
    .setLocation("eastus")
    .setImageUrlCol("url")
    .setOutputCol("invoices")
    .setErrorCol("errors")
    .setConcurrency(5)
    .transform(df2)
    .cache()
)
```


```python
display(analyzed_df)
```


```python
from synapse.ml.cognitive import FormOntologyLearner

organized_df = (
    FormOntologyLearner()
    .setInputCol("invoices")
    .setOutputCol("extracted")
    .fit(analyzed_df)
    .transform(analyzed_df)
    .select("url", "extracted.*")
    .cache()
)
```


```python
display(organized_df)
```


```python
from pyspark.sql.functions import explode, col

itemized_df = (
    organized_df.select("*", explode(col("Items")).alias("Item"))
    .drop("Items")
    .select("Item.*", "*")
    .drop("Item")
)
```


```python
display(itemized_df)
```


```python
display(itemized_df.where(col("ProductCode") == 48))
```


```python
from synapse.ml.cognitive import Translate

translated_df = (
    Translate()
    .setSubscriptionKey(translator_key)
    .setLocation("eastus")
    .setTextCol("Description")
    .setErrorCol("TranslationError")
    .setOutputCol("output")
    .setToLanguage(["zh-Hans", "fr", "ru", "cy"])
    .setConcurrency(5)
    .transform(itemized_df)
    .withColumn("Translations", col("output.translations")[0])
    .drop("output", "TranslationError")
    .cache()
)
```


```python
display(translated_df)
```


```python
from synapse.ml.cognitive import *
from pyspark.sql.functions import monotonically_increasing_id, lit

(
    translated_df.withColumn("DocID", monotonically_increasing_id().cast("string"))
    .withColumn("SearchAction", lit("upload"))
    .writeToAzureSearch(
        subscriptionKey=search_key,
        actionCol="SearchAction",
        serviceName=search_service,
        indexName=search_index,
        keyCol="DocID",
    )
)
```


```python
import requests

url = "https://{}.search.windows.net/indexes/{}/docs/search?api-version=2019-05-06".format(
    search_service, search_index
)
requests.post(url, json={"search": "door"}, headers={"api-key": search_key}).json()
```


```python

```
