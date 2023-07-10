---
title: Vowpal Wabbit - Contextual Bandits
hide_title: true
status: stable
---
<img width="200" src="https://mmlspark.blob.core.windows.net/graphics/emails/vw-blue-dark-orange.svg" />

# Contextual-Bandits using Vowpal Wabbit

In the contextual bandit problem, a learner repeatedly observes a context, chooses an action, and observes a loss/cost/reward for the chosen action only. Contextual bandit algorithms use additional side information (or context) to aid real world decision-making. They work well for choosing actions in dynamic environments where options change rapidly, and the set of available actions is limited.

An in-depth tutorial can be found [here](https://vowpalwabbit.org/docs/vowpal_wabbit/python/latest/tutorials/python_Contextual_bandits_and_Vowpal_Wabbit.html)

[Azure Personalizer](https://azure.microsoft.com/en-us/products/cognitive-services/personalizer) emits logs in DSJSON-format. This example demonstrates how to perform off-policy evaluation.


## Step1: Read the dataset


```python
from pyspark.sql import SparkSession

# Bootstrap Spark Session
spark = SparkSession.builder.getOrCreate()

from synapse.ml.core.platform import *

from synapse.ml.core.platform import materializing_display as display
```


```python
import pyspark.sql.types as T
from pyspark.sql import functions as F

schema = T.StructType(
    [
        T.StructField("input", T.StringType(), False),
    ]
)

df = (
    spark.read.format("text")
    .schema(schema)
    .load("wasbs://publicwasb@mmlspark.blob.core.windows.net/decisionservice.json")
)
# print dataset basic info
print("records read: " + str(df.count()))
print("Schema: ")
df.printSchema()
```


```python
display(df)
```

## Step 2: Use VowpalWabbitFeaturizer to convert data features into vector


```python
from synapse.ml.vw import VowpalWabbitDSJsonTransformer

df_train = (
    VowpalWabbitDSJsonTransformer()
    .setDsJsonColumn("input")
    .transform(df)
    .withColumn("splitId", F.lit(0))
    .repartition(2)
)

# Show structured nature of rewards
df_train.printSchema()

# exclude JSON to avoid overflow
display(df_train.drop("input"))
```

## Step 3: Train model

VowpalWabbitGeneric performs these steps:

* trains a model for each split (=group)
* synchronizes accross partitions after every split
* store the 1-step ahead predictions in the model


```python
from synapse.ml.vw import VowpalWabbitGeneric

model = (
    VowpalWabbitGeneric()
    .setPassThroughArgs(
        "--cb_adf --cb_type mtr --clip_p 0.1 -q GT -q MS -q GR -q OT -q MT -q OS --dsjson --preserve_performance_counters"
    )
    .setInputCol("input")
    .setSplitCol("splitId")
    .setPredictionIdCol("EventId")
    .fit(df_train)
)
```

## Step 4: Predict and evaluate


```python
df_predictions = model.getOneStepAheadPredictions()  # .show(5, False)
df_headers = df_train.drop("input")

df_headers_predictions = df_headers.join(df_predictions, "EventId")
display(df_headers_predictions)
```


```python
from synapse.ml.vw import VowpalWabbitCSETransformer

metrics = VowpalWabbitCSETransformer().transform(df_headers_predictions)

display(metrics)
```

For each field of the reward column the metrics are calculated


```python
per_reward_metrics = metrics.select("reward.*")

display(per_reward_metrics)
```
