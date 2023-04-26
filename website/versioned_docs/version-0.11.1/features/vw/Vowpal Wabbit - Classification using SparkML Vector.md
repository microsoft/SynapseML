---
title: Vowpal Wabbit - Classification using SparkML Vector
hide_title: true
status: stable
---
<img width="200" src="https://mmlspark.blob.core.windows.net/graphics/emails/vw-blue-dark-orange.svg" />

# Binary Classification with VowalWabbit on Criteo Dataset 


## SparkML Vector input

#### Read dataset


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
        T.StructField("label", T.IntegerType(), True),
        *[T.StructField("i" + str(i), T.IntegerType(), True) for i in range(1, 13)],
        *[T.StructField("s" + str(i), T.StringType(), True) for i in range(26)],
    ]
)

df = (
    spark.read.format("csv")
    .option("header", False)
    .option("delimiter", "\t")
    .schema(schema)
    .load("wasbs://publicwasb@mmlspark.blob.core.windows.net/criteo_day0_1k.csv.gz")
)
# print dataset basic info
print("records read: " + str(df.count()))
print("Schema: ")
df.printSchema()
```


```python
display(df)
```

#### Use VowalWabbitFeaturizer to convert data features into vector


```python
from synapse.ml.vw import VowpalWabbitFeaturizer

featurizer = VowpalWabbitFeaturizer(
    inputCols=[
        *["i" + str(i) for i in range(1, 13)],
        *["s" + str(i) for i in range(26)],
    ],
    outputCol="features",
)

df = featurizer.transform(df).select("label", "features")
```

#### Split the dataset into train and test


```python
train, test = df.randomSplit([0.85, 0.15], seed=1)
```

#### Model Training


```python
from synapse.ml.vw import VowpalWabbitClassifier

model = VowpalWabbitClassifier(
    numPasses=20,
    labelCol="label",
    featuresCol="features",
    passThroughArgs="--holdout_off --loss_function logistic",
).fit(train)
```

#### Model Prediction


```python
predictions = model.transform(test)
display(predictions)
```


```python
from synapse.ml.train import ComputeModelStatistics

metrics = ComputeModelStatistics(
    evaluationMetric="classification", labelCol="label", scoredLabelsCol="prediction"
).transform(predictions)
display(metrics)
```
