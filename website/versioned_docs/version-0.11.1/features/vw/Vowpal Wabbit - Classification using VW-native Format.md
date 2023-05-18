---
title: Vowpal Wabbit - Classification using VW-native Format
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

#### Reformat into VW-native format
See VW [docs](https://github.com/VowpalWabbit/vowpal_wabbit/wiki/Input-format) for format details


```python
# create VW string format
cols = [
    F.col("label"),
    F.lit("|"),
    *[F.col("i" + str(i)) for i in range(1, 13)],
    *[F.col("s" + str(i)) for i in range(26)],
]

df = df.select(F.concat_ws(" ", *cols).alias("value"))
display(df)
```

#### Split the dataset into train and test


```python
train, test = df.randomSplit([0.6, 0.4], seed=1)
```

#### Model Training


```python
from synapse.ml.vw import VowpalWabbitGeneric

# number of partitions determines data parallelism
train = train.repartition(2)

model = VowpalWabbitGeneric(
    numPasses=5,
    passThroughArgs="--holdout_off --loss_function logistic --link logistic",
).fit(train)
```

#### Model Prediction


```python
predictions = model.transform(test)

predictions = predictions.withColumn(
    "prediction", F.col("prediction").cast("double")
).withColumn("label", F.substring("value", 0, 1).cast("double"))

display(predictions)
```


```python
from synapse.ml.train import ComputeModelStatistics

metrics = ComputeModelStatistics(
    evaluationMetric="classification", labelCol="label", scoredLabelsCol="prediction"
).transform(predictions)
display(metrics)
```
