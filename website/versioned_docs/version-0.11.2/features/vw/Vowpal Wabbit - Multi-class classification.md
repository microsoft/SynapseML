---
title: Vowpal Wabbit - Multi-class classification
hide_title: true
status: stable
---
<img width="200" src="https://mmlspark.blob.core.windows.net/graphics/emails/vw-blue-dark-orange.svg" />

# Multi-class Classification using Vowpal Wabbit


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
        T.StructField("sepal_length", T.DoubleType(), False),
        T.StructField("sepal_width", T.DoubleType(), False),
        T.StructField("petal_length", T.DoubleType(), False),
        T.StructField("petal_width", T.DoubleType(), False),
        T.StructField("variety", T.StringType(), False),
    ]
)

df = (
    spark.read.format("csv")
    .option("header", True)
    .schema(schema)
    .load("wasbs://publicwasb@mmlspark.blob.core.windows.net/iris.txt")
)
# print dataset basic info
print("records read: " + str(df.count()))
print("Schema: ")
df.printSchema()
```


```python
display(df)
```

#### Use VowpalWabbitFeaturizer to convert data features into vector


```python
from pyspark.ml.feature import StringIndexer

from synapse.ml.vw import VowpalWabbitFeaturizer

indexer = StringIndexer(inputCol="variety", outputCol="label")
featurizer = VowpalWabbitFeaturizer(
    inputCols=["sepal_length", "sepal_width", "petal_length", "petal_width"],
    outputCol="features",
)

# label needs to be integer (0 to n)
df_label = indexer.fit(df).transform(df).withColumn("label", F.col("label").cast("int"))

# featurize data
df_featurized = featurizer.transform(df_label).select("label", "features")

display(df_featurized)
```

#### Split the dataset into train and test


```python
train, test = df_featurized.randomSplit([0.8, 0.2], seed=1)
```

#### Model Training


```python
from synapse.ml.vw import VowpalWabbitClassifier


model = (
    VowpalWabbitClassifier(
        numPasses=5,
        passThroughArgs="--holdout_off --oaa 3 --holdout_off --loss_function=logistic --indexing 0 -q ::",
    )
    .setNumClasses(3)
    .fit(train)
)
```

#### Model Prediction


```python
predictions = model.transform(test)

display(predictions)
```
