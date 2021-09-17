---
title: SparkServing - Deploying a Classifier
hide_title: true
type: notebook
status: stable
categories: ["Spark Serving"]
---

## Model Deployment with Spark Serving 
In this example, we try to predict incomes from the *Adult Census* dataset. Then we will use Spark serving to deploy it as a realtime web service. 
First, we import needed packages:


```python
import sys
import numpy as np
import pandas as pd

```

Now let's read the data and split it to train and test sets:


```python
data = spark.read.parquet("wasbs://publicwasb@mmlspark.blob.core.windows.net/AdultCensusIncome.parquet")
data = data.select(["education", "marital-status", "hours-per-week", "income"])
train, test = data.randomSplit([0.75, 0.25], seed=123)
train.limit(10).toPandas()
```

`TrainClassifier` can be used to initialize and fit a model, it wraps SparkML classifiers.
You can use `help(mmlspark.TrainClassifier)` to view the different parameters.

Note that it implicitly converts the data into the format expected by the algorithm. More specifically it:
 tokenizes, hashes strings, one-hot encodes categorical variables, assembles the features into a vector
etc.  The parameter `numFeatures` controls the number of hashed features.


```python
from mmlspark.train import TrainClassifier
from pyspark.ml.classification import LogisticRegression
model = TrainClassifier(model=LogisticRegression(), labelCol="income", numFeatures=256).fit(train)
```

After the model is trained, we score it against the test dataset and view metrics.


```python
from mmlspark.train import ComputeModelStatistics, TrainedClassifierModel
prediction = model.transform(test)
prediction.printSchema()
```


```python
metrics = ComputeModelStatistics().transform(prediction)
metrics.limit(10).toPandas()
```

First, we will define the webservice input/output.
For more information, you can visit the [documentation for Spark Serving](https://github.com/Azure/mmlspark/blob/master/docs/mmlspark-serving.md)


```python
from pyspark.sql.types import *
from mmlspark.io import *
import uuid

serving_inputs = spark.readStream.server() \
    .address("localhost", 8898, "my_api") \
    .option("name", "my_api") \
    .load() \
    .parseRequest("my_api", test.schema)

serving_outputs = model.transform(serving_inputs) \
  .makeReply("scored_labels")

server = serving_outputs.writeStream \
    .server() \
    .replyTo("my_api") \
    .queryName("my_query") \
    .option("checkpointLocation", "file:///tmp/checkpoints-{}".format(uuid.uuid1())) \
    .start()

```

Test the webservice


```python
import requests
data = u'{"education":" 10th","marital-status":"Divorced","hours-per-week":40.0}'
r = requests.post(data=data, url="http://localhost:8898/my_api")
print("Response {}".format(r.text))
```


```python
import requests
data = u'{"education":" Masters","marital-status":"Married-civ-spouse","hours-per-week":40.0}'
r = requests.post(data=data, url="http://localhost:8898/my_api")
print("Response {}".format(r.text))
```


```python
import time
time.sleep(20) # wait for server to finish setting up (just to be safe)
server.stop()
```


```python

```
