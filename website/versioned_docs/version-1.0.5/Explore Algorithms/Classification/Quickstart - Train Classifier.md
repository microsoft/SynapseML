---
title: Quickstart - Train Classifier
hide_title: true
status: stable
---
## Classification - Adult Census

In this example, we try to predict incomes from the *Adult Census* dataset.

First, we import the packages (use `help(synapse)` to view contents),

Now let's read the data and split it to train and test sets:


```python
data = spark.read.parquet(
    "wasbs://publicwasb@mmlspark.blob.core.windows.net/AdultCensusIncome.parquet"
)
data = data.select(["education", "marital-status", "hours-per-week", "income"])
train, test = data.randomSplit([0.75, 0.25], seed=123)
train.limit(10).toPandas()
```

`TrainClassifier` can be used to initialize and fit a model, it wraps SparkML classifiers.
You can use `help(synapse.ml.train.TrainClassifier)` to view the different parameters.

Note that it implicitly converts the data into the format expected by the algorithm: tokenize
and hash strings, one-hot encodes categorical variables, assembles the features into a vector
and so on.  The parameter `numFeatures` controls the number of hashed features.


```python
from synapse.ml.train import TrainClassifier
from pyspark.ml.classification import LogisticRegression

model = TrainClassifier(
    model=LogisticRegression(), labelCol="income", numFeatures=256
).fit(train)
```

Finally, we save the model so it can be used in a scoring program.


```python
from synapse.ml.core.platform import *

if running_on_synapse() or running_on_synapse_internal():
    model.write().overwrite().save("Files/models/AdultCensus.mml")
elif running_on_databricks():
    model.write().overwrite().save("dbfs:/AdultCensus.mml")
elif running_on_binder():
    model.write().overwrite().save("/tmp/AdultCensus.mml")
else:
    print(f"{current_platform()} platform not supported")
```
