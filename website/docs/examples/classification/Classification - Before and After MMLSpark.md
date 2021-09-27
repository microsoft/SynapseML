---
title: Classification - Before and After MMLSpark
hide_title: true
status: stable
---
## Classification - Before and After MMLSpark

### 1. Introduction

<p><img src="https://images-na.ssl-images-amazon.com/images/G/01/img16/books/bookstore/landing-page/1000638_books_landing-page_bookstore-photo-01.jpg"  title="Image from https://images-na.ssl-images-amazon.com/images/G/01/img16/books/bookstore/landing-page/1000638_books_landing-page_bookstore-photo-01.jpg" /><br /></p>

In this tutorial, we perform the same classification task in two
different ways: once using plain **`pyspark`** and once using the
**`mmlspark`** library.  The two methods yield the same performance,
but one of the two libraries is drastically simpler to use and iterate
on (can you guess which one?).

The task is simple: Predict whether a user's review of a book sold on
Amazon is good (rating > 3) or bad based on the text of the review.  We
accomplish this by training LogisticRegression learners with different
hyperparameters and choosing the best model.


```python
import os
if os.environ.get("AZURE_SERVICE", None) == "Microsoft.ProjectArcadia":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
```

### 2. Read the data

We download and read in the data. We show a sample below:


```python
rawData = spark.read.parquet("wasbs://publicwasb@mmlspark.blob.core.windows.net/BookReviewsFromAmazon10K.parquet")
rawData.show(5)
```

### 3. Extract more features and process data

Real data however is more complex than the above dataset. It is common
for a dataset to have features of multiple types: text, numeric,
categorical.  To illustrate how difficult it is to work with these
datasets, we add two numerical features to the dataset: the **word
count** of the review and the **mean word length**.


```python
from pyspark.sql.functions import udf
from pyspark.sql.types import *
def wordCount(s):
    return len(s.split())
def wordLength(s):
    import numpy as np
    ss = [len(w) for w in s.split()]
    return round(float(np.mean(ss)), 2)
wordLengthUDF = udf(wordLength, DoubleType())
wordCountUDF = udf(wordCount, IntegerType())
```


```python
from mmlspark.stages import UDFTransformer
wordLength = "wordLength"
wordCount = "wordCount"
wordLengthTransformer = UDFTransformer(inputCol="text", outputCol=wordLength, udf=wordLengthUDF)
wordCountTransformer = UDFTransformer(inputCol="text", outputCol=wordCount, udf=wordCountUDF)

```


```python
from pyspark.ml import Pipeline
data = Pipeline(stages=[wordLengthTransformer, wordCountTransformer]) \
       .fit(rawData).transform(rawData) \
       .withColumn("label", rawData["rating"] > 3).drop("rating")
```


```python
data.show(5)
```

### 4a. Classify using pyspark

To choose the best LogisticRegression classifier using the `pyspark`
library, need to *explictly* perform the following steps:

1. Process the features:
   * Tokenize the text column
   * Hash the tokenized column into a vector using hashing
   * Merge the numeric features with the vector in the step above
2. Process the label column: cast it into the proper type.
3. Train multiple LogisticRegression algorithms on the `train` dataset
   with different hyperparameters
4. Compute the area under the ROC curve for each of the trained models
   and select the model with the highest metric as computed on the
   `test` dataset
5. Evaluate the best model on the `validation` set

As you can see below, there is a lot of work involved and a lot of
steps where something can go wrong!


```python
from pyspark.ml.feature import Tokenizer, HashingTF
from pyspark.ml.feature import VectorAssembler

# Featurize text column
tokenizer = Tokenizer(inputCol="text", outputCol="tokenizedText")
numFeatures = 10000
hashingScheme = HashingTF(inputCol="tokenizedText",
                          outputCol="TextFeatures",
                          numFeatures=numFeatures)
tokenizedData = tokenizer.transform(data)
featurizedData = hashingScheme.transform(tokenizedData)

# Merge text and numeric features in one feature column
featureColumnsArray = ["TextFeatures", "wordCount", "wordLength"]
assembler = VectorAssembler(
    inputCols = featureColumnsArray,
    outputCol="features")
assembledData = assembler.transform(featurizedData)

# Select only columns of interest
# Convert rating column from boolean to int
processedData = assembledData \
                .select("label", "features") \
                .withColumn("label", assembledData.label.cast(IntegerType()))
```


```python
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import LogisticRegression

# Prepare data for learning
train, test, validation = processedData.randomSplit([0.60, 0.20, 0.20], seed=123)

# Train the models on the 'train' data
lrHyperParams = [0.05, 0.1, 0.2, 0.4]
logisticRegressions = [LogisticRegression(regParam = hyperParam)
                       for hyperParam in lrHyperParams]
evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction",
                                          metricName="areaUnderROC")
metrics = []
models = []

# Select the best model
for learner in logisticRegressions:
    model = learner.fit(train)
    models.append(model)
    scoredData = model.transform(test)
    metrics.append(evaluator.evaluate(scoredData))
bestMetric = max(metrics)
bestModel = models[metrics.index(bestMetric)]

# Get AUC on the validation dataset
scoredVal = bestModel.transform(validation)
print(evaluator.evaluate(scoredVal))
```

### 4b. Classify using mmlspark

Life is a lot simpler when using `mmlspark`!

1. The **`TrainClassifier`** Estimator featurizes the data internally,
   as long as the columns selected in the `train`, `test`, `validation`
   dataset represent the features

2. The **`FindBestModel`** Estimator find the best model from a pool of
   trained models by find the model which performs best on the `test`
   dataset given the specified metric

3. The **`CompueModelStatistics`** Transformer computes the different
   metrics on a scored dataset (in our case, the `validation` dataset)
   at the same time


```python
from mmlspark.train import TrainClassifier, ComputeModelStatistics
from mmlspark.automl import FindBestModel

# Prepare data for learning
train, test, validation = data.randomSplit([0.60, 0.20, 0.20], seed=123)

# Train the models on the 'train' data
lrHyperParams = [0.05, 0.1, 0.2, 0.4]
logisticRegressions = [LogisticRegression(regParam = hyperParam)
                       for hyperParam in lrHyperParams]
lrmodels = [TrainClassifier(model=lrm, labelCol="label", numFeatures=10000).fit(train)
            for lrm in logisticRegressions]

# Select the best model
bestModel = FindBestModel(evaluationMetric="AUC", models=lrmodels).fit(test)


# Get AUC on the validation dataset
predictions = bestModel.transform(validation)
metrics = ComputeModelStatistics().transform(predictions)
print("Best model's AUC on validation set = "
      + "{0:.2f}%".format(metrics.first()["AUC"] * 100))
```
