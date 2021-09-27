---
title: LightGBM - Overview
hide_title: true
status: stable
---
# LightGBM

[LightGBM](https://github.com/Microsoft/LightGBM) is an open-source,
distributed, high-performance gradient boosting (GBDT, GBRT, GBM, or
MART) framework. This framework specializes in creating high-quality and
GPU enabled decision tree algorithms for ranking, classification, and
many other machine learning tasks. LightGBM is part of Microsoft's
[DMTK](http://github.com/microsoft/dmtk) project.

### Advantages of LightGBM

-   **Composability**: LightGBM models can be incorporated into existing
    SparkML Pipelines, and used for batch, streaming, and serving
    workloads.
-   **Performance**: LightGBM on Spark is 10-30% faster than SparkML on
    the Higgs dataset, and achieves a 15% increase in AUC.  [Parallel
    experiments](https://github.com/Microsoft/LightGBM/blob/master/docs/Experiments.rst#parallel-experiment)
    have verified that LightGBM can achieve a linear speed-up by using
    multiple machines for training in specific settings.
-   **Functionality**: LightGBM offers a wide array of [tunable
    parameters](https://github.com/Microsoft/LightGBM/blob/master/docs/Parameters.rst),
    that one can use to customize their decision tree system. LightGBM on
    Spark also supports new types of problems such as quantile regression.
-   **Cross platform** LightGBM on Spark is available on Spark, PySpark, and SparklyR

### LightGBM Usage:

- LightGBMClassifier: used for building classification models. For example, to predict whether a company will bankrupt or not, we could build a binary classification model with LightGBMClassifier.
- LightGBMRegressor: used for building regression models. For example, to predict the house price, we could build a regression model with LightGBMRegressor.
- LightGBMRanker: used for building ranking models. For example, to predict website searching result relevance, we could build a ranking model with LightGBMRanker.

## Bankruptcy Prediction with LightGBM Classifier

<img src="https://mmlspark.blob.core.windows.net/graphics/Documentation/bankruptcy image.png" width="800" />

In this example, we use LightGBM to build a classification model in order to predict bankruptcy.

#### Read dataset


```python
import os



if os.environ.get("AZURE_SERVICE", None) == "Microsoft.ProjectArcadia":

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
```


```python
df = spark.read.format("csv")\

  .option("header", True)\

  .option("inferSchema", True)\

  .load("wasbs://publicwasb@mmlspark.blob.core.windows.net/company_bankruptcy_prediction_data.csv")

# print dataset size

print("records read: " + str(df.count()))

print("Schema: ")

df.printSchema()
```


```python
display(df)
```

#### Split the dataset into train and test


```python
train, test = df.randomSplit([0.85, 0.15], seed=1)
```

#### Add featurizer to convert features to vector


```python
from pyspark.ml.feature import VectorAssembler

feature_cols = df.columns[1:]

featurizer = VectorAssembler(

    inputCols=feature_cols,

    outputCol='features'

)

train_data = featurizer.transform(train)['Bankrupt?', 'features']

test_data = featurizer.transform(test)['Bankrupt?', 'features']
```

#### Check if the data is unbalanced


```python
display(train_data.groupBy("Bankrupt?").count())
```

#### Model Training


```python
from mmlspark.lightgbm import LightGBMClassifier

model = LightGBMClassifier(objective="binary", featuresCol="features", labelCol="Bankrupt?", isUnbalance=True)
```


```python
model = model.fit(train_data)
```

By calling "saveNativeModel", it allows you to extract the underlying lightGBM model for fast deployment after you train on Spark.


```python
from mmlspark.lightgbm import LightGBMClassificationModel



if os.environ.get("AZURE_SERVICE", None) == "Microsoft.ProjectArcadia":

    model.saveNativeModel("/models/lgbmclassifier.model")

    model = LightGBMClassificationModel.loadNativeModelFromFile("/models/lgbmclassifier.model")

else:

    model.saveNativeModel("/lgbmclassifier.model")

    model = LightGBMClassificationModel.loadNativeModelFromFile("/lgbmclassifier.model")


```

#### Feature Importances Visualization


```python
import pandas as pd

import matplotlib.pyplot as plt



feature_importances = model.getFeatureImportances()

fi = pd.Series(feature_importances,index = feature_cols)

fi = fi.sort_values(ascending = True)

f_index = fi.index

f_values = fi.values

 

# print feature importances 

print ('f_index:',f_index)

print ('f_values:',f_values)



# plot

x_index = list(range(len(fi)))

x_index = [x/len(fi) for x in x_index]

plt.rcParams['figure.figsize'] = (20,20)

plt.barh(x_index,f_values,height = 0.028 ,align="center",color = 'tan',tick_label=f_index)

plt.xlabel('importances')

plt.ylabel('features')

plt.show()
```

#### Model Prediction


```python
predictions = model.transform(test_data)
predictions.limit(10).toPandas()
```


```python
from mmlspark.train import ComputeModelStatistics
metrics = ComputeModelStatistics(evaluationMetric="classification", labelCol='Bankrupt?', scoredLabelsCol='prediction').transform(predictions)
display(metrics)
```

## Quantile Regression for Drug Discovery with LightGBMRegressor

<img src="https://mmlspark.blob.core.windows.net/graphics/Documentation/drug.png" width="800" />

In this example, we show how to use LightGBM to build a simple regression model.

#### Read dataset


```python
triazines = spark.read.format("libsvm")\
    .load("wasbs://publicwasb@mmlspark.blob.core.windows.net/triazines.scale.svmlight")
```


```python
# print some basic info
print("records read: " + str(triazines.count()))
print("Schema: ")
triazines.printSchema()
display(triazines.limit(10))
```

#### Split dataset into train and test


```python
train, test = triazines.randomSplit([0.85, 0.15], seed=1)
```

#### Model Training


```python
from mmlspark.lightgbm import LightGBMRegressor
model = LightGBMRegressor(objective='quantile',
                          alpha=0.2,
                          learningRate=0.3,
                          numLeaves=31).fit(train)
```


```python
print(model.getFeatureImportances())
```

#### Model Prediction


```python
scoredData = model.transform(test)
display(scoredData)
```


```python
from mmlspark.train import ComputeModelStatistics
metrics = ComputeModelStatistics(evaluationMetric='regression',
                                 labelCol='label',
                                 scoresCol='prediction') \
            .transform(scoredData)
display(metrics)
```

## LightGBM Ranker

#### Read dataset


```python
df = spark.read.format("parquet").load("wasbs://publicwasb@mmlspark.blob.core.windows.net/lightGBMRanker_train.parquet")
# print some basic info
print("records read: " + str(df.count()))
print("Schema: ")
df.printSchema()
display(df.limit(10))
```

#### Model Training


```python
from mmlspark.lightgbm import LightGBMRanker

features_col = 'features'
query_col = 'query'
label_col = 'labels'
lgbm_ranker = LightGBMRanker(labelCol=label_col,
                             featuresCol=features_col,
                             groupCol=query_col,
                             predictionCol='preds',
                             leafPredictionCol='leafPreds',
                             featuresShapCol='importances',
                             repartitionByGroupingColumn=True,
                             numLeaves=32,
                             numIterations=200,
                             evalAt=[1,3,5],
                             metric='ndcg')
```


```python
lgbm_ranker_model = lgbm_ranker.fit(df)
```

#### Model Prediction


```python
dt = spark.read.format("parquet").load("wasbs://publicwasb@mmlspark.blob.core.windows.net/lightGBMRanker_test.parquet")
predictions = lgbm_ranker_model.transform(dt)
predictions.limit(10).toPandas()
```
