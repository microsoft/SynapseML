---
title: Vowpal Wabbit - Overview
hide_title: true
status: stable
---
<img width="200" src="https://mmlspark.blob.core.windows.net/graphics/emails/vw-blue-dark-orange.svg" />

# VowpalWabbit 

[VowpalWabbit](https://github.com/VowpalWabbit/vowpal_wabbit) (VW) is a machine learning system which
pushes the frontier of machine learning with techniques such as online, hashing, allreduce,
reductions, learning2search, active, and interactive learning. 
VowpalWabbit is a popular choice in ad-tech due to it's speed and cost efficacy. 
Furthermore it includes many advances in the area of reinforcement learning (e.g. contextual bandits). 


### Advantages of VowpalWabbit

-  **Composability**: VowpalWabbit models can be incorporated into existing
    SparkML Pipelines, and used for batch, streaming, and serving workloads.
-  **Small footprint**: VowpalWabbit memory consumption is rather small and can be controlled through '-b 18' or setNumBits method.   
    This determines the size of the model (e.g. 2^18 * some_constant).
-  **Feature Interactions**: Feature interactions (e.g. quadratic, cubic,... terms) are created on-the-fly within the most inner
    learning loop in VW.
    Interactions can be specified by using the -q parameter and passing the first character of the namespaces that should be _interacted_. 
    The VW namespace concept is mapped to Spark using columns. The column name is used as namespace name, thus one sparse or dense Spark ML vector corresponds to the features of a single namespace. 
    To allow passing of multiple namespaces the VW estimator (classifier or regression) expose an additional property called _additionalFeatures_. Users can pass an array of column names.
-  **Simple deployment**: all native dependencies are packaged into a single jars (including boost and zlib).
-  **VowpalWabbit command line arguments**: users can pass VW command line arguments to control the learning process.
-  **VowpalWabbit binary models** Users can supply an initial VowpalWabbit model to start the training which can be produced outside of 
    VW on Spark by invoking _setInitialModel_ and pass the model as a byte array. Similarly users can access the binary model by invoking
    _getModel_ on the trained model object.
-  **Java-based hashing** VWs version of murmur-hash was re-implemented in Java (praise to [JackDoe](https://github.com/jackdoe)) 
    providing a major performance improvement compared to passing input strings through JNI and hashing in C++.
-  **Cross language** VowpalWabbit on Spark is available on Spark, PySpark, and SparklyR.

## Why use VowpalWabbit on Spark?

1. Large-scale distributed learning
1. Composability with Spark eco-system (SparkML and data processing)

## Operation modes

VW Spark-bindings cater to both SparkML and VW users by supporting different input and output format.

| Class                          | Input            | Output                  | ML Type     |
|--------------------------------|------------------|-------------------------|-------------|
| VowpalWabbitClassifier         | SparkML Vector   | Model                   | Multi-class |
| VowpalWabbitRegressor          | SparkML Vector   | Model                   | Regression  |
| VowpalWabbitGeneric            | VW-native format | Model                   | All         |
| VowpalWabbitGenericProgressive | VW-native format | 1-step ahead prediction | All         |

SparkML vectors can be created by standard Spark tools or using the VowpalWabbitFeaturizer.
[VWs native input format](https://github.com/VowpalWabbit/vowpal_wabbit/wiki/Input-format) supports a wide variety of ML tasks: [classification](https://vowpalwabbit.org/docs/vowpal_wabbit/python/latest/tutorials/python_classification.html), [regression](https://vowpalwabbit.org/docs/vowpal_wabbit/python/latest/examples/poisson_regression.html), [cost-sensitive classification](https://towardsdatascience.com/multi-label-classification-using-vowpal-wabbit-from-why-to-how-c1451ca0ded5), [contextual bandits](https://vowpalwabbit.org/docs/vowpal_wabbit/python/latest/tutorials/python_Contextual_bandits_and_Vowpal_Wabbit.html), ... 


### Limitations of VowpalWabbit on Spark

-  **Linux and CentOS only** The native binaries included with the published jar are built Linux and CentOS only.
    We're working on creating a more portable version by statically linking Boost and lib C++.

### VowpalWabbit Usage:

-  VowpalWabbitClassifier: used to build classification models.
-  VowpalWabbitRegressor: used to build regression models.
-  VowpalWabbitFeaturizer: used for feature hashing and extraction. For details please visit [here](https://github.com/VowpalWabbit/vowpal_wabbit/wiki/Feature-Hashing-and-Extraction).
-  VowpalWabbitContextualBandit: used to solve contextual bandits problems. For algorithm details please visit [here](https://github.com/VowpalWabbit/vowpal_wabbit/wiki/Contextual-Bandit-algorithms).

## Heart Disease Detection with VowpalWabbit Classifier

<img src="https://mmlspark.blob.core.windows.net/graphics/Documentation/heart disease.png" width="800" />

#### Read dataset


```python
from pyspark.sql import SparkSession

# Bootstrap Spark Session
spark = SparkSession.builder.getOrCreate()

from synapse.ml.core.platform import *

if running_on_synapse():
    from synapse.ml.core.platform import materializing_display as display
```


```python
df = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(
        "wasbs://publicwasb@mmlspark.blob.core.windows.net/heart_disease_prediction_data.csv"
    )
)
# print dataset basic info
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

#### Use VowpalWabbitFeaturizer to convert data features into vector


```python
from synapse.ml.vw import VowpalWabbitFeaturizer

featurizer = VowpalWabbitFeaturizer(inputCols=df.columns[:-1], outputCol="features")
train_data = featurizer.transform(train)["target", "features"]
test_data = featurizer.transform(test)["target", "features"]
```


```python
display(train_data.groupBy("target").count())
```

#### Model Training


```python
from synapse.ml.vw import VowpalWabbitClassifier

model = VowpalWabbitClassifier(
    numPasses=20, labelCol="target", featuresCol="features"
).fit(train_data)
```

#### Model Prediction


```python
predictions = model.transform(test_data)
display(predictions)
```


```python
from synapse.ml.train import ComputeModelStatistics

metrics = ComputeModelStatistics(
    evaluationMetric="classification", labelCol="target", scoredLabelsCol="prediction"
).transform(predictions)
display(metrics)
```

## Adult Census with VowpalWabbitClassifier

In this example, we predict incomes from the Adult Census dataset using Vowpal Wabbit (VW) Classifier in SynapseML.

#### Read dataset and split them into train & test


```python
data = spark.read.parquet(
    "wasbs://publicwasb@mmlspark.blob.core.windows.net/AdultCensusIncome.parquet"
)
data = data.select(["education", "marital-status", "hours-per-week", "income"])
train, test = data.randomSplit([0.75, 0.25], seed=123)
display(train)
```

#### Model Training

We define a pipeline that includes feature engineering and training of a VW classifier. We use a featurizer provided by VW that hashes the feature names. Note that VW expects classification labels being -1 or 1. Thus, the income category is mapped to this space before feeding training data into the pipeline.

Note: VW supports distributed learning, and it's controlled by number of partitions of dataset.


```python
from pyspark.sql.functions import when, col
from pyspark.ml import Pipeline
from synapse.ml.vw import VowpalWabbitFeaturizer, VowpalWabbitClassifier

# Define classification label
train = train.withColumn(
    "label", when(col("income").contains("<"), 0.0).otherwise(1.0)
).repartition(1)
print(train.count())

# Specify featurizer
vw_featurizer = VowpalWabbitFeaturizer(
    inputCols=["education", "marital-status", "hours-per-week"], outputCol="features"
)
```

Note: "passThroughArgs" parameter lets you pass in any params not exposed through our API. Full command line argument docs can be found [here](https://github.com/VowpalWabbit/vowpal_wabbit/wiki/Command-Line-Arguments).


```python
# Define VW classification model
args = "--loss_function=logistic --quiet --holdout_off"
vw_model = VowpalWabbitClassifier(
    featuresCol="features", labelCol="label", passThroughArgs=args, numPasses=10
)

# Create a pipeline
vw_pipeline = Pipeline(stages=[vw_featurizer, vw_model])
```


```python
vw_trained = vw_pipeline.fit(train)
```

#### Model Prediction

After the model is trained, we apply it to predict the income of each sample in the test set.


```python
# Making predictions
test = test.withColumn("label", when(col("income").contains("<"), 0.0).otherwise(1.0))
prediction = vw_trained.transform(test)
display(prediction)
```

Finally, we evaluate the model performance using ComputeModelStatistics function which will compute confusion matrix, accuracy, precision, recall, and AUC by default for classification models.


```python
from synapse.ml.train import ComputeModelStatistics

metrics = ComputeModelStatistics(
    evaluationMetric="classification", labelCol="label", scoredLabelsCol="prediction"
).transform(prediction)
display(metrics)
```

## California house price prediction with VowpalWabbitRegressor - Quantile Regression

In this example, we show how to build regression model with VW using California housing dataset

#### Read dataset

We use [*California Housing* dataset](https://scikit-learn.org/stable/datasets/real_world.html#california-housing-dataset). 
The data was derived from the 1990 U.S. census. It consists of 20640 entries with 8 features. 
We use `sklearn.datasets` module to download it easily, then split the set into training and testing by 75/25.


```python
import math
from matplotlib.colors import ListedColormap, Normalize
from matplotlib.cm import get_cmap
import matplotlib.pyplot as plt
from synapse.ml.train import ComputeModelStatistics
from synapse.ml.vw import VowpalWabbitRegressor, VowpalWabbitFeaturizer
import numpy as np
import pandas as pd
from sklearn.datasets import fetch_california_housing
```


```python
california = fetch_california_housing()

feature_cols = ["f" + str(i) for i in range(california.data.shape[1])]
header = ["target"] + feature_cols
df = spark.createDataFrame(
    pd.DataFrame(
        data=np.column_stack((california.target, california.data)), columns=header
    )
).repartition(1)
print("Dataframe has {} rows".format(df.count()))
display(df.limit(10))
```


```python
train_data, test_data = df.randomSplit([0.75, 0.25], seed=42)
```


```python
display(train_data.summary().toPandas())
```


```python
train_data.show(10)
```

Exploratory analysis: plot feature distributions over different target values.


```python
features = train_data.columns[1:]
values = train_data.drop("target").toPandas()
ncols = 5
nrows = math.ceil(len(features) / ncols)

yy = [r["target"] for r in train_data.select("target").collect()]

f, axes = plt.subplots(nrows, ncols, sharey=True, figsize=(30, 10))
f.tight_layout()

for irow in range(nrows):
    axes[irow][0].set_ylabel("target")
    for icol in range(ncols):
        try:
            feat = features[irow * ncols + icol]
            xx = values[feat]

            axes[irow][icol].scatter(xx, yy, s=10, alpha=0.25)
            axes[irow][icol].set_xlabel(feat)
            axes[irow][icol].get_yaxis().set_ticks([])
        except IndexError:
            f.delaxes(axes[irow][icol])
```

#### VW-style feature hashing


```python
vw_featurizer = VowpalWabbitFeaturizer(
    inputCols=feature_cols,
    outputCol="features",
)
vw_train_data = vw_featurizer.transform(train_data)["target", "features"]
vw_test_data = vw_featurizer.transform(test_data)["target", "features"]
display(vw_train_data)
```

#### Model training & Prediction

See [VW wiki](https://github.com/vowpalWabbit/vowpal_wabbit/wiki/Command-Line-Arguments) for command line arguments.


```python
args = "--holdout_off --loss_function quantile -l 0.004 -q :: --power_t 0.3"
vwr = VowpalWabbitRegressor(
    labelCol="target",
    featuresCol="features",
    passThroughArgs=args,
    numPasses=200,
)

# To reduce number of partitions (which will effect performance), use `vw_train_data.repartition(1)`
vw_model = vwr.fit(vw_train_data.repartition(1))
vw_predictions = vw_model.transform(vw_test_data)

display(vw_predictions)
```

#### Compute Statistics & Visualization


```python
metrics = ComputeModelStatistics(
    evaluationMetric="regression", labelCol="target", scoresCol="prediction"
).transform(vw_predictions)

vw_result = metrics.toPandas()
vw_result.insert(0, "model", ["Vowpal Wabbit"])
display(vw_result)
```


```python
cmap = get_cmap("YlOrRd")
target = np.array(test_data.select("target").collect()).flatten()
model_preds = [("Vowpal Wabbit", vw_predictions)]

f, axe = plt.subplots(figsize=(6, 6))
f.tight_layout()

preds = np.array(vw_predictions.select("prediction").collect()).flatten()
err = np.absolute(preds - target)
norm = Normalize()
clrs = cmap(np.asarray(norm(err)))[:, :-1]
plt.scatter(preds, target, s=60, c=clrs, edgecolors="#888888", alpha=0.75)
plt.plot((0, 6), (0, 6), line, color="#888888")
axe.set_xlabel("Predicted values")
axe.set_ylabel("Actual values")
axe.set_title("Vowpal Wabbit")
```

## Quantile Regression for Drug Discovery with VowpalWabbitRegressor

<img src="https://mmlspark.blob.core.windows.net/graphics/Documentation/drug.png" width="800" />

#### Read dataset


```python
triazines = spark.read.format("libsvm").load(
    "wasbs://publicwasb@mmlspark.blob.core.windows.net/triazines.scale.svmlight"
)
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
from synapse.ml.vw import VowpalWabbitRegressor

model = VowpalWabbitRegressor(
    numPasses=20, passThroughArgs="--holdout_off --loss_function quantile -q :: -l 0.1"
).fit(train)
```

#### Model Prediction


```python
scoredData = model.transform(test)
display(scoredData.limit(10))
```


```python
from synapse.ml.train import ComputeModelStatistics

metrics = ComputeModelStatistics(
    evaluationMetric="regression", labelCol="label", scoresCol="prediction"
).transform(scoredData)
display(metrics)
```

## VW Contextual Bandit

#### Read dataset


```python
data = spark.read.format("json").load(
    "wasbs://publicwasb@mmlspark.blob.core.windows.net/vwcb_input.dsjson"
)
```

Note: Actions are all five TAction_x_topic columns.


```python
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType

data = (
    data.withColumn("GUser_id", col("c.GUser.id"))
    .withColumn("GUser_major", col("c.GUser.major"))
    .withColumn("GUser_hobby", col("c.GUser.hobby"))
    .withColumn("GUser_favorite_character", col("c.GUser.favorite_character"))
    .withColumn("TAction_0_topic", col("c._multi.TAction.topic")[0])
    .withColumn("TAction_1_topic", col("c._multi.TAction.topic")[1])
    .withColumn("TAction_2_topic", col("c._multi.TAction.topic")[2])
    .withColumn("TAction_3_topic", col("c._multi.TAction.topic")[3])
    .withColumn("TAction_4_topic", col("c._multi.TAction.topic")[4])
    .withColumn("chosenAction", col("_label_Action").cast(IntegerType()))
    .withColumn("label", col("_labelIndex").cast(DoubleType()))
    .withColumn("probability", col("_label_probability"))
    .select(
        "GUser_id",
        "GUser_major",
        "GUser_hobby",
        "GUser_favorite_character",
        "TAction_0_topic",
        "TAction_1_topic",
        "TAction_2_topic",
        "TAction_3_topic",
        "TAction_4_topic",
        "chosenAction",
        "label",
        "probability",
    )
)

print("Schema: ")
data.printSchema()
```

Add pipeline to add featurizer, convert all feature columns into vector.


```python
from synapse.ml.vw import (
    VowpalWabbitFeaturizer,
    VowpalWabbitContextualBandit,
    VectorZipper,
)
from pyspark.ml import Pipeline

pipeline = Pipeline(
    stages=[
        VowpalWabbitFeaturizer(inputCols=["GUser_id"], outputCol="GUser_id_feature"),
        VowpalWabbitFeaturizer(
            inputCols=["GUser_major"], outputCol="GUser_major_feature"
        ),
        VowpalWabbitFeaturizer(
            inputCols=["GUser_hobby"], outputCol="GUser_hobby_feature"
        ),
        VowpalWabbitFeaturizer(
            inputCols=["GUser_favorite_character"],
            outputCol="GUser_favorite_character_feature",
        ),
        VowpalWabbitFeaturizer(
            inputCols=["TAction_0_topic"], outputCol="TAction_0_topic_feature"
        ),
        VowpalWabbitFeaturizer(
            inputCols=["TAction_1_topic"], outputCol="TAction_1_topic_feature"
        ),
        VowpalWabbitFeaturizer(
            inputCols=["TAction_2_topic"], outputCol="TAction_2_topic_feature"
        ),
        VowpalWabbitFeaturizer(
            inputCols=["TAction_3_topic"], outputCol="TAction_3_topic_feature"
        ),
        VowpalWabbitFeaturizer(
            inputCols=["TAction_4_topic"], outputCol="TAction_4_topic_feature"
        ),
        VectorZipper(
            inputCols=[
                "TAction_0_topic_feature",
                "TAction_1_topic_feature",
                "TAction_2_topic_feature",
                "TAction_3_topic_feature",
                "TAction_4_topic_feature",
            ],
            outputCol="features",
        ),
    ]
)
tranformation_pipeline = pipeline.fit(data)
transformed_data = tranformation_pipeline.transform(data)

display(transformed_data)
```

Build VowpalWabbit Contextual Bandit model and compute performance statistics.


```python
estimator = (
    VowpalWabbitContextualBandit()
    .setPassThroughArgs("--cb_explore_adf --epsilon 0.2 --quiet")
    .setSharedCol("GUser_id_feature")
    .setAdditionalSharedFeatures(
        [
            "GUser_major_feature",
            "GUser_hobby_feature",
            "GUser_favorite_character_feature",
        ]
    )
    .setFeaturesCol("features")
    .setUseBarrierExecutionMode(False)
    .setChosenActionCol("chosenAction")
    .setLabelCol("label")
    .setProbabilityCol("probability")
)
model = estimator.fit(transformed_data)
display(model.getPerformanceStatistics())
```
