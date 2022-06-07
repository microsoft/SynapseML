---
title: Examples
description: Examples using SynapseML with MLflow
---

## Prerequisites

If you're using Databricks, please install mlflow with below command:
```
# run this so that mlflow is installed on workers besides driver
%pip install mlflow
```

Install SynapseML based on the [installation guidance](../getting_started/installation.md).

## API Reference

* [mlflow.spark.save_model](https://www.mlflow.org/docs/latest/python_api/mlflow.spark.html#mlflow.spark.save_model)
* [mlflow.spark.log_model](https://www.mlflow.org/docs/latest/python_api/mlflow.spark.html#mlflow.spark.log_model)
* [mlflow.spark.load_model](https://www.mlflow.org/docs/latest/python_api/mlflow.spark.html#mlflow.spark.load_model)
* [mlflow.log_metric](https://mlflow.org/docs/latest/python_api/mlflow.html#mlflow.log_metric)

## LightGBMClassificationModel

```python
import mlflow
from synapse.ml.featurize import Featurize
from synapse.ml.lightgbm import *
from synapse.ml.train import ComputeModelStatistics

with mlflow.start_run():

    feature_columns = ["Number of times pregnant","Plasma glucose concentration a 2 hours in an oral glucose tolerance test",
    "Diastolic blood pressure (mm Hg)","Triceps skin fold thickness (mm)","2-Hour serum insulin (mu U/ml)",
    "Body mass index (weight in kg/(height in m)^2)","Diabetes pedigree function","Age (years)"]
    df = spark.createDataFrame([
        (0,131,66,40,0,34.3,0.196,22,1),
        (7,194,68,28,0,35.9,0.745,41,1),
        (3,139,54,0,0,25.6,0.402,22,1),
        (6,134,70,23,130,35.4,0.542,29,1),
        (9,124,70,33,402,35.4,0.282,34,0),
        (0,93,100,39,72,43.4,1.021,35,0),
        (4,110,76,20,100,28.4,0.118,27,0),
        (2,127,58,24,275,27.7,1.6,25,0),
        (0,104,64,37,64,33.6,0.51,22,1),
        (2,120,54,0,0,26.8,0.455,27,0),
        (7,178,84,0,0,39.9,0.331,41,1),
        (2,88,58,26,16,28.4,0.766,22,0),
        (1,91,64,24,0,29.2,0.192,21,0),
        (10,101,76,48,180,32.9,0.171,63,0),
        (5,73,60,0,0,26.8,0.268,27,0),
        (3,158,70,30,328,35.5,0.344,35,1),
        (2,105,75,0,0,23.3,0.56,53,0),
        (12,84,72,31,0,29.7,0.297,46,1),
        (9,119,80,35,0,29.0,0.263,29,1),
        (6,93,50,30,64,28.7,0.356,23,0),
        (1,126,60,0,0,30.1,0.349,47,1)
    ], feature_columns+["labels"]).repartition(2)


    featurize = (Featurize()
    .setOutputCol("features")
    .setInputCols(featureColumns)
    .setOneHotEncodeCategoricals(True)
    .setNumFeatures(4096))

    df_trans = featurize.fit(df).transform(df)

    lightgbm_classifier = (LightGBMClassifier()
            .setFeaturesCol("features")
            .setRawPredictionCol("rawPrediction")
            .setDefaultListenPort(12402)
            .setNumLeaves(5)
            .setNumIterations(10)
            .setObjective("binary")
            .setLabelCol("labels")
            .setLeafPredictionCol("leafPrediction")
            .setFeaturesShapCol("featuresShap"))

    lightgbm_model = lightgbm_classifier.fit(df_trans)

    # Use mlflow.spark.save_model to save the model to your path
    mlflow.spark.save_model(lightgbm_model, "lightgbm_model")
    # Use mlflow.spark.log_model to log the model if you have a connected mlflow service
    mlflow.spark.log_model(lightgbm_model, "lightgbm_model")

    # Use mlflow.pyfunc.load_model to load model back as PyFuncModel and apply predict
    prediction = mlflow.pyfunc.load_model("lightgbm_model").predict(df_trans.toPandas())
    prediction = list(map(str, prediction))
    mlflow.log_param("prediction", ",".join(prediction))

    # Use mlflow.spark.load_model to load model back as PipelineModel and apply transform
    predictions = mlflow.spark.load_model("lightgbm_model").transform(df_trans)
    metrics = ComputeModelStatistics(evaluationMetric="classification", labelCol='labels', scoredLabelsCol='prediction').transform(predictions).collect()
    mlflow.log_metric("accuracy", metrics[0]['accuracy'])
```

## Cognitive Services

Note: Cognitive Services are not supported direct save/load by mlflow for now, so we need to wrap it as a PipelineModel manually.

The [feature request](https://github.com/mlflow/mlflow/issues/5216) to support them (Transformers in general) has been under progress, please vote for the issue if you'd like.

```python
import mlflow
from synapse.ml.cognitive import *
from pyspark.ml import PipelineModel

with mlflow.start_run():

    text_key = "YOUR_COG_SERVICE_SUBSCRIPTION_KEY"
    df = spark.createDataFrame([
    ("I am so happy today, its sunny!", "en-US"),
    ("I am frustrated by this rush hour traffic", "en-US"),
    ("The cognitive services on spark aint bad", "en-US"),
    ], ["text", "language"])

    sentiment = (TextSentiment()
                .setSubscriptionKey(text_key)
                .setLocation("eastus")
                .setTextCol("text")
                .setOutputCol("prediction")
                .setErrorCol("error")
                .setLanguageCol("language"))

    display(sentiment.transform(df))

    # Wrap it as a stage in the PipelineModel
    sentiment_model = PipelineModel(stages=[sentiment])
    mlflow.spark.save_model(sentiment_model, "sentiment_model")
    mlflow.spark.log_model(sentiment_model, "sentiment_model")

    output_df = mlflow.spark.load_model("sentiment_model").transform(df)
    display(output_df)

    # In order to call the predict function successfully you need to specify the
    # outputCol name as `prediction`
    prediction = mlflow.pyfunc.load_model("sentiment_model").predict(df.toPandas())
    prediction = list(map(str, prediction))
    mlflow.log_param("prediction", ",".join(prediction))
```

