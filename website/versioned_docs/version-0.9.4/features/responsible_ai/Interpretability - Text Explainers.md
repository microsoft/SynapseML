---
title: Interpretability - Text Explainers
hide_title: true
status: stable
---
## Interpretability - Text Explainers

In this example, we use LIME and Kernel SHAP explainers to explain a text classification model.

First we import the packages and define some UDFs and a plotting function we will need later.


```
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import StopWordsRemover, HashingTF, IDF, Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from synapse.ml.explainers import *
from synapse.ml.featurize.text import TextFeaturizer

vec2array = udf(lambda vec: vec.toArray().tolist(), ArrayType(FloatType()))
vec_access = udf(lambda v, i: float(v[i]), FloatType())
```

Load training data, and convert rating to binary label.


```
data = (
    spark.read.parquet("wasbs://publicwasb@mmlspark.blob.core.windows.net/BookReviewsFromAmazon10K.parquet")
    .withColumn("label", (col("rating") > 3).cast(LongType()))
    .select("label", "text")
)

data.limit(10).toPandas()
```

We train a text classification model, and randomly sample 10 rows to explain.


```
train, test = data.randomSplit([0.60, 0.40])

pipeline = Pipeline(
    stages=[
        TextFeaturizer(
            inputCol="text",
            outputCol="features",
            useStopWordsRemover=True,
            useIDF=True,
            minDocFreq=20,
            numFeatures=1 << 16,
        ),
        LogisticRegression(maxIter=100, regParam=0.005, labelCol="label", featuresCol="features"),
    ]
)

model = pipeline.fit(train)

prediction = model.transform(test)

explain_instances = prediction.orderBy(rand()).limit(10)
```


```
def plotConfusionMatrix(df, label, prediction, classLabels):
    from synapse.ml.plot import confusionMatrix
    import matplotlib.pyplot as plt

    fig = plt.figure(figsize=(4.5, 4.5))
    confusionMatrix(df, label, prediction, classLabels)
    display(fig)


plotConfusionMatrix(model.transform(test), "label", "prediction", [0, 1])
```

First we use the LIME text explainer to explain the model's predicted probability for a given observation.


```
lime = TextLIME(
    model=model,
    outputCol="weights",
    inputCol="text",
    targetCol="probability",
    targetClasses=[1],
    tokensCol="tokens",
    samplingFraction=0.7,
    numSamples=2000,
)

lime_results = (
    lime.transform(explain_instances)
    .select("tokens", "weights", "r2", "probability", "text")
    .withColumn("probability", vec_access("probability", lit(1)))
    .withColumn("weights", vec2array(col("weights").getItem(0)))
    .withColumn("r2", vec_access("r2", lit(0)))
    .withColumn("tokens_weights", arrays_zip("tokens", "weights"))
)

display(lime_results.select("probability", "r2", "tokens_weights", "text").orderBy(col("probability").desc()))
```

Then we use the Kernel SHAP text explainer to explain the model's predicted probability for a given observation.

> Notice that we drop the base value from the SHAP output before displaying the SHAP values. The base value is the model output for an empty string.


```
shap = TextSHAP(
    model=model,
    outputCol="shaps",
    inputCol="text",
    targetCol="probability",
    targetClasses=[1],
    tokensCol="tokens",
    numSamples=5000,
)

shap_results = (
    shap.transform(explain_instances)
    .select("tokens", "shaps", "r2", "probability", "text")
    .withColumn("probability", vec_access("probability", lit(1)))
    .withColumn("shaps", vec2array(col("shaps").getItem(0)))
    .withColumn("shaps", slice(col("shaps"), lit(2), size(col("shaps"))))
    .withColumn("r2", vec_access("r2", lit(0)))
    .withColumn("tokens_shaps", arrays_zip("tokens", "shaps"))
)

display(shap_results.select("probability", "r2", "tokens_shaps", "text").orderBy(col("probability").desc()))
```
