---
title: Quickstart - ONNX Model Inference
hide_title: true
status: stable
---
# ONNX Inference on Spark

In this example, you train a LightGBM model and convert the model to [ONNX](https://onnx.ai/) format. Once converted, you use the model to infer some testing data on Spark.

This example uses the following Python packages and versions:

- `onnxmltools==1.7.0`
- `lightgbm==3.2.1`


## Load the example data

To load the example data, add the following code examples to cells in your notebook and then run the cells:


```python
%pip install lightgbm onnxmltools==1.7.0
```


```python
df = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(
        "wasbs://publicwasb@mmlspark.blob.core.windows.net/company_bankruptcy_prediction_data.csv"
    )
)

display(df)
```

The output should look similar to the following table, though the values and number of rows may differ:

| Interest Coverage Ratio | Net Income Flag | Equity to Liability |
| ----- | ----- | ----- |
| 0.5641 | 1.0 | 0.0165 |
| 0.5702 | 1.0 | 0.0208 |
| 0.5673 | 1.0 | 0.0165 |

## Use LightGBM to train a model


```python
from pyspark.ml.feature import VectorAssembler
from synapse.ml.lightgbm import LightGBMClassifier

feature_cols = df.columns[1:]
featurizer = VectorAssembler(inputCols=feature_cols, outputCol="features")

train_data = featurizer.transform(df)["Bankrupt?", "features"]

model = (
    LightGBMClassifier(featuresCol="features", labelCol="Bankrupt?")
    .setEarlyStoppingRound(300)
    .setLambdaL1(0.5)
    .setNumIterations(1000)
    .setNumThreads(-1)
    .setMaxDeltaStep(0.5)
    .setNumLeaves(31)
    .setMaxDepth(-1)
    .setBaggingFraction(0.7)
    .setFeatureFraction(0.7)
    .setBaggingFreq(2)
    .setObjective("binary")
    .setIsUnbalance(True)
    .setMinSumHessianInLeaf(20)
    .setMinGainToSplit(0.01)
)

model = model.fit(train_data)
```

## Convert the model to ONNX format

The following code exports the trained model to a LightGBM booster and then converts it to ONNX format:


```python
from synapse.ml.core.platform import running_on_binder

if running_on_binder():
    from IPython import get_ipython
```


```python
import lightgbm as lgb
from lightgbm import Booster, LGBMClassifier


def convertModel(lgbm_model: LGBMClassifier or Booster, input_size: int) -> bytes:
    from onnxmltools.convert import convert_lightgbm
    from onnxconverter_common.data_types import FloatTensorType

    initial_types = [("input", FloatTensorType([-1, input_size]))]
    onnx_model = convert_lightgbm(
        lgbm_model, initial_types=initial_types, target_opset=9
    )
    return onnx_model.SerializeToString()


booster_model_str = model.getLightGBMBooster().modelStr().get()
booster = lgb.Booster(model_str=booster_model_str)
model_payload_ml = convertModel(booster, len(feature_cols))
```

After conversion, load the ONNX payload into an `ONNXModel` and inspect the model inputs and outputs:


```python
from synapse.ml.onnx import ONNXModel

onnx_ml = ONNXModel().setModelPayload(model_payload_ml)

print("Model inputs:" + str(onnx_ml.getModelInputs()))
print("Model outputs:" + str(onnx_ml.getModelOutputs()))
```

Map the model input to the input dataframe's column name (FeedDict), and map the output dataframe's column names to the model outputs (FetchDict).


```python
onnx_ml = (
    onnx_ml.setDeviceType("CPU")
    .setFeedDict({"input": "features"})
    .setFetchDict({"probability": "probabilities", "prediction": "label"})
    .setMiniBatchSize(5000)
)
```

## Use the model for inference

To perform inference with the model, the following code creates test data and transforms the data through the ONNX model.


```python
from pyspark.ml.feature import VectorAssembler
import pandas as pd
import numpy as np

n = 1000 * 1000
m = 95
test = np.random.rand(n, m)
testPdf = pd.DataFrame(test)
cols = list(map(str, testPdf.columns))
testDf = spark.createDataFrame(testPdf)
testDf = testDf.union(testDf).repartition(200)
testDf = (
    VectorAssembler()
    .setInputCols(cols)
    .setOutputCol("features")
    .transform(testDf)
    .drop(*cols)
    .cache()
)

display(onnx_ml.transform(testDf))
```

The output should look similar to the following table, though the values and number of rows may differ:

| Index | Features | Prediction | Probability |
| ----- | ----- | ----- | ----- |
| 1 | `"{"type":1,"values":[0.105...` | 0 | `"{"0":0.835...` |
| 2 | `"{"type":1,"values":[0.814...` | 0 | `"{"0":0.658...` |
