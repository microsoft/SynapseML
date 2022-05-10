---
title: SynapseML Autologging
description: SynapseML autologging
---

## Automatic Logging

[MLflow automatic logging](https://www.mlflow.org/docs/latest/tracking.html#automatic-logging) allows you to log metrics, parameters, and models without the need for explicit log statements.
For SynapseML we support autologging for all of our existing models.

To enable autologging for SynapseML:
1. Download this customized [log_model_allowlist file](https://mmlspark.blob.core.windows.net/publicwasb/log_model_allowlist.txt) and put it at a place that your code have access to.
2. Set spark configuration `spark.mlflow.pysparkml.autolog.logModelAllowlistFile` to the path of your `log_model_allowlist.txt` file. 
3. Call `mlflow.pyspark.ml.autolog()` before your training code to enable autologging for all supported models.

Note:
1. If you want to support autologging of pyspark models that's not in the log_model_allowlist file, you can modify the file to add them.
2. If you've enabled autologging, then please don't write explicit `with mlflow.start_run()` as the behavior would be strange.


## Configuration process in Databricks as an example

1. Install MLflow via `%pip install mlflow`
2. Upload your customized `log_model_allowlist.txt` file to dbfs by clicking File/Upload Data button on Databricks UI.
3. Set Spark configuration:
```
spark.conf.set("spark.mlflow.pysparkml.autolog.logModelAllowlistFile", "/dbfs/FileStore/PATH_TO_YOUR_log_model_allowlist.txt")
```
4. Run the below code before your training code, you can also customize corresponding [parameters](https://www.mlflow.org/docs/latest/python_api/mlflow.pyspark.ml.html#mlflow.pyspark.ml.autolog) here.
```
mlflow.pyspark.ml.autolog()
```
5. Enjoy playing with models and find your experiment results in `Experiments` tab.

## Example for ConditionalKNNModel
```python
from pyspark.ml.linalg import Vectors
from synapse.ml.nn import *

df = spark.createDataFrame([
    (Vectors.dense(2.0,2.0,2.0), "foo", 1),
    (Vectors.dense(2.0,2.0,4.0), "foo", 3),
    (Vectors.dense(2.0,2.0,6.0), "foo", 4),
    (Vectors.dense(2.0,2.0,8.0), "foo", 3),
    (Vectors.dense(2.0,2.0,10.0), "foo", 1),
    (Vectors.dense(2.0,2.0,12.0), "foo", 2),
    (Vectors.dense(2.0,2.0,14.0), "foo", 0),
    (Vectors.dense(2.0,2.0,16.0), "foo", 1),
    (Vectors.dense(2.0,2.0,18.0), "foo", 3),
    (Vectors.dense(2.0,2.0,20.0), "foo", 0),
    (Vectors.dense(2.0,4.0,2.0), "foo", 2),
    (Vectors.dense(2.0,4.0,4.0), "foo", 4),
    (Vectors.dense(2.0,4.0,6.0), "foo", 2),
    (Vectors.dense(2.0,4.0,8.0), "foo", 2),
    (Vectors.dense(2.0,4.0,10.0), "foo", 4),
    (Vectors.dense(2.0,4.0,12.0), "foo", 3),
    (Vectors.dense(2.0,4.0,14.0), "foo", 2),
    (Vectors.dense(2.0,4.0,16.0), "foo", 1),
    (Vectors.dense(2.0,4.0,18.0), "foo", 4),
    (Vectors.dense(2.0,4.0,20.0), "foo", 4)
], ["features","values","labels"])

cnn = (ConditionalKNN().setOutputCol("prediction"))
cnnm = cnn.fit(df)

test_df = spark.createDataFrame([
    (Vectors.dense(2.0,2.0,2.0), "foo", 1, [0, 1]),
    (Vectors.dense(2.0,2.0,4.0), "foo", 4, [0, 1]),
    (Vectors.dense(2.0,2.0,6.0), "foo", 2, [0, 1]),
    (Vectors.dense(2.0,2.0,8.0), "foo", 4, [0, 1]),
    (Vectors.dense(2.0,2.0,10.0), "foo", 4, [0, 1])
], ["features","values","labels","conditioner"])

display(cnnm.transform(test_df))
```

This should log one run that consists of ConditionalKNNModel artifact and its parameters.
