---
title: SynapseML Autologging
description: SynapseML autologging
---

## Automatic Logging

[MLflow automatic logging](https://www.mlflow.org/docs/latest/tracking.html#automatic-logging) allows you to log metrics, parameters, and models without the need for explicit log statements.
SynapseML supports autologging for every model in the library.

To enable autologging for SynapseML:
1. Download this customized [log_model_allowlist file](https://mmlspark.blob.core.windows.net/publicwasb/log_model_allowlist.txt) and put it at a place that your code has access to.
For example:
* In Synapse `wasb://<containername>@<accountname>.blob.core.windows.net/PATH_TO_YOUR/log_model_allowlist.txt`
* In Databricks `/dbfs/FileStore/PATH_TO_YOUR/log_model_allowlist.txt`.
2. Set spark configuration `spark.mlflow.pysparkml.autolog.logModelAllowlistFile` to the path of your `log_model_allowlist.txt` file.
3. Call `mlflow.pyspark.ml.autolog()` before your training code to enable autologging for all supported models.

Note:
1. If you want to support autologging of additional PySpark models not in the log_model_allowlist file, you can add models to the file.
2. If you've enabled autologging, then please don't write explicit `with mlflow.start_run()` as it might cause multiple runs for one single model or one run for multiple models.


## Configuration process in Databricks as an example

1. Install latest MLflow via `%pip install mlflow -u`
2. Upload your customized `log_model_allowlist.txt` file to dbfs by clicking File/Upload Data button on Databricks UI.
3. Set Cluster Spark configuration following [this documentation](https://docs.microsoft.com/en-us/azure/databricks/clusters/configure#spark-configuration)
```
spark.mlflow.pysparkml.autolog.logModelAllowlistFile /dbfs/FileStore/PATH_TO_YOUR/log_model_allowlist.txt
```
4. Run the following before your training code, you can also customize corresponding [parameters](https://www.mlflow.org/docs/latest/python_api/mlflow.pyspark.ml.html#mlflow.pyspark.ml.autolog) by passing arguments to `autolog`.
```
mlflow.pyspark.ml.autolog()
```
5. To find your experiment's results via the `Experiments` tab of the MLFlow UI.
<img src="https://mmlspark.blob.core.windows.net/graphics/adb_experiments.png" width="1200" />

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

This should log one run with a ConditionalKNNModel artifact and its parameters.
<img src="https://mmlspark.blob.core.windows.net/graphics/autologgingRunSample.png" width="1200" />
