---
title: Quickstart - Fine-tune a Vision Classifier
hide_title: true
status: stable
---
## Deep Learning - Deep Vision Classifier

### Environment Setup on databricks
### -- reinstall horovod based on new version of pytorch


```python
# install cloudpickle 2.0.0 to add synapse module for usage of horovod
%pip install cloudpickle==2.0.0 --force-reinstall --no-deps
```


```python
import synapse
import cloudpickle
import os
import urllib.request
import zipfile

cloudpickle.register_pickle_by_value(synapse)
```


```python
! horovodrun --check-build
```


```python
from pyspark.sql.functions import udf, col, regexp_replace
from pyspark.sql.types import IntegerType
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
```

### Read Dataset


```python
folder_path = "/tmp/flowers_prepped"
zip_url = "https://mmlspark.blob.core.windows.net/datasets/Flowers/flowers_prepped.zip"
zip_path = "/dbfs/tmp/flowers_prepped.zip"

if not os.path.exists("/dbfs" + folder_path):
    urllib.request.urlretrieve(zip_url, zip_path)
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall("/dbfs/tmp")
    os.remove(zip_path)
```


```python
def assign_label(path):
    num = int(path.split("/")[-1].split(".")[0].split("_")[1])
    return num // 81


assign_label_udf = udf(assign_label, IntegerType())
```


```python
# These files are already uploaded for build test machine
train_df = (
    spark.read.format("binaryFile")
    .option("pathGlobFilter", "*.jpg")
    .load(folder_path + "/train")
    .withColumn("image", regexp_replace("path", "dbfs:", "/dbfs"))
    .withColumn("label", assign_label_udf(col("path")))
    .select("image", "label")
)

display(train_df.limit(100))
```


```python
test_df = (
    spark.read.format("binaryFile")
    .option("pathGlobFilter", "*.jpg")
    .load(folder_path + "/test")
    .withColumn("image", regexp_replace("path", "dbfs:", "/dbfs"))
    .withColumn("label", assign_label_udf(col("path")))
    .select("image", "label")
)
```

### Training


```python
from horovod.spark.common.store import DBFSLocalStore
from pytorch_lightning.callbacks import ModelCheckpoint
from synapse.ml.dl import *
import uuid

run_output_dir = f"/dbfs/FileStore/test/resnet50/{str(uuid.uuid4())[:8]}"
store = DBFSLocalStore(run_output_dir)

epochs = 10

callbacks = [ModelCheckpoint(filename="{epoch}-{train_loss:.2f}")]
```


```python
deep_vision_classifier = DeepVisionClassifier(
    backbone="resnet50",
    store=store,
    callbacks=callbacks,
    num_classes=17,
    batch_size=16,
    epochs=epochs,
    validation=0.1,
)

deep_vision_model = deep_vision_classifier.fit(train_df)
```

### Prediction


```python
pred_df = deep_vision_model.transform(test_df)
evaluator = MulticlassClassificationEvaluator(
    predictionCol="prediction", labelCol="label", metricName="accuracy"
)
print("Test accuracy:", evaluator.evaluate(pred_df))
```


```python
# Cleanup the output dir for test
dbutils.fs.rm(run_output_dir, True)
```
