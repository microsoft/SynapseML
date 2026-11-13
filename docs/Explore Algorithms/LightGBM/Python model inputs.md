---
title: Python model inputs
hide_title: true
sidebar_label: Python model inputs
---

# Categorical and missing inputs for a Python-trained model

A LightGBM text model can be imported into SynapseML. Its inputs still need the
**same feature order and categorical codes used during Python training**.
`LightGBMBooster.score` and `predictLeaf` take numeric Spark vectors; they do not
look up categorical strings or reconstruct your preprocessing pipeline.

The examples below use a synthetic regression model trained by Python LightGBM
3.3.5 and loaded by lightgbmlib 3.3.510, the native library used by this SynapseML
version. Other version pairs require their own checks. This is an input
preparation example, not a guarantee that arbitrary Python and native releases
are interchangeable.

## Preserve the Python encoding

For pandas categorical training columns, the text model's `pandas_categorical:`
line records the ordered category lists. Save the training feature order and
which columns are categorical as part of your preprocessing contract too.
A category's numeric code is its zero-based position in its training list.
For example, `["red", "blue", "green"]` means red=0, blue=1, green=2.

A newly fitted Spark `StringIndexer` defaults to ordering labels by frequency;
it can produce a different code for the same label. A model imported from
Python does not retrain itself to account for that change. Use a fixed training
mapping, for example `StringIndexerModel.from_labels` below, and explicitly
convert null and unseen labels to NaN. Do not use the additional index from
`handleInvalid="keep"` as a replacement for Python's missing-value conversion.

For pandas categorical prediction, Python aligns the categories to training,
then converts unknown or missing category codes to NaN. Follow that conversion
when preparing JVM inputs. Do not substitute category 0, -1 or an out-of-range
code for NaN. The route taken by a missing value depends on the trained model
and its per-split missing-value settings, not just whether a training column
contained missing values.

For numeric columns, preserve missing values as `Double.NaN` / `float("nan")`.
A primitive double vector cannot contain null; convert nullable values first.
Under the default `zero_as_missing=false`, omitted sparse entries are zeros,
not NaNs. Retain NaNs explicitly in sparse values. A zero is not a general
replacement for missing data. Models trained with different missing-value
settings must retain those settings and their corresponding preprocessing.

## Train and export a reproducible example in Python

Run from the SynapseML source root in Python 3.11.8 with LightGBM 3.3.5,
NumPy 1.26.4, pandas 2.0.3, SciPy 1.17.1, scikit-learn 1.9.1 and wheel 0.48.0.
The optional fixture generator is not part of ordinary upstream Scala tests;
the extra Python LightGBM package is not in the general SynapseML environment.
Choose a new output directory shared by the following Python and Scala driver
examples. It is a local driver path, not a distributed filesystem URI.

```python
from pathlib import Path
import json
import subprocess
import sys

fixture = Path("/tmp/python-lightgbm-interop")
subprocess.run(
    [sys.executable, "lightgbm/src/test/resources/python-interop/generate.py",
     "--output", str(fixture)],
    check=True,
)
model_text = (fixture / "python-model.txt").read_text()
metadata_line = next(line for line in model_text.splitlines()
                     if line.startswith("pandas_categorical:"))
category_lists = json.loads(metadata_line.split(":", 1)[1])
assert category_lists == [["red", "blue", "green"], ["red", "blue", "green"]]
```

The generator trains a real Python model, serializes and reloads its text, and
records predictions and leaf indices. Its four input columns are
`numeric_missing`, `numeric_complete`, `category_missing`, `category_complete`.
Both categorical columns use the same training order in this example. Keep
separate ordered mappings when your columns use different categories.

## Score that model directly in Scala

The Python-generated files must be available to the Scala driver at the same
local path. Native prediction uses numeric codes; no extra StringIndexer is
needed when your application already preserves the training mapping.

```scala
import com.microsoft.azure.synapse.ml.lightgbm.booster.LightGBMBooster
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import org.apache.spark.ml.linalg.Vectors

val directory = Paths.get("/tmp/python-lightgbm-interop")
val modelText = new String(Files.readAllBytes(directory.resolve("python-model.txt")), StandardCharsets.UTF_8)
val booster = new LightGBMBooster(modelText)
try {
  // The third feature is blue (code 1); the fourth is red (code 0).
  val features = Vectors.dense(1.0, 0.0, 1.0, 0.0)
  val scores = booster.score(features, raw = false, classification = false, disableShapeCheck = false)
  val leaves = booster.predictLeaf(features)
  assert(scores.length == 1 && leaves.length == 40)
  assert(math.abs(scores.head - 13.05047166690856) < 1e-9)
  println(s"prediction=${scores.head}, leaves=${leaves.mkString(",")}")
} finally {
  booster.freeNativeMemory()
}
```

Changing blue's code from 1 to 2 gives a different prediction for this model.
Similarly, replacing a missing categorical input with red's code 0 changes its
prediction. The committed regression checks all 14 reference inputs using
dense and sparse vectors, raw and normal scores, and exact leaf indices.
These observations demonstrate why the encoding matters; they do not define
universal routing rules for NaN or out-of-range codes.

## Prepare strings and nulls in PySpark

This example uses the same exported model, an active Spark session `spark`,
and SynapseML installed on the cluster. The fixed indexer reuses the exact
training label order. Null and unseen input values become NaN explicitly;
`VectorAssembler` then keeps those NaNs in the feature vector.

```python
from pyspark.ml.feature import StringIndexer, StringIndexerModel, VectorAssembler
from pyspark.sql import functions as F
from synapse.ml.lightgbm import LightGBMRegressionModel

labels = category_lists[0]
rows = [(1.0, 0.0, "blue", "red"),
        (1.0, 0.0, None, "red"),
        (1.0, 0.0, "purple", "red"),
        (None, 0.0, "red", "red")]
inputs = spark.createDataFrame(
    rows, "numeric_missing double, numeric_complete double, color string, other_color string"
)
fixed = StringIndexerModel.from_labels(
    labels, inputCol="color", outputCol="indexed_color", handleInvalid="keep"
)
# A fresh frequency-ordered fit would code blue as 0 instead of 1.
frequency_data = spark.createDataFrame(
    [("blue",)] * 3 + [("red",)] * 2 + [("green",)], ["color"]
)
fresh = StringIndexer(inputCol="color", outputCol="fresh_index").fit(frequency_data)
assert fresh.labels[0] == "blue" and fixed.labels[0] == "red"
indexed = fixed.transform(inputs).withColumn(
    "category_missing",
    F.when(F.col("color").isNull() | ~F.col("color").isin(labels), F.lit(float("nan")))
     .otherwise(F.col("indexed_color")),
)
other_labels = category_lists[1]
other_fixed = StringIndexerModel.from_labels(
    other_labels, inputCol="other_color", outputCol="indexed_other", handleInvalid="keep"
)
indexed = other_fixed.transform(indexed).withColumn(
    "category_complete",
    F.when(F.col("other_color").isNull() | ~F.col("other_color").isin(other_labels),
           F.lit(float("nan"))).otherwise(F.col("indexed_other")),
)
assembler = VectorAssembler(
    inputCols=["numeric_missing", "numeric_complete", "category_missing", "category_complete"],
    outputCol="features", handleInvalid="keep",
)
encoded = assembler.transform(indexed)
imported = LightGBMRegressionModel.loadNativeModelFromString(model_text)
predictions = imported.transform(encoded)
actual = {row.color: row.prediction for row in predictions.select("color", "prediction").collect()}
assert abs(actual["blue"] - 13.05047166690856) < 1e-9
assert abs(actual[None] - 43.92079434558188) < 1e-9
assert abs(actual["purple"] - actual[None]) < 1e-9
assert abs(actual["red"] - 29.92946555978822) < 1e-9
```

Keep the label mappings with the model for serving. Refitting an indexer or
assembling columns in a new order is a preprocessing change, even when the
model text itself is unchanged. Repeat interoperability checks when changing
LightGBM versions or model settings.
