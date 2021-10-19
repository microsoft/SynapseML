import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";

<!-- 
```python
import pyspark
import os
import json
from IPython.display import display

spark = (pyspark.sql.SparkSession.builder.appName("MyApp")
        .config("spark.jars.packages", "com.microsoft.azure:synapseml:0.9.1")
        .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven")
        .getOrCreate())

def getSecret(secretName):
        get_secret_cmd = 'az keyvault secret show --vault-name mmlspark-build-keys --name {}'.format(secretName)
        value = json.loads(os.popen(get_secret_cmd).read())["value"]
        return value

import synapse.ml
```
-->

## Train

### ComputeModelStatistics

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.train import *
from numpy import random

df = spark.createDataFrame(
      [(random.rand(), random.rand()) for _ in range(4096)], ["label", "prediction"]
)

cms = (ComputeModelStatistics()
      .setLabelCol("label")
      .setScoredLabelsCol("prediction")
      .setEvaluationMetric("classification"))

display(cms.transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.train._
import scala.util.Random

val rand = new Random(1337)
val df = (Seq.fill(4096)(rand.nextDouble())
      .zip(Seq.fill(4096)(rand.nextDouble()))
      .toDF("label", "prediction"))

val cms = (new ComputeModelStatistics()
      .setLabelCol("label")
      .setScoredLabelsCol("prediction")
      .setEvaluationMetric("classification"))

display(cms.transform(df))
```

</TabItem>
</Tabs>

<DocTable className="ComputeModelStatistics"
py="mmlspark.train.html#module-mmlspark.train.ComputeModelStatistics"
scala="com/microsoft/azure/synapse/ml/train/ComputeModelStatistics.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/train/ComputeModelStatistics.scala" />


### ComputePerInstanceStatistics

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.train import *
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import FastVectorAssembler

logisticRegression = (LogisticRegression()
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setMaxIter(10)
      .setLabelCol("label")
      .setPredictionCol("LogRegScoredLabelsCol")
      .setRawPredictionCol("LogRegScoresCol")
      .setProbabilityCol("LogRegProbCol")
      .setFeaturesCol("features"))

dataset = (spark.createDataFrame([
    (0.0, 2, 0.50, 0.60, 0.0),
    (1.0, 3, 0.40, 0.50, 1.0),
    (2.0, 4, 0.78, 0.99, 2.0),
    (3.0, 5, 0.12, 0.34, 3.0),
    (0.0, 1, 0.50, 0.60, 0.0),
    (1.0, 3, 0.40, 0.50, 1.0),
    (2.0, 3, 0.78, 0.99, 2.0),
    (3.0, 4, 0.12, 0.34, 3.0),
    (0.0, 0, 0.50, 0.60, 0.0),
    (1.0, 2, 0.40, 0.50, 1.0),
    (2.0, 3, 0.78, 0.99, 2.0),
    (3.0, 4, 0.12, 0.34, 3.0)],
    ["label", "col1", "col2", "col3", "prediction"]))

assembler = (FastVectorAssembler()
      .setInputCols(["col1", "col2", "col3"])
      .setOutputCol("features"))
assembledDataset = assembler.transform(dataset)
model = logisticRegression.fit(assembledDataset)
scoredData = model.transform(assembledDataset)

cps = (ComputePerInstanceStatistics()
      .setLabelCol("label")
      .setScoredLabelsCol("LogRegScoredLabelsCol")
      .setScoresCol("LogRegScoresCol")
      .setScoredProbabilitiesCol("LogRegProbCol")
      .setEvaluationMetric("classification"))

display(cps.transform(scoredData))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.train._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.FastVectorAssembler

val logisticRegression = (new LogisticRegression()
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setMaxIter(10)
      .setLabelCol("label")
      .setPredictionCol("LogRegScoredLabelsCol")
      .setRawPredictionCol("LogRegScoresCol")
      .setProbabilityCol("LogRegProbCol")
      .setFeaturesCol("features"))

val dataset = spark.createDataFrame(Seq(
    (0.0, 2, 0.50, 0.60, 0.0),
    (1.0, 3, 0.40, 0.50, 1.0),
    (2.0, 4, 0.78, 0.99, 2.0),
    (3.0, 5, 0.12, 0.34, 3.0),
    (0.0, 1, 0.50, 0.60, 0.0),
    (1.0, 3, 0.40, 0.50, 1.0),
    (2.0, 3, 0.78, 0.99, 2.0),
    (3.0, 4, 0.12, 0.34, 3.0),
    (0.0, 0, 0.50, 0.60, 0.0),
    (1.0, 2, 0.40, 0.50, 1.0),
    (2.0, 3, 0.78, 0.99, 2.0),
    (3.0, 4, 0.12, 0.34, 3.0)))
    .toDF("label", "col1", "col2", "col3", "prediction")

val assembler = (new FastVectorAssembler()
      .setInputCols(Array("col1", "col2", "col3"))
      .setOutputCol("features"))
val assembledDataset = assembler.transform(dataset)
val model = logisticRegression.fit(assembledDataset)
val scoredData = model.transform(assembledDataset)

val cps = (new ComputePerInstanceStatistics()
      .setLabelCol("label")
      .setScoredLabelsCol("LogRegScoredLabelsCol")
      .setScoresCol("LogRegScoresCol")
      .setScoredProbabilitiesCol("LogRegProbCol")
      .setEvaluationMetric("classification"))

display(cps.transform(scoredData))
```

</TabItem>
</Tabs>

<DocTable className="ComputePerInstanceStatistics"
py="mmlspark.train.html#module-mmlspark.train.ComputePerInstanceStatistics"
scala="com/microsoft/azure/synapse/ml/train/ComputePerInstanceStatistics.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/train/ComputePerInstanceStatistics.scala" />



