import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";




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

cms.transform(df).show()
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

cms.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="ComputeModelStatistics"
py="synapse.ml.train.html#module-synapse.ml.train.ComputeModelStatistics"
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

cps = (ComputePerInstanceStatistics()
      .setLabelCol("label")
      .setScoredLabelsCol("LogRegScoredLabelsCol")
      .setScoresCol("LogRegScoresCol")
      .setScoredProbabilitiesCol("LogRegProbCol")
      .setEvaluationMetric("classification"))
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

cps.transform(scoredData).show()
```

</TabItem>
</Tabs>

<DocTable className="ComputePerInstanceStatistics"
py="synapse.ml.train.html#module-synapse.ml.train.ComputePerInstanceStatistics"
scala="com/microsoft/azure/synapse/ml/train/ComputePerInstanceStatistics.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/train/ComputePerInstanceStatistics.scala" />
