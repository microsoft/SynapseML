import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";


## DoubleMLEstimator

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.causal import *
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType

schema = StructType([
    StructField("Treatment", IntegerType()),
    StructField("Outcome", IntegerType()),
    StructField("col2", DoubleType()),
    StructField("col3", DoubleType()),
    StructField("col4", DoubleType())
    ])

df = spark.createDataFrame([
      (0, 1, 0.30, 0.66, 0.2),
      (1, 0, 0.38, 0.53, 1.5),
      (0, 1, 0.68, 0.98, 3.2),
      (1, 0, 0.15, 0.32, 6.6),
      (0, 1, 0.50, 0.65, 2.8),
      (1, 1, 0.40, 0.54, 3.7),
      (0, 1, 0.78, 0.97, 8.1),
      (1, 0, 0.12, 0.32, 10.2),
      (0, 1, 0.35, 0.63, 1.8),
      (1, 0, 0.45, 0.57, 4.3),
      (0, 1, 0.75, 0.97, 7.2),
      (1, 1, 0.16, 0.32, 11.7)], schema
)

dml = (DoubleMLEstimator()
      .setTreatmentCol("Treatment")
      .setTreatmentModel(LogisticRegression())
      .setOutcomeCol("Outcome")
      .setOutcomeModel(LogisticRegression())
      .setMaxIter(20))

dmlModel = dml.fit(df)
dmlModel.getAvgTreatmentEffect()
dmlModel.getConfidenceInterval()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.causal._
import org.apache.spark.ml.classification.LogisticRegression

val df = (Seq(
  (0, 1, 0.50, 0.60, 0),
  (1, 0, 0.40, 0.50, 1),
  (0, 1, 0.78, 0.99, 2),
  (1, 0, 0.12, 0.34, 3),
  (0, 1, 0.50, 0.60, 0),
  (1, 0, 0.40, 0.50, 1),
  (0, 1, 0.78, 0.99, 2),
  (1, 0, 0.12, 0.34, 3),
  (0, 0, 0.50, 0.60, 0),
  (1, 1, 0.40, 0.50, 1),
  (0, 1, 0.78, 0.99, 2),
  (1, 0, 0.12, 0.34, 3))
  .toDF("Treatment", "Outcome", "col2", "col3", "col4"))

val dml = (new DoubleMLEstimator()
  .setTreatmentCol("Treatment")
  .setTreatmentModel(new LogisticRegression())
  .setOutcomeCol("Outcome")
  .setOutcomeModel(new LogisticRegression())
  .setMaxIter(20))

val dmlModel = dml.fit(df)
dmlModel.getAvgTreatmentEffect
dmlModel.getConfidenceInterval
```

</TabItem>
</Tabs>

<DocTable className="DoubleMLEstimator"
py="synapse.ml.causal.html#module-synapse.ml.causal.DoubleMLEstimator"
scala="com/microsoft/azure/synapse/ml/causal/DoubleMLEstimator.html"
csharp="classSynapse_1_1ML_1_1Causal_1_1DoubleMLEstimator.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/causal/DoubleMLEstimator.scala" />
