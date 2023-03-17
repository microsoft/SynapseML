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
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, BooleanType

schema = StructType([
    StructField("Treatment", BooleanType()),
    StructField("Outcome", BooleanType()),
    StructField("col2", DoubleType()),
    StructField("col3", DoubleType()),
    StructField("col4", DoubleType())
    ])


df = spark.createDataFrame([
      (False, True, 0.30, 0.66, 0.2),
      (True, False, 0.38, 0.53, 1.5),
      (False, True, 0.68, 0.98, 3.2),
      (True, False, 0.15, 0.32, 6.6),
      (False, True, 0.50, 0.65, 2.8),
      (True, True, 0.40, 0.54, 3.7),
      (False, True, 0.78, 0.97, 8.1),
      (True, False, 0.12, 0.32, 10.2),
      (False, True, 0.35, 0.63, 1.8),
      (True, False, 0.45, 0.57, 4.3),
      (False, True, 0.75, 0.97, 7.2),
      (True, True, 0.16, 0.32, 11.7)], schema
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
  (false, true, 0.50, 0.60, 0),
  (true, false, 0.40, 0.50, 1),
  (false, true, 0.78, 0.99, 2),
  (true, false, 0.12, 0.34, 3),
  (false, true, 0.50, 0.60, 0),
  (true, false, 0.40, 0.50, 1),
  (false, true, 0.78, 0.99, 2),
  (true, false, 0.12, 0.34, 3),
  (false, false, 0.50, 0.60, 0),
  (true, true, 0.40, 0.50, 1),
  (false, true, 0.78, 0.99, 2),
  (true, false, 0.12, 0.34, 3))
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
