import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";


## LinearDMLEstimator

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

df = spark.createDataFrame([
      (0, 1, 0.50, 0.60, 0),
      (1, 0, 0.40, 0.50, 1),
      (0, 1, 0.78, 0.99, 2),
      (1, 0, 0.12, 0.34, 3),
      (0, 1, 0.50, 0.60, 0),
      (1, 1, 0.40, 0.50, 1),
      (0, 1, 0.78, 0.99, 2),
      (1, 0, 0.12, 0.34, 3),
      (0, 1, 0.50, 0.60, 0),
      (1, 0, 0.40, 0.50, 1),
      (0, 1, 0.78, 0.99, 2),
      (1, 1, 0.12, 0.34, 3)],
      ["Treatment", "Outcome", "col2", "col3", "col4"]
)

dml = (LinearDMLEstimator()
      .setTreatmentCol("Treatment")
      .setTreatmentModel(LogisticRegression())
      .setOutcomeCol("Outcome")
      .setOutcomeModel(LogisticRegression())
      .setMaxIter(100))

dmlModel = dml.fit(df)
dmlModel.getAte()
dmlMOdel.getCi()
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

val dml = (new LinearDMLEstimator()
  .setTreatmentCol("Treatment")
  .setTreatmentModel(new LogisticRegression())
  .setOutcomeCol("Outcome")
  .setOutcomeModel(new LogisticRegression())
  .setMaxIter(100))

val dmlModel = dml.fit(df)
dmlModel.getAte
dmlModel.getCi
```

</TabItem>
</Tabs>

<DocTable className="LinearDMLEstimator"
py="synapse.ml.causal.html#module-synapse.ml.causal.LinearDMLEstimator"
scala="com/microsoft/azure/synapse/ml/causal/LinearDMLEstimator.html"
csharp="classSynapse_1_1ML_1_1Causal_1_1LinearDMLEstimator.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/causal/LinearDMLEstimator.scala" />
