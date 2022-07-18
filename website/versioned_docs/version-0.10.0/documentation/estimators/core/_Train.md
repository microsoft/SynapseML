import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";




## Train

### TrainClassifier

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

df = spark.createDataFrame([
      (0, 2, 0.50, 0.60, 0),
      (1, 3, 0.40, 0.50, 1),
      (0, 4, 0.78, 0.99, 2),
      (1, 5, 0.12, 0.34, 3),
      (0, 1, 0.50, 0.60, 0),
      (1, 3, 0.40, 0.50, 1),
      (0, 3, 0.78, 0.99, 2),
      (1, 4, 0.12, 0.34, 3),
      (0, 0, 0.50, 0.60, 0),
      (1, 2, 0.40, 0.50, 1),
      (0, 3, 0.78, 0.99, 2),
      (1, 4, 0.12, 0.34, 3)],
      ["Label", "col1", "col2", "col3", "col4"]
)

tc = (TrainClassifier()
      .setModel(LogisticRegression())
      .setLabelCol("Label"))

tc.fit(df).transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.train._
import org.apache.spark.ml.classification.LogisticRegression

val df = (Seq(
      (0, 2, 0.50, 0.60, 0),
      (1, 3, 0.40, 0.50, 1),
      (0, 4, 0.78, 0.99, 2),
      (1, 5, 0.12, 0.34, 3),
      (0, 1, 0.50, 0.60, 0),
      (1, 3, 0.40, 0.50, 1),
      (0, 3, 0.78, 0.99, 2),
      (1, 4, 0.12, 0.34, 3),
      (0, 0, 0.50, 0.60, 0),
      (1, 2, 0.40, 0.50, 1),
      (0, 3, 0.78, 0.99, 2),
      (1, 4, 0.12, 0.34, 3))
      .toDF("Label", "col1", "col2", "col3", "col4"))

val tc = (new TrainClassifier()
      .setModel(new LogisticRegression())
      .setLabelCol("Label"))

tc.fit(df).transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="TrainClassifier"
py="synapse.ml.train.html#module-synapse.ml.train.TrainClassifier"
scala="com/microsoft/azure/synapse/ml/train/TrainClassifier.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/train/TrainClassifier.scala" />


### TrainRegressor

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
from pyspark.ml.regression import LinearRegression

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
    ["label", "col1", "col2", "col3", "col4"]))

linearRegressor = (LinearRegression()
      .setRegParam(0.3)
      .setElasticNetParam(0.8))
trainRegressor = (TrainRegressor()
      .setModel(linearRegressor)
      .setLabelCol("label"))

trainRegressor.fit(dataset).transform(dataset).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.train._
import org.apache.spark.ml.regression.LinearRegression

val dataset = (spark.createDataFrame(Seq(
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
    .toDF("label", "col1", "col2", "col3", "col4"))

val linearRegressor = (new LinearRegression()
      .setRegParam(0.3)
      .setElasticNetParam(0.8))
val trainRegressor = (new TrainRegressor()
      .setModel(linearRegressor)
      .setLabelCol("label"))

trainRegressor.fit(dataset).transform(dataset).show()
```

</TabItem>
</Tabs>

<DocTable className="TrainRegressor"
py="synapse.ml.train.html#module-synapse.ml.train.TrainRegressor"
scala="com/microsoft/azure/synapse/ml/train/TrainRegressor.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/train/TrainRegressor.scala" />
