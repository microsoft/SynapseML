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

## TrainClassifier

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

display(tc.fit(df).transform(df))
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

display(tc.fit(df).transform(df))
```

</TabItem>
</Tabs>

<DocTable className="TrainClassifier"
py="mmlspark.train.html#module-mmlspark.train.TrainClassifier"
scala="com/microsoft/ml/spark/train/TrainClassifier.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/train/TrainClassifier.scala" />


## TrainRegressor

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

linearRegressor = (LinearRegression()
      .setRegParam(0.3)
      .setElasticNetParam(0.8))
trainRegressor = (TrainRegressor()
      .setModel(linearRegressor)
      .setLabelCol("Label"))

display(trainRegressor.fit(dataset).transform(dataset))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.train._
import org.apache.spark.ml.classification.LogisticRegression

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

val linearRegressor = (new LinearRegression()
      .setRegParam(0.3)
      .setElasticNetParam(0.8))
val trainRegressor = (new TrainRegressor()
      .setModel(linearRegressor)
      .setLabelCol("Label"))

display(trainRegressor.fit(dataset).transform(dataset))
```

</TabItem>
</Tabs>

<DocTable className="TrainRegressor"
py="mmlspark.train.html#module-mmlspark.train.TrainRegressor"
scala="com/microsoft/ml/spark/train/TrainRegressor.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/train/TrainRegressor.scala" />



