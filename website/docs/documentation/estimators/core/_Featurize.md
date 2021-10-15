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
        .config("spark.jars.packages", "com.microsoft.ml.spark:mmlspark:1.0.0-rc4")
        .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven")
        .getOrCreate())

def getSecret(secretName):
        get_secret_cmd = 'az keyvault secret show --vault-name mmlspark-build-keys --name {}'.format(secretName)
        value = json.loads(os.popen(get_secret_cmd).read())["value"]
        return value

import mmlspark
```
-->

## CleanMissingData

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from mmlspark.featurize import *

dataset = spark.createDataFrame([
    (0,    2,    0.50, 0.60, 0),
    (1,    3,    0.40, None, None),
    (0,    4,    0.78, 0.99, 2),
    (1,    5,    0.12, 0.34, 3),
    (0,    1,    0.50, 0.60, 0),
    (None, None, None, None, None),
    (0,    3,    0.78, 0.99, 2),
    (1,    4,    0.12, 0.34, 3),
    (0,    None, 0.50, 0.60, 0),
    (1,    2,    0.40, 0.50, None),
    (0,    3,    None, 0.99, 2),
    (1,    4,    0.12, 0.34, 3)
], ["col1", "col2", "col3", "col4", "col5"])

cmd = (CleanMissingData()
      .setInputCols(dataset.columns)
      .setOutputCols(dataset.columns)
      .setCleaningMode("Mean"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.featurize._
import java.lang.{Boolean => JBoolean, Double => JDouble, Integer => JInt}
import spark.implicits._

def createMockDataset: DataFrame = {
    Seq[(JInt, JInt, JDouble, JDouble, JInt)](
      (0,    2,    0.50, 0.60, 0),
      (1,    3,    0.40, null, null),
      (0,    4,    0.78, 0.99, 2),
      (1,    5,    0.12, 0.34, 3),
      (0,    1,    0.50, 0.60, 0),
      (null, null, null, null, null),
      (0,    3,    0.78, 0.99, 2),
      (1,    4,    0.12, 0.34, 3),
      (0,    null, 0.50, 0.60, 0),
      (1,    2,    0.40, 0.50, null),
      (0,    3,    null, 0.99, 2),
      (1,    4,    0.12, 0.34, 3))
      .toDF("col1", "col2", "col3", "col4", "col5")
  }

val dataset = createMockDataset
val cmd = (new CleanMissingData()
      .setInputCols(dataset.columns)
      .setOutputCols(dataset.columns)
      .setCleaningMode("Mean"))
```

</TabItem>
</Tabs>

<DocTable className="CleanMissingData"
py="mmlspark.featurize.html#module-mmlspark.featurize.CleanMissingData"
scala="com/microsoft/ml/spark/featurize/CleanMissingData.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/ml/spark/featurize/CleanMissingData.scala" />


## CountSelector

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from mmlspark.featurize import *
from pyspark.ml.linalg import Vectors

df = spark.createDataFrame([
    (Vectors.sparse(3, [(0, 1.0), (2, 2.0)]), Vectors.dense(1.0, 0.1, 0)),
    (Vectors.sparse(3, [(0, 1.0), (2, 2.0)]), Vectors.dense(1.0, 0.1, 0))
], ["col1", "col2"])

cs = CountSelector().setInputCol("col1").setOutputCol("col3")

display(cs.fit(df).transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.featurize._
import org.apache.spark.ml.linalg.Vectors
import spark.implicits._

val df = Seq(
    (Vectors.sparse(3, Seq((0, 1.0), (2, 2.0))), Vectors.dense(1.0, 0.1, 0)),
    (Vectors.sparse(3, Seq((0, 1.0), (2, 2.0))), Vectors.dense(1.0, 0.1, 0))
  ).toDF("col1", "col2")

val cs = (new CountSelector()
            .setInputCol("col1")
            .setOutputCol("col3"))

display(cs.fit(df).transform(df))
```

</TabItem>
</Tabs>

<DocTable className="CountSelector"
py="mmlspark.featurize.html#module-mmlspark.featurize.CountSelector"
scala="com/microsoft/ml/spark/featurize/CountSelector.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/ml/spark/featurize/CountSelector.scala" />


## Featurize

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from mmlspark.featurize import *

dataset = spark.createDataFrame([
    (0, 2, 0.50, 0.60, "pokemon are everywhere"),
    (1, 3, 0.40, 0.50, "they are in the woods"),
    (0, 4, 0.78, 0.99, "they are in the water"),
    (1, 5, 0.12, 0.34, "they are in the fields"),
    (0, 3, 0.78, 0.99, "pokemon - gotta catch em all")
], ["Label", "col1", "col2", "col3"])

featureColumns = filter(lambda x: x != "Label", dataset.columns)

feat = (Featurize()
      .setNumFeatures(10)
      .setOutputCol("testColumn")
      .setInputCols(list(featureColumns))
      .setOneHotEncodeCategoricals(False))

display(feat.fit(dataset).transform(dataset))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.featurize._
import spark.implicits._

val dataset = Seq(
      (0, 2, 0.50, 0.60, "pokemon are everywhere"),
      (1, 3, 0.40, 0.50, "they are in the woods"),
      (0, 4, 0.78, 0.99, "they are in the water"),
      (1, 5, 0.12, 0.34, "they are in the fields"),
      (0, 3, 0.78, 0.99, "pokemon - gotta catch em all")).toDF("Label", "col1", "col2", "col3")

val featureColumns = dataset.columns.filter(_ != "Label")

val feat = (new Featurize()
      .setNumFeatures(10)
      .setOutputCol("testColumn")
      .setInputCols(featureColumns)
      .setOneHotEncodeCategoricals(false))

display(feat.fit(dataset).transform(dataset))
```

</TabItem>
</Tabs>

<DocTable className="Featurize"
py="mmlspark.featurize.html#module-mmlspark.featurize.Featurize"
scala="com/microsoft/ml/spark/featurize/Featurize.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/ml/spark/featurize/Featurize.scala" />


## ValueIndexer

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from mmlspark.featurize import *

df = spark.createDataFrame([
    (-3, 24, 0.32534, True, "piano"),
    (1, 5, 5.67, False, "piano"),
    (-3, 5, 0.32534, False, "guitar")
], ["int", "long", "double", "bool", "string"])

vi = ValueIndexer().setInputCol("string").setOutputCol("string_cat")

display(vi.fit(df).transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.featurize._
import spark.implicits._

val df = Seq[(Int, Long, Double, Boolean, String)](
    (-3, 24L, 0.32534, true, "piano"),
    (1, 5L, 5.67, false, "piano"),
    (-3, 5L, 0.32534, false, "guitar")).toDF("int", "long", "double", "bool", "string")

val vi = new ValueIndexer().setInputCol("string").setOutputCol("string_cat")

display(vi.fit(df).transform(df))
```

</TabItem>
</Tabs>

<DocTable className="ValueIndexer"
py="mmlspark.featurize.html#module-mmlspark.featurize.ValueIndexer"
scala="com/microsoft/ml/spark/featurize/ValueIndexer.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/ml/spark/featurize/ValueIndexer.scala" />

# Featurize Text

## TextFeaturizer

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from mmlspark.featurize.text import *

dfRaw = spark.createDataFrame([
    (0, "Hi I"),
    (1, "I wish for snow today"),
    (2, "we Cant go to the park, because of the snow!"),
    (3, "")
], ["label", "sentence"])

tfRaw = (TextFeaturizer()
      .setInputCol("sentence")
      .setOutputCol("features")
      .setNumFeatures(20))

display(tfRaw.fit(dfRaw).transform(dfRaw))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.featurize.text._
import spark.implicits._

val dfRaw = Seq((0, "Hi I"),
            (1, "I wish for snow today"),
            (2, "we Cant go to the park, because of the snow!"),
            (3, "")).toDF("label", "sentence")

val tfRaw = (new TextFeaturizer()
      .setInputCol("sentence")
      .setOutputCol("features")
      .setNumFeatures(20))

display(tfRaw.fit(dfRaw).transform(dfRaw))
```

</TabItem>
</Tabs>

<DocTable className="TextFeaturizer"
py="mmlspark.featurize.text.html#module-mmlspark.featurize.text.TextFeaturizer"
scala="com/microsoft/ml/spark/featurize/text/TextFeaturizer.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/ml/spark/featurize/text/TextFeaturizer.scala" />


