import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";


## Featurize

### CleanMissingData

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.featurize import *

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
import com.microsoft.azure.synapse.ml.featurize._
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
py="synapse.ml.featurize.html#module-synapse.ml.featurize.CleanMissingData"
scala="com/microsoft/azure/synapse/ml/featurize/CleanMissingData.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/featurize/CleanMissingData.scala" />


### CountSelector

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.featurize import *
from pyspark.ml.linalg import Vectors

df = spark.createDataFrame([
    (Vectors.sparse(3, [(0, 1.0), (2, 2.0)]), Vectors.dense(1.0, 0.1, 0)),
    (Vectors.sparse(3, [(0, 1.0), (2, 2.0)]), Vectors.dense(1.0, 0.1, 0))
], ["col1", "col2"])

cs = CountSelector().setInputCol("col1").setOutputCol("col3")

cs.fit(df).transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.featurize._
import org.apache.spark.ml.linalg.Vectors
import spark.implicits._

val df = Seq(
    (Vectors.sparse(3, Seq((0, 1.0), (2, 2.0))), Vectors.dense(1.0, 0.1, 0)),
    (Vectors.sparse(3, Seq((0, 1.0), (2, 2.0))), Vectors.dense(1.0, 0.1, 0))
  ).toDF("col1", "col2")

val cs = (new CountSelector()
            .setInputCol("col1")
            .setOutputCol("col3"))

cs.fit(df).transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="CountSelector"
py="synapse.ml.featurize.html#module-synapse.ml.featurize.CountSelector"
scala="com/microsoft/azure/synapse/ml/featurize/CountSelector.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/featurize/CountSelector.scala" />


### Featurize

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.featurize import *

dataset = spark.createDataFrame([
    (0, 2, 0.50, 0.60, "pokemon are everywhere"),
    (1, 3, 0.40, 0.50, "they are in the woods"),
    (0, 4, 0.78, 0.99, "they are in the water"),
    (1, 5, 0.12, 0.34, "they are in the fields"),
    (0, 3, 0.78, 0.99, "pokemon - gotta catch em all")
], ["Label", "col1", "col2", "col3"])

feat = (Featurize()
      .setNumFeatures(10)
      .setOutputCol("testColumn")
      .setInputCols(["col1", "col2", "col3"])
      .setOneHotEncodeCategoricals(False))

feat.fit(dataset).transform(dataset).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.featurize._
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

feat.fit(dataset).transform(dataset).show()
```

</TabItem>
</Tabs>

<DocTable className="Featurize"
py="synapse.ml.featurize.html#module-synapse.ml.featurize.Featurize"
scala="com/microsoft/azure/synapse/ml/featurize/Featurize.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/featurize/Featurize.scala" />


### ValueIndexer

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.featurize import *

df = spark.createDataFrame([
    (-3, 24, 0.32534, True, "piano"),
    (1, 5, 5.67, False, "piano"),
    (-3, 5, 0.32534, False, "guitar")
], ["int", "long", "double", "bool", "string"])

vi = ValueIndexer().setInputCol("string").setOutputCol("string_cat")

vi.fit(df).transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.featurize._
import spark.implicits._

val df = Seq[(Int, Long, Double, Boolean, String)](
    (-3, 24L, 0.32534, true, "piano"),
    (1, 5L, 5.67, false, "piano"),
    (-3, 5L, 0.32534, false, "guitar")).toDF("int", "long", "double", "bool", "string")

val vi = new ValueIndexer().setInputCol("string").setOutputCol("string_cat")

vi.fit(df).transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="ValueIndexer"
py="synapse.ml.featurize.html#module-synapse.ml.featurize.ValueIndexer"
scala="com/microsoft/azure/synapse/ml/featurize/ValueIndexer.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/featurize/ValueIndexer.scala" />

## Featurize Text

### TextFeaturizer

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.featurize.text import *

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

tfRaw.fit(dfRaw).transform(dfRaw).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.featurize.text._
import spark.implicits._

val dfRaw = Seq((0, "Hi I"),
            (1, "I wish for snow today"),
            (2, "we Cant go to the park, because of the snow!"),
            (3, "")).toDF("label", "sentence")

val tfRaw = (new TextFeaturizer()
      .setInputCol("sentence")
      .setOutputCol("features")
      .setNumFeatures(20))

tfRaw.fit(dfRaw).transform(dfRaw).show()
```

</TabItem>
</Tabs>

<DocTable className="TextFeaturizer"
py="synapse.ml.featurize.text.html#module-synapse.ml.featurize.text.TextFeaturizer"
scala="com/microsoft/azure/synapse/ml/featurize/text/TextFeaturizer.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/featurize/text/TextFeaturizer.scala" />
