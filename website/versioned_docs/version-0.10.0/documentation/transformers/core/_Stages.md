import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";




## Stages

### Cacher

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.stages import *

df = (spark.createDataFrame([
      (0, "guitars", "drums"),
      (1, "piano", "trumpet"),
      (2, "bass", "cymbals"),
      (3, "guitars", "drums"),
      (4, "piano", "trumpet"),
      (5, "bass", "cymbals"),
      (6, "guitars", "drums"),
      (7, "piano", "trumpet"),
      (8, "bass", "cymbals"),
      (9, "guitars", "drums"),
      (10, "piano", "trumpet"),
      (11, "bass", "cymbals")
      ], ["numbers", "words", "more"]))

cacher = Cacher()

cacher.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.stages._

val df = Seq(
      (0, "guitars", "drums"),
      (1, "piano", "trumpet"),
      (2, "bass", "cymbals"),
      (3, "guitars", "drums"),
      (4, "piano", "trumpet"),
      (5, "bass", "cymbals"),
      (6, "guitars", "drums"),
      (7, "piano", "trumpet"),
      (8, "bass", "cymbals"),
      (9, "guitars", "drums"),
      (10, "piano", "trumpet"),
      (11, "bass", "cymbals")
    ).toDF("numbers", "words", "more")

val cacher = new Cacher()

cacher.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="HTTPTransformer"
py="synapse.ml.stages.html#module-synapse.ml.stages.Cacher"
scala="com/microsoft/azure/synapse/ml/stages/Cacher.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/stages/Cacher.scala" />


### DropColumns

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.stages import *

df = (spark.createDataFrame([
      (0, 0, "guitars", "drums", 1, True),
      (1, 1, "piano", "trumpet", 2, False),
      (2, 2, "bass", "cymbals", 3, True)
      ], ["numbers", "doubles", "words", "more", "longs", "booleans"]))

dc = DropColumns().setCols([])

dc.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.stages._

val df = (Seq(
      (0, 0.toDouble, "guitars", "drums", 1.toLong, true),
      (1, 1.toDouble, "piano", "trumpet", 2.toLong, false),
      (2, 2.toDouble, "bass", "cymbals", 3.toLong, true))
      .toDF("numbers", "doubles", "words", "more", "longs", "booleans"))

val dc = new DropColumns().setCols(Array())

dc.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="DropColumns"
py="synapse.ml.stages.html#module-synapse.ml.stages.DropColumns"
scala="com/microsoft/azure/synapse/ml/stages/DropColumns.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/stages/DropColumns.scala" />


### EnsembleByKey

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.stages import *
from pyspark.ml.feature import VectorAssembler

scoreDF = (spark.createDataFrame([
      (0, "foo", 1.0, .1),
      (1, "bar", 4.0, -2.0),
      (1, "bar", 0.0, -3.0)
      ], ["label1", "label2", "score1", "score2"]))

va = VectorAssembler().setInputCols(["score1", "score2"]).setOutputCol("v1")
scoreDF2 = va.transform(scoreDF)

ebk = EnsembleByKey().setKeys(["label1"]).setCols(["score1"])

ebk.transform(scoreDF2).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.stages._
import org.apache.spark.ml.feature.VectorAssembler

val scoreDF = (Seq(
      (0, "foo", 1.0, .1),
      (1, "bar", 4.0, -2.0),
      (1, "bar", 0.0, -3.0))
      .toDF("label1", "label2", "score1", "score2"))

val va = new VectorAssembler().setInputCols(Array("score1", "score2")).setOutputCol("v1")
val scoreDF2 = va.transform(scoreDF)

val ebk = new EnsembleByKey().setKey("label1").setCol("score1")

ebk.transform(scoreDF2).show()
```

</TabItem>
</Tabs>

<DocTable className="EnsembleByKey"
py="synapse.ml.stages.html#module-synapse.ml.stages.EnsembleByKey"
scala="com/microsoft/azure/synapse/ml/stages/EnsembleByKey.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/stages/EnsembleByKey.scala" />


### Explode

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.stages import *

df = (spark.createDataFrame([
      (0, ["guitars", "drums"]),
      (1, ["piano"]),
      (2, [])
      ], ["numbers", "words"]))

explode = Explode().setInputCol("words").setOutputCol("exploded")

explode.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.stages._

val df = (Seq(
    (0, Seq("guitars", "drums")),
    (1, Seq("piano")),
    (2, Seq()))
    .toDF("numbers", "words"))

val explode = new Explode().setInputCol("words").setOutputCol("exploded")

explode.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="Explode"
py="synapse.ml.stages.html#module-synapse.ml.stages.Explode"
scala="com/microsoft/azure/synapse/ml/stages/Explode.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/stages/Explode.scala" />


### Lambda

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.stages import *
from pyspark.sql.types import StringType, StructType

df = (spark.createDataFrame([
      (0, 0.0, "guitars", "drums", 1, True),
      (1, 1.0, "piano", "trumpet", 2, False),
      (2, 2.0, "bass", "cymbals", 3, True)
      ], ["numbers", "doubles", "words", "more", "longs", "booleans"]))

def transformFunc(df):
      return df.select("numbers")

def transformSchemaFunc(schema):
      return StructType([schema("numbers")])

l = (Lambda()
      .setTransformFunc(transformFunc)
      .setTransformSchemaFunc(transformSchemaFunc))

```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.stages._
import org.apache.spark.sql.types.{StringType, StructType}

val df = (Seq(
      (0, 0.toDouble, "guitars", "drums", 1.toLong, true),
      (1, 1.toDouble, "piano", "trumpet", 2.toLong, false),
      (2, 2.toDouble, "bass", "cymbals", 3.toLong, true))
      .toDF("numbers", "doubles", "words", "more", "longs", "booleans"))

val lambda = (new Lambda()
      .setTransform(df => df.select("numbers"))
      .setTransformSchema(schema => new StructType(Array(schema("numbers")))))

lambda.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="Lambda"
py="synapse.ml.stages.html#module-synapse.ml.stages.Lambda"
scala="com/microsoft/azure/synapse/ml/stages/Lambda.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/stages/Lambda.scala" />


### DynamicMiniBatchTransformer

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.stages import *
from pyspark.sql.types import StringType, StructType

df = (spark.createDataFrame([(_, "foo") for _ in range(1, 11)], ["in1", "in2"]))

dmbt = DynamicMiniBatchTransformer()

dmbt.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.stages._

val df = (1 until 11).map(x => (x, "foo")).toDF("in1", "in2")

val dmbt = new DynamicMiniBatchTransformer()

dmbt.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="DynamicMiniBatchTransformer"
py="synapse.ml.stages.html#module-synapse.ml.stages.DynamicMiniBatchTransformer"
scala="com/microsoft/azure/synapse/ml/stages/DynamicMiniBatchTransformer.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/stages/MiniBatchTransformer.scala" />


### FixedMiniBatchTransformer

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.stages import *

fmbt = (FixedMiniBatchTransformer()
      .setBuffered(True)
      .setBatchSize(3))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.stages._

val fmbt = (new FixedMiniBatchTransformer()
      .setBuffered(true)
      .setBatchSize(3))
```

</TabItem>
</Tabs>

<DocTable className="FixedMiniBatchTransformer"
py="synapse.ml.stages.html#module-synapse.ml.stages.FixedMiniBatchTransformer"
scala="com/microsoft/azure/synapse/ml/stages/FixedMiniBatchTransformer.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/stages/MiniBatchTransformer.scala" />


### TimeIntervalMiniBatchTransformer

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.stages import *

df = (spark.createDataFrame([(_, "foo") for _ in range(1, 11)], ["in1", "in2"]))

timbt = (TimeIntervalMiniBatchTransformer()
        .setMillisToWait(1000)
        .setMaxBatchSize(30))

timbt.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.stages._

val df = (1 until 11).map(x => (x, "foo")).toDF("in1", "in2")

val timbt = (new TimeIntervalMiniBatchTransformer()
        .setMillisToWait(1000)
        .setMaxBatchSize(30))

timbt.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="TimeIntervalMiniBatchTransformer"
py="synapse.ml.stages.html#module-synapse.ml.stages.TimeIntervalMiniBatchTransformer"
scala="com/microsoft/azure/synapse/ml/stages/TimeIntervalMiniBatchTransformer.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/stages/MiniBatchTransformer.scala" />


### FlattenBatch

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.stages import *

df = (spark.createDataFrame([(_, "foo") for _ in range(1, 11)], ["in1", "in2"]))

transDF = DynamicMiniBatchTransformer().transform(df)

fb = FlattenBatch()

fb.transform(transDF).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.stages._

val df = (1 until 11).map(x => (x, "foo")).toDF("in1", "in2")

val transDF = new DynamicMiniBatchTransformer().transform(df)

val fb = new FlattenBatch()

fb.transform(transDF).show()
```

</TabItem>
</Tabs>

<DocTable className="FlattenBatch"
py="synapse.ml.stages.html#module-synapse.ml.stages.FlattenBatch"
scala="com/microsoft/azure/synapse/ml/stages/FlattenBatch.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/stages/MiniBatchTransformer.scala" />


### RenameColumn

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.stages import *

df = (spark.createDataFrame([
      (0, 0, "guitars", "drums", 1, True),
      (1, 1, "piano", "trumpet", 2, False),
      (2, 2, "bass", "cymbals", 3, True)
], ["numbers", "doubles", "words", "more", "longs", "booleans"]))

rc = RenameColumn().setInputCol("words").setOutputCol("numbers")

rc.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.stages._

val df = (Seq(
      (0, 0.toDouble, "guitars", "drums", 1.toLong, true),
      (1, 1.toDouble, "piano", "trumpet", 2.toLong, false),
      (2, 2.toDouble, "bass", "cymbals", 3.toLong, true))
      .toDF("numbers", "doubles", "words", "more", "longs", "booleans"))

val rc = new RenameColumn().setInputCol("words").setOutputCol("numbers")

rc.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="RenameColumn"
py="synapse.ml.stages.html#module-synapse.ml.stages.RenameColumn"
scala="com/microsoft/azure/synapse/ml/stages/RenameColumn.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/stages/RenameColumn.scala" />


### Repartition

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.stages import *

df = (spark.createDataFrame([
      (0, "guitars", "drums"),
      (1, "piano", "trumpet"),
      (2, "bass", "cymbals"),
      (3, "guitars", "drums"),
      (4, "piano", "trumpet"),
      (5, "bass", "cymbals"),
      (6, "guitars", "drums"),
      (7, "piano", "trumpet"),
      (8, "bass", "cymbals"),
      (9, "guitars", "drums"),
      (10, "piano", "trumpet"),
      (11, "bass", "cymbals")
], ["numbers", "words", "more"]))

repartition = Repartition().setN(1)

repartition.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.stages._

val df = (Seq(
    (0, "guitars", "drums"),
    (1, "piano", "trumpet"),
    (2, "bass", "cymbals"),
    (3, "guitars", "drums"),
    (4, "piano", "trumpet"),
    (5, "bass", "cymbals"),
    (6, "guitars", "drums"),
    (7, "piano", "trumpet"),
    (8, "bass", "cymbals"),
    (9, "guitars", "drums"),
    (10, "piano", "trumpet"),
    (11, "bass", "cymbals")
  ).toDF("numbers", "words", "more"))

val repartition = new Repartition().setN(1)

repartition.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="Repartition"
py="synapse.ml.stages.html#module-synapse.ml.stages.Repartition"
scala="com/microsoft/azure/synapse/ml/stages/Repartition.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/stages/Repartition.scala" />


### SelectColumns

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.stages import *

df = (spark.createDataFrame([
      (0, 0.0, "guitars", "drums", 1, True),
      (1, 1.0, "piano", "trumpet", 2, False),
      (2, 2.0, "bass", "cymbals", 3, True)
], ["numbers", "words", "more"]))

sc = SelectColumns().setCols(["words", "more"])

sc.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.stages._

val df = (Seq(
      (0, 0.toDouble, "guitars", "drums", 1.toLong, true),
      (1, 1.toDouble, "piano", "trumpet", 2.toLong, false),
      (2, 2.toDouble, "bass", "cymbals", 3.toLong, true))
      .toDF("numbers", "doubles", "words", "more", "longs", "booleans"))

val sc = new SelectColumns().setCols(Array("words", "more"))

sc.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="SelectColumns"
py="synapse.ml.stages.html#module-synapse.ml.stages.SelectColumns"
scala="com/microsoft/azure/synapse/ml/stages/SelectColumns.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/stages/SelectColumns.scala" />


### StratifiedRepartition

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.stages import *

df = (spark.createDataFrame([
      (0, "Blue", 2),
      (0, "Red", 2),
      (0, "Green", 2),
      (1, "Purple", 2),
      (1, "Orange", 2),
      (1, "Indigo", 2),
      (2, "Violet", 2),
      (2, "Black", 2),
      (2, "White", 2),
      (3, "Gray", 2),
      (3, "Yellow", 2),
      (3, "Cerulean", 2)
], ["values", "colors", "const"]))

sr = StratifiedRepartition().setLabelCol("values").setMode("equal")
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.stages._

val df = (Seq(
    (0, "Blue", 2),
    (0, "Red", 2),
    (0, "Green", 2),
    (1, "Purple", 2),
    (1, "Orange", 2),
    (1, "Indigo", 2),
    (2, "Violet", 2),
    (2, "Black", 2),
    (2, "White", 2),
    (3, "Gray", 2),
    (3, "Yellow", 2),
    (3, "Cerulean", 2)
  ).toDF("values", "colors", "const"))

val sr = new StratifiedRepartition().setLabelCol("values").setMode("equal")

sr.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="StratifiedRepartition"
py="synapse.ml.stages.html#module-synapse.ml.stages.StratifiedRepartition"
scala="com/microsoft/azure/synapse/ml/stages/StratifiedRepartition.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/stages/StratifiedRepartition.scala" />


### SummarizeData

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.stages import *

df = (spark.createDataFrame([
      (0, 0.0, "guitars", "drums", 1, True),
      (1, 1.0, "piano", "trumpet", 2, False),
      (2, 2.0, "bass", "cymbals", 3, True)
], ["numbers", "doubles", "words", "more", "longs", "booleans"]))

summary = SummarizeData()

summary.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.stages._

val df = (Seq(
      (0, 0.toDouble, "guitars", "drums", 1.toLong, true),
      (1, 1.toDouble, "piano", "trumpet", 2.toLong, false),
      (2, 2.toDouble, "bass", "cymbals", 3.toLong, true))
      .toDF("numbers", "doubles", "words", "more", "longs", "booleans"))

val summary = new SummarizeData()

summary.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="SummarizeData"
py="synapse.ml.stages.html#module-synapse.ml.stages.SummarizeData"
scala="com/microsoft/azure/synapse/ml/stages/SummarizeData.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/stages/SummarizeData.scala" />


### TextPreprocessor

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.stages import *

df = (spark.createDataFrame([
      ("The happy sad boy drank sap", ),
      ("The hater sad doy drank sap", ),
      ("foo", ),
      ("The hater sad doy aABc0123456789Zz_", )
], ["words1"]))

testMap = {"happy": "sad", "hater": "sap",
      "sad": "sap", "sad doy": "sap"}

textPreprocessor = (TextPreprocessor()
      .setNormFunc("lowerCase")
      .setMap(testMap)
      .setInputCol("words1")
      .setOutputCol("out"))

textPreprocessor.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.stages._

val df = (Seq(
    ("The happy sad boy drank sap", ),
    ("The hater sad doy drank sap", ),
    ("foo", ),
    ("The hater sad doy aABc0123456789Zz_", ))
    .toDF("words1"))

val testMap = Map[String, String] (
    "happy"   -> "sad",
    "hater"   -> "sap",
    "sad"     -> "sap",
    "sad doy" -> "sap"
  )

val textPreprocessor = (new TextPreprocessor()
      .setNormFunc("lowerCase")
      .setMap(testMap)
      .setInputCol("words1")
      .setOutputCol("out"))

textPreprocessor.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="TextPreprocessor"
py="synapse.ml.stages.html#module-synapse.ml.stages.TextPreprocessor"
scala="com/microsoft/azure/synapse/ml/stages/TextPreprocessor.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/stages/TextPreprocessor.scala" />


### UDFTransformer

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.stages import *
from pyspark.sql.functions import udf

df = (spark.createDataFrame([
      (0, 0.0, "guitars", "drums", 1, True),
      (1, 1.0, "piano", "trumpet", 2, False),
      (2, 2.0, "bass", "cymbals", 3, True)
], ["numbers", "doubles", "words", "more", "longs", "booleans"]))

stringToIntegerUDF = udf(lambda x: 1)

udfTransformer = (UDFTransformer()
      .setUDF(stringToIntegerUDF)
      .setInputCol("numbers")
      .setOutputCol("out"))

udfTransformer.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.stages._
import org.apache.spark.sql.functions.udf

val df = (Seq(
      (0, 0.toDouble, "guitars", "drums", 1.toLong, true),
      (1, 1.toDouble, "piano", "trumpet", 2.toLong, false),
      (2, 2.toDouble, "bass", "cymbals", 3.toLong, true))
      .toDF("numbers", "doubles", "words", "more", "longs", "booleans"))

val stringToIntegerUDF = udf((_: String) => 1)

val udfTransformer = (new UDFTransformer()
      .setUDF(stringToIntegerUDF)
      .setInputCol("numbers")
      .setOutputCol("out"))

udfTransformer.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="UDFTransformer"
py="synapse.ml.stages.html#module-synapse.ml.stages.UDFTransformer"
scala="com/microsoft/azure/synapse/ml/stages/UDFTransformer.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/stages/UDFTransformer.scala" />


### UnicodeNormalize

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.stages import *

df = (spark.createDataFrame([
      ("Schön", 1),
      ("Scho\u0308n", 1),
      (None, 1)
], ["words1", "dummy"]))

unicodeNormalize = (UnicodeNormalize()
      .setForm("NFC")
      .setInputCol("words1")
      .setOutputCol("norm1"))

unicodeNormalize.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.stages._

val df = (Seq(
    ("Schön", 1),
    ("Scho\u0308n", 1),
    (null, 1))
    .toDF("words1", "dummy"))

val unicodeNormalize = (new UnicodeNormalize()
      .setForm("NFC")
      .setInputCol("words1")
      .setOutputCol("norm1"))

unicodeNormalize.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="UnicodeNormalize"
py="synapse.ml.stages.html#module-synapse.ml.stages.UnicodeNormalize"
scala="com/microsoft/azure/synapse/ml/stages/UnicodeNormalize.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/stages/UnicodeNormalize.scala" />
