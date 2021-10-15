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
        .config("spark.jars.packages", "com.microsoft.azure:synapseml:0.9.0")
        .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven")
        .getOrCreate())

def getSecret(secretName):
        get_secret_cmd = 'az keyvault secret show --vault-name mmlspark-build-keys --name {}'.format(secretName)
        value = json.loads(os.popen(get_secret_cmd).read())["value"]
        return value

import synapse.ml
```
-->

## ClassBalancer

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
      (0, 1.0, "Hi I"),
      (1, 1.0, "I wish for snow today"),
      (2, 2.0, "I wish for snow today"),
      (3, 2.0, "I wish for snow today"),
      (4, 2.0, "I wish for snow today"),
      (5, 2.0, "I wish for snow today"),
      (6, 0.0, "I wish for snow today"),
      (7, 1.0, "I wish for snow today"),
      (8, 0.0, "we Cant go to the park, because of the snow!"),
      (9, 2.0, "")
      ], ["index", "label", "sentence"]))

cb = ClassBalancer().setInputCol("label")

display(cb.fit(df).transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.stages._

val df = Seq(
      (0, 1.0, "Hi I"),
      (1, 1.0, "I wish for snow today"),
      (2, 2.0, "I wish for snow today"),
      (3, 2.0, "I wish for snow today"),
      (4, 2.0, "I wish for snow today"),
      (5, 2.0, "I wish for snow today"),
      (6, 0.0, "I wish for snow today"),
      (7, 1.0, "I wish for snow today"),
      (8, 0.0, "we Cant go to the park, because of the snow!"),
      (9, 2.0, "")).toDF("index", "label", "sentence")

val cb = new ClassBalancer().setInputCol("label")

display(cb.fit(df).transform(df))
```

</TabItem>
</Tabs>

<DocTable className="ClassBalancer"
py="mmlspark.stages.html#module-mmlspark.stages.ClassBalancer"
scala="com/microsoft/ml/spark/stages/ClassBalancer.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/stages/ClassBalancer.scala" />


## MultiColumnAdapter

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
from pyspark.ml.feature import Tokenizer

df = (spark.createDataFrame([
        (0, "This is a test", "this is one too"),
        (1, "could be a test", "bar"),
        (2, "foo", "bar"),
        (3, "foo", "maybe not")
      ], ["label", "words1", "words2"]))

stage1 = Tokenizer()
mca = (MultiColumnAdapter()
        .setBaseStage(stage1)
        .setInputCols(["words1",  "words2"])
        .setOutputCols(["output1", "output2"]))

display(mca.fit(df).transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.stages._
import org.apache.spark.ml.feature.Tokenizer

val df = (Seq(
    (0, "This is a test", "this is one too"),
    (1, "could be a test", "bar"),
    (2, "foo", "bar"),
    (3, "foo", "maybe not"))
    .toDF("label", "words1", "words2"))

val stage1 = new Tokenizer()
val mca = (new MultiColumnAdapter()
        .setBaseStage(stage1)
        .setInputCols(Array[String]("words1",  "words2"))
        .setOutputCols(Array[String]("output1", "output2")))

display(mca.fit(df).transform(df))
```

</TabItem>
</Tabs>

<DocTable className="MultiColumnAdapter"
py="mmlspark.stages.html#module-mmlspark.stages.MultiColumnAdapter"
scala="com/microsoft/ml/spark/stages/MultiColumnAdapter.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/stages/MultiColumnAdapter.scala" />


## Timer

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
from pyspark.ml.feature import *

df = (spark.createDataFrame([
        (0, "Hi I"),
        (1, "I wish for snow today"),
        (2, "we Cant go to the park, because of the snow!"),
        (3, "")
      ], ["label", "sentence"]))

tok = (Tokenizer()
      .setInputCol("sentence")
      .setOutputCol("tokens"))

df2 = Timer().setStage(tok).fit(df).transform(df)

df3 = HashingTF().setInputCol("tokens").setOutputCol("hash").transform(df2)

idf = IDF().setInputCol("hash").setOutputCol("idf")
timer = Timer().setStage(idf)

display(timer.fit(df3).transform(df3))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.stages._
import org.apache.spark.ml.feature._

val df = (Seq(
    (0, "Hi I"),
    (1, "I wish for snow today"),
    (2, "we Cant go to the park, because of the snow!"),
    (3, "")
  ).toDF("label", "sentence"))

val tok = (new Tokenizer()
      .setInputCol("sentence")
      .setOutputCol("tokens"))

val df2 = new Timer().setStage(tok).fit(df).transform(df)

val df3 = new HashingTF().setInputCol("tokens").setOutputCol("hash").transform(df2)

val idf = new IDF().setInputCol("hash").setOutputCol("idf")
val timer = new Timer().setStage(idf)

display(timer.fit(df3).transform(df3))
```

</TabItem>
</Tabs>

<DocTable className="Timer"
py="mmlspark.stages.html#module-mmlspark.stages.Timer"
scala="com/microsoft/ml/spark/stages/Timer.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/stages/Timer.scala" />




