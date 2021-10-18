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

## DataConversion

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
    (True, 1, 2, 3, 4, 5.0, 6.0, "7", "8.0"),
    (False, 9, 10, 11, 12, 14.5, 15.5, "16", "17.456"),
    (True, -127, 345, 666, 1234, 18.91, 20.21, "100", "200.12345")
], ["bool", "byte", "short", "int", "long", "float", "double", "intstring", "doublestring"])

dc = (DataConversion()
        .setCols(["byte"])
        .setConvertTo("boolean"))

display(dc.transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.featurize._
import spark.implicits._

val df = Seq(
    (true: Boolean, 1: Byte, 2: Short, 3: Integer, 4: Long, 5.0F, 6.0, "7", "8.0"),
    (false, 9: Byte, 10: Short, 11: Integer, 12: Long, 14.5F, 15.5, "16", "17.456"),
    (true, -127: Byte, 345: Short, Short.MaxValue + 100, (Int.MaxValue).toLong + 100, 18.91F, 20.21, "100", "200.12345"))
    .toDF("bool", "byte", "short", "int", "long", "float", "double", "intstring", "doublestring")

val dc = (new DataConversion()
        .setCols(Array("byte"))
        .setConvertTo("boolean"))

display(dc.transform(df))
```

</TabItem>
</Tabs>

<DocTable className="DataConversion"
py="synapse.ml.featurize.html#module-synapse.ml.featurize.DataConversion"
scala="com/microsoft/azure/synapse/ml/featurize/DataConversion.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/featurize/DataConversion.scala" />


## IndexToValue

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

df2 = ValueIndexer().setInputCol("string").setOutputCol("string_cat").fit(df).transform(df)

itv = (IndexToValue()
        .setInputCol("string_cat")
        .setOutputCol("string_noncat"))

display(itv.transform(df2))
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

val df2 = new ValueIndexer().setInputCol("string").setOutputCol("string_cat").fit(df).transform(df)

val itv = (new IndexToValue()
        .setInputCol("string_cat")
        .setOutputCol("string_noncat"))

display(itv.transform(df2))
```

</TabItem>
</Tabs>

<DocTable className="IndexToValue"
py="synapse.ml.featurize.html#module-synapse.ml.featurize.IndexToValue"
scala="com/microsoft/azure/synapse/ml/featurize/IndexToValue.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/featurize/IndexToValue.scala" />


# Featurize Text

## MultiNGram

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
from pyspark.ml.feature import Tokenizer

dfRaw = spark.createDataFrame([
    (0, "Hi I"),
    (1, "I wish for snow today"),
    (2, "we Cant go to the park, because of the snow!"),
    (3, ""),
    (4, "1 2 3 4 5 6 7 8 9")
], ["label", "sentence"])

dfTok = (Tokenizer()
    .setInputCol("sentence")
    .setOutputCol("tokens")
    .transform(dfRaw))

mng = (MultiNGram()
    .setLengths([1, 3, 4])
    .setInputCol("tokens")
    .setOutputCol("ngrams"))

display(mng.transform(dfTok))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.featurize.text._
import org.apache.spark.ml.feature.Tokenizer
import spark.implicits._

val dfRaw = (Seq(
    (0, "Hi I"),
    (1, "I wish for snow today"),
    (2, "we Cant go to the park, because of the snow!"),
    (3, ""),
    (4, (1 to 10).map(_.toString).mkString(" ")))
    .toDF("label", "sentence"))

val dfTok = (new Tokenizer()
    .setInputCol("sentence")
    .setOutputCol("tokens")
    .transform(dfRaw))

val mng = (new MultiNGram()
    .setLengths(Array(1, 3, 4))
    .setInputCol("tokens")
    .setOutputCol("ngrams"))

display(mng.transform(dfTok))
```

</TabItem>
</Tabs>

<DocTable className="MultiNGram"
py="synapse.ml.featurize.text.html#module-synapse.ml.featurize.text.MultiNGram"
scala="com/microsoft/azure/synapse/ml/featurize/text/MultiNGram.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/featurize/text/MultiNGram.scala" />


## PageSplitter

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

df = spark.createDataFrame([
    ("words words  words     wornssaa ehewjkdiw weijnsikjn xnh", ),
    ("s s  s   s     s           s", ),
    ("hsjbhjhnskjhndwjnbvckjbnwkjwenbvfkjhbnwevkjhbnwejhkbnvjkhnbndjkbnd", ),
    ("hsjbhjhnskjhndwjnbvckjbnwkjwenbvfkjhbnwevkjhbnwejhkbnvjkhnbndjkbnd 190872340870271091309831097813097130i3u709781", ),
    ("", ),
    (None, )
], ["text"])

ps = (PageSplitter()
    .setInputCol("text")
    .setMaximumPageLength(20)
    .setMinimumPageLength(10)
    .setOutputCol("pages"))

display(ps.transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.featurize.text._
import spark.implicits._

val df = Seq(
    "words words  words     wornssaa ehewjkdiw weijnsikjn xnh",
    "s s  s   s     s           s",
    "hsjbhjhnskjhndwjnbvckjbnwkjwenbvfkjhbnwevkjhbnwejhkbnvjkhnbndjkbnd",
    "hsjbhjhnskjhndwjnbvckjbnwkjwenbvfkjhbnwevkjhbnwejhkbnvjkhnbndjkbnd " +
      "190872340870271091309831097813097130i3u709781",
    "",
    null
  ).toDF("text")

val ps = (new PageSplitter()
    .setInputCol("text")
    .setMaximumPageLength(20)
    .setMinimumPageLength(10)
    .setOutputCol("pages"))

display(ps.transform(df))
```

</TabItem>
</Tabs>

<DocTable className="PageSplitter"
py="synapse.ml.featurize.text.html#module-synapse.ml.featurize.text.PageSplitter"
scala="com/microsoft/azure/synapse/ml/featurize/text/PageSplitter.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/featurize/text/PageSplitter.scala" />


