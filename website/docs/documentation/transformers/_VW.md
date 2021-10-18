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

## VectorZipper

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.vw import *

df = spark.createDataFrame([
      ("action1_f", "action2_f"),
      ("action1_f", "action2_f"),
      ("action1_f", "action2_f"),
      ("action1_f", "action2_f")
], ["action1", "action2"])

actionOneFeaturizer = (VowpalWabbitFeaturizer()
    .setInputCols(["action1"])
    .setOutputCol("sequence_one"))

actionTwoFeaturizer = (VowpalWabbitFeaturizer()
    .setInputCols(["action2"])
    .setOutputCol("sequence_two"))

seqDF = actionTwoFeaturizer.transform(actionOneFeaturizer.transform(df))

vectorZipper = (VectorZipper()
    .setInputCols(["sequence_one", "sequence_two"])
    .setOutputCol("out"))

display(vectorZipper.transform(seqDF))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.vw._

val df = (Seq(
      ("action1_f", "action2_f"),
      ("action1_f", "action2_f"),
      ("action1_f", "action2_f"),
      ("action1_f", "action2_f")
    ).toDF("action1", "action2"))

val actionOneFeaturizer = (new VowpalWabbitFeaturizer()
    .setInputCols(Array("action1"))
    .setOutputCol("sequence_one"))

val actionTwoFeaturizer = (new VowpalWabbitFeaturizer()
    .setInputCols(Array("action2"))
    .setOutputCol("sequence_two"))

val seqDF = actionTwoFeaturizer.transform(actionOneFeaturizer.transform(df))

val vectorZipper = (new VectorZipper()
    .setInputCols(Array("sequence_one", "sequence_two"))
    .setOutputCol("out"))

display(vectorZipper.transform(seqDF))
```

</TabItem>
</Tabs>

<DocTable className="VectorZipper"
py="mmlspark.vw.html#module-mmlspark.vw.VectorZipper"
scala="com/microsoft/ml/spark/vw/VectorZipper.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/vw/src/main/scala/com/microsoft/azure/synapse/ml/vw/VectorZipper.scala" />


## VowpalWabbitClassifier

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.vw import *

vw = (VowpalWabbitClassifier()
      .setNumBits(10)
      .setLearningRate(3.1)
      .setPowerT(0)
      .setLabelConversion(False))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.vw._

val vw = (new VowpalWabbitClassifier()
      .setNumBits(10)
      .setLearningRate(3.1)
      .setPowerT(0)
      .setLabelConversion(false))
```

</TabItem>
</Tabs>

<DocTable className="VowpalWabbitClassifier"
py="mmlspark.vw.html#module-mmlspark.vw.VowpalWabbitClassifier"
scala="com/microsoft/ml/spark/vw/VowpalWabbitClassifier.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/vw/src/main/scala/com/microsoft/azure/synapse/ml/vw/VowpalWabbitClassifier.scala" />


## VowpalWabbitFeaturizer

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.vw import *

featurizer = (VowpalWabbitFeaturizer()
      .setStringSplitInputCols(["in"])
      .setPreserveOrderNumBits(2)
      .setNumBits(18)
      .setPrefixStringsWithColumnName(False)
      .setOutputCol("features"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.vw._

val featurizer = (new VowpalWabbitFeaturizer()
      .setStringSplitInputCols(Array("in"))
      .setPreserveOrderNumBits(2)
      .setNumBits(18)
      .setPrefixStringsWithColumnName(false)
      .setOutputCol("features"))
```

</TabItem>
</Tabs>

<DocTable className="VowpalWabbitFeaturizer"
py="mmlspark.vw.html#module-mmlspark.vw.VowpalWabbitFeaturizer"
scala="com/microsoft/ml/spark/vw/VowpalWabbitFeaturizer.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/vw/src/main/scala/com/microsoft/azure/synapse/ml/vw/VowpalWabbitFeaturizer.scala" />


## VowpalWabbitInteractions

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.vw import *

interactions = (VowpalWabbitInteractions()
    .setInputCols(["v1"])
    .setOutputCol("out"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.vw._
import org.apache.spark.ml.linalg._

case class Data(v1: Vector, v2: Vector, v3: Vector)

val df = spark.createDataFrame(Seq(Data(
  Vectors.dense(Array(1.0, 2.0, 3.0)),
  Vectors.sparse(8, Array(5), Array(4.0)),
  Vectors.sparse(11, Array(8, 9), Array(7.0, 8.0))
)))

val interactions = (new VowpalWabbitInteractions()
    .setInputCols(Array("v1"))
    .setOutputCol("out"))

display(interactions.transform(df))
```

</TabItem>
</Tabs>

<DocTable className="VowpalWabbitInteractions"
py="mmlspark.vw.html#module-mmlspark.vw.VowpalWabbitInteractions"
scala="com/microsoft/ml/spark/vw/VowpalWabbitInteractions.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/vw/src/main/scala/com/microsoft/azure/synapse/ml/vw/VowpalWabbitInteractions.scala" />




