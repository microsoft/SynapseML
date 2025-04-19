import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";




## VectorZipper

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
{label: `.NET`, value: `csharp`},
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

vectorZipper.transform(seqDF).show()
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

vectorZipper.transform(seqDF).show()
```

</TabItem>
<TabItem value="csharp">

```csharp
using System;
using System.Collections.Generic;
using Synapse.ML.Vw;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace SynapseMLApp
{
    class Program
    {
        static void Main(string[] args)
        {
            SparkSession spark =
                SparkSession
                    .Builder()
                    .AppName("Example")
                    .GetOrCreate();

            DataFrame df = spark.CreateDataFrame(
                new List<GenericRow>
                {
                    new GenericRow(new object[] {"action1_f", "action2_f"}),
                    new GenericRow(new object[] {"action1_f", "action2_f"}),
                    new GenericRow(new object[] {"action1_f", "action2_f"}),
                    new GenericRow(new object[] {"action1_f", "action2_f"})
                },
                new StructType(new List<StructField>
                {
                    new StructField("action1", new StringType()),
                    new StructField("action2", new StringType())
                })
            );

            var actionOneFeaturizer = new VowpalWabbitFeaturizer()
                .SetInputCols(new string[]{"action1"})
                .SetOutputCol("sequence_one");
            var actionTwoFeaturizer = new VowpalWabbitFeaturizer()
                .SetInputCols(new string[]{"action2"})
                .SetOutputCol("sequence_two");
            var seqDF = actionTwoFeaturizer.Transform(actionOneFeaturizer.Transform(df));

            var vectorZipper = new VectorZipper()
                .SetInputCols(new string[]{"sequence_one", "sequence_two"})
                .SetOutputCol("out");
            vectorZipper.Transform(seqDF).Show();

            spark.Stop();
        }
    }
}
```

</TabItem>
</Tabs>

<DocTable className="VectorZipper"
py="synapse.ml.vw.html#module-synapse.ml.vw.VectorZipper"
scala="com/microsoft/azure/synapse/ml/vw/VectorZipper.html"
csharp="classSynapse_1_1ML_1_1Vw_1_1VectorZipper.html"
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
py="synapse.ml.vw.html#module-synapse.ml.vw.VowpalWabbitClassifier"
scala="com/microsoft/azure/synapse/ml/vw/VowpalWabbitClassifier.html"
csharp="classSynapse_1_1ML_1_1Vw_1_1VowpalWabbitClassifier.html"
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
py="synapse.ml.vw.html#module-synapse.ml.vw.VowpalWabbitFeaturizer"
scala="com/microsoft/azure/synapse/ml/vw/VowpalWabbitFeaturizer.html"
csharp="classSynapse_1_1ML_1_1Vw_1_1VowpalWabbitFeaturizer.html"
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

interactions.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="VowpalWabbitInteractions"
py="synapse.ml.vw.html#module-synapse.ml.vw.VowpalWabbitInteractions"
scala="com/microsoft/azure/synapse/ml/vw/VowpalWabbitInteractions.html"
csharp="classSynapse_1_1ML_1_1Vw_1_1VowpalWabbitInteractions.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/vw/src/main/scala/com/microsoft/azure/synapse/ml/vw/VowpalWabbitInteractions.scala" />
