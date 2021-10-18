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

## Explainers

### ImageLIME

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.explainers import *
from synapse.ml.onnx import ONNXModel

model = ONNXModel()

lime = (ImageLIME()
    .setModel(model)
    .setOutputCol("weights")
    .setInputCol("image")
    .setCellSize(150.0)
    .setModifier(50.0)
    .setNumSamples(500)
    .setTargetCol("probability")
    .setTargetClassesCol("top2pred")
    .setSamplingFraction(0.7))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.explainers._
import com.microsoft.azure.synapse.ml.onnx._
import spark.implicits._

val model = (new ONNXModel())

val lime = (new ImageLIME()
    .setModel(model)
    .setOutputCol("weights")
    .setInputCol("image")
    .setCellSize(150.0)
    .setModifier(50.0)
    .setNumSamples(500)
    .setTargetCol("probability")
    .setTargetClassesCol("top2pred")
    .setSamplingFraction(0.7))
```

</TabItem>
</Tabs>

<DocTable className="ImageLIME"
py="synapse.ml.explainers.html#module-synapse.ml.explainers.ImageLIME"
scala="com/microsoft/azure/synapse/ml/explainers/ImageLIME.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/explainers/ImageLIME.scala" />


### ImageSHAP

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.explainers import *
from synapse.ml.onnx import ONNXModel

model = ONNXModel()

shap = (
    ImageSHAP()
    .setModel(model)
    .setOutputCol("shaps")
    .setSuperpixelCol("superpixels")
    .setInputCol("image")
    .setCellSize(150.0)
    .setModifier(50.0)
    .setNumSamples(500)
    .setTargetCol("probability")
    .setTargetClassesCol("top2pred")
)
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.explainers._
import com.microsoft.azure.synapse.ml.onnx._
import spark.implicits._

val model = (new ONNXModel())

val shap = (new ImageSHAP()
    .setModel(model)
    .setOutputCol("shaps")
    .setSuperpixelCol("superpixels")
    .setInputCol("image")
    .setCellSize(150.0)
    .setModifier(50.0)
    .setNumSamples(500)
    .setTargetCol("probability")
    .setTargetClassesCol("top2pred")
))
```

</TabItem>
</Tabs>

<DocTable className="ImageSHAP"
py="synapse.ml.explainers.html#module-synapse.ml.explainers.ImageSHAP"
scala="com/microsoft/azure/synapse/ml/explainers/ImageSHAP.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/explainers/ImageSHAP.scala" />


### TabularLIME

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.explainers import *
from synapse.ml.onnx import ONNXModel

model = ONNXModel()
data = spark.createDataFrame([
    (-6.0, 0),
    (-5.0, 0),
    (5.0, 1),
    (6.0, 1)
], ["col1", "label"])

lime = (TabularLIME()
    .setModel(model)
    .setInputCols(["col1"])
    .setOutputCol("weights")
    .setBackgroundData(data)
    .setKernelWidth(0.001)
    .setNumSamples(1000)
    .setTargetCol("probability")
    .setTargetClasses([0, 1]))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.explainers._
import com.microsoft.azure.synapse.ml.onnx._
import spark.implicits._

val model = (new ONNXModel())
val data = Seq(
  (-6.0, 0),
  (-5.0, 0),
  (5.0, 1),
  (6.0, 1)
).toDF("col1", "label")

val lime = (new TabularLIME()
    .setInputCols(Array("col1"))
    .setOutputCol("weights")
    .setBackgroundData(data)
    .setKernelWidth(0.001)
    .setNumSamples(1000)
    .setModel(model)
    .setTargetCol("probability")
    .setTargetClasses(Array(0, 1)))
```

</TabItem>
</Tabs>

<DocTable className="TabularLIME"
py="synapse.ml.explainers.html#module-synapse.ml.explainers.TabularLIME"
scala="com/microsoft/azure/synapse/ml/explainers/TabularLIME.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/explainers/TabularLIME.scala" />


### TabularSHAP

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.explainers import *
from synapse.ml.onnx import ONNXModel

model = ONNXModel()
data = spark.createDataFrame([
    (-5.0, "a", -5.0, 0),
    (-5.0, "b", -5.0, 0),
    (5.0, "a", 5.0, 1),
    (5.0, "b", 5.0, 1)
]*100, ["col1", "label"])

shap = (TabularSHAP()
    .setInputCols(["col1", "col2", "col3"])
    .setOutputCol("shapValues")
    .setBackgroundData(data)
    .setNumSamples(1000)
    .setModel(model)
    .setTargetCol("probability")
    .setTargetClasses([1]))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.explainers._
import com.microsoft.azure.synapse.ml.onnx._
import spark.implicits._

val model = (new ONNXModel())
val data = (1 to 100).flatMap(_ => Seq(
    (-5d, "a", -5d, 0),
    (-5d, "b", -5d, 0),
    (5d, "a", 5d, 1),
    (5d, "b", 5d, 1)
  )).toDF("col1", "col2", "col3", "label")

val shap = (new TabularSHAP()
    .setInputCols(Array("col1", "col2", "col3"))
    .setOutputCol("shapValues")
    .setBackgroundData(data)
    .setNumSamples(1000)
    .setModel(model)
    .setTargetCol("probability")
    .setTargetClasses(Array(1)))
```

</TabItem>
</Tabs>

<DocTable className="TabularSHAP"
py="synapse.ml.explainers.html#module-synapse.ml.explainers.TabularSHAP"
scala="com/microsoft/azure/synapse/ml/explainers/TabularSHAP.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/explainers/TabularSHAP.scala" />


### TextLIME

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.explainers import *
from synapse.ml.onnx import ONNXModel

model = ONNXModel()

lime = (TextLIME()
    .setModel(model)
    .setInputCol("text")
    .setTargetCol("prob")
    .setTargetClasses([1])
    .setOutputCol("weights")
    .setTokensCol("tokens")
    .setSamplingFraction(0.7)
    .setNumSamples(1000))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.explainers._
import com.microsoft.azure.synapse.ml.onnx._
import spark.implicits._

val model = (new ONNXModel())

val lime = (new TextLIME()
    .setModel(model)
    .setInputCol("text")
    .setTargetCol("prob")
    .setTargetClasses(Array(1))
    .setOutputCol("weights")
    .setTokensCol("tokens")
    .setSamplingFraction(0.7)
    .setNumSamples(1000))
```

</TabItem>
</Tabs>

<DocTable className="TextLIME"
py="synapse.ml.explainers.html#module-synapse.ml.explainers.TextLIME"
scala="com/microsoft/azure/synapse/ml/explainers/TextLIME.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/explainers/TextLIME.scala" />


### TextSHAP

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.explainers import *
from synapse.ml.onnx import ONNXModel

model = ONNXModel()

shap = (TextSHAP()
    .setModel(model)
    .setInputCol("text")
    .setTargetCol("prob")
    .setTargetClasses([1])
    .setOutputCol("weights")
    .setTokensCol("tokens")
    .setNumSamples(1000))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.explainers._
import com.microsoft.azure.synapse.ml.onnx._
import spark.implicits._

val model = (new ONNXModel())

val shap = (new TextSHAP()
    .setModel(model)
    .setInputCol("text")
    .setTargetCol("prob")
    .setTargetClasses(Array(1))
    .setOutputCol("weights")
    .setTokensCol("tokens")
    .setNumSamples(1000))
```

</TabItem>
</Tabs>

<DocTable className="TextSHAP"
py="synapse.ml.explainers.html#module-synapse.ml.explainers.TextSHAP"
scala="com/microsoft/azure/synapse/ml/explainers/TextSHAP.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/explainers/TextSHAP.scala" />


### VectorLIME

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.explainers import *
from synapse.ml.onnx import ONNXModel

model = ONNXModel()

df = spark.createDataframe([
  ([0.2729799734928408, -0.4637273304253777, 1.565593782147994], 4.541185129673482),
  ([1.9511879801376864, 1.495644437589599, -0.4667847796501322], 0.19526424470709836)
])

lime = (VectorLIME()
    .setModel(model)
    .setBackgroundData(df)
    .setInputCol("features")
    .setTargetCol("label")
    .setOutputCol("weights")
    .setNumSamples(1000))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.explainers._
import spark.implicits._
import breeze.linalg.{*, DenseMatrix => BDM}
import breeze.stats.distributions.Rand
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression

val d1 = 3
val d2 = 1
val coefficients: BDM[Double] = new BDM(d1, d2, Array(1.0, -1.0, 2.0))

val df = {
    val nRows = 100
    val intercept: Double = math.random()

    val x: BDM[Double] = BDM.rand(nRows, d1, Rand.gaussian)
    val y = x * coefficients + intercept

    val xRows = x(*, ::).iterator.toSeq.map(dv => Vectors.dense(dv.toArray))
    val yRows = y(*, ::).iterator.toSeq.map(dv => dv(0))
    xRows.zip(yRows).toDF("features", "label")
  }

val model: LinearRegressionModel = new LinearRegression().fit(df)

val lime = (new VectorLIME()
    .setModel(model)
    .setBackgroundData(df)
    .setInputCol("features")
    .setTargetCol(model.getPredictionCol)
    .setOutputCol("weights")
    .setNumSamples(1000))
```

</TabItem>
</Tabs>

<DocTable className="VectorLIME"
py="synapse.ml.explainers.html#module-synapse.ml.explainers.VectorLIME"
scala="com/microsoft/azure/synapse/ml/explainers/VectorLIME.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/explainers/VectorLIME.scala" />


### VectorSHAP

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.explainers import *
from synapse.ml.onnx import ONNXModel

model = ONNXModel()

shap = (VectorSHAP()
    .setModel(model)
    .setInputCol("text")
    .setTargetCol("prob")
    .setTargetClasses([1])
    .setOutputCol("weights")
    .setTokensCol("tokens")
    .setNumSamples(1000))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.explainers._
import spark.implicits._
import breeze.linalg.{*, DenseMatrix => BDM}
import breeze.stats.distributions.RandBasis
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vectors

val randBasis = RandBasis.withSeed(123)
val m: BDM[Double] = BDM.rand[Double](1000, 5, randBasis.gaussian)
val l: BDV[Double] = m(*, ::).map {
    row =>
      if (row(2) + row(3) > 0.5) 1d else 0d
  }
val data = m(*, ::).iterator.zip(l.valuesIterator).map {
    case (f, l) => (f.toSpark, l)
  }.toSeq.toDF("features", "label")

val model = new LogisticRegression()
    .setFeaturesCol("features")
    .setLabelCol("label")
    .fit(data)

val shap = (new VectorSHAP()
    .setInputCol("features")
    .setOutputCol("shapValues")
    .setBackgroundData(data)
    .setNumSamples(1000)
    .setModel(model)
    .setTargetCol("probability")
    .setTargetClasses(Array(1))

val infer = Seq(
    Tuple1(Vectors.dense(1d, 1d, 1d, 1d, 1d))
  ) toDF "features"
val predicted = model.transform(infer)
display(shap.transform(predicted))
```

</TabItem>
</Tabs>

<DocTable className="VectorSHAP"
py="synapse.ml.explainers.html#module-synapse.ml.explainers.VectorSHAP"
scala="com/microsoft/azure/synapse/ml/explainers/VectorSHAP.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/explainers/VectorSHAP.scala" />



