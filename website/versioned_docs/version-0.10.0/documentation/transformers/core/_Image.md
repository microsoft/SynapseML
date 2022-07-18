import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";




## Image

### ResizeImageTransformer

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.image import *

# images = (spark.read.format("image")
#         .option("dropInvalid", True)
#         .load("wasbs://datasets@mmlspark.blob.core.windows.net/LIME/greyscale.jpg"))

rit = (ResizeImageTransformer()
        .setOutputCol("out")
        .setHeight(15)
        .setWidth(10))

# rit.transform(images).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.image._
import spark.implicits._

// val images = (spark.read.format("image")
//     .option("dropInvalid", true)
//     .load("wasbs://datasets@mmlspark.blob.core.windows.net/LIME/greyscale.jpg"))

val rit = (new ResizeImageTransformer()
    .setOutputCol("out")
    .setHeight(15)
    .setWidth(10))

// rit.transform(images).show()
```

</TabItem>
</Tabs>

<DocTable className="ResizeImageTransformer"
py="synapse.ml.image.html#module-synapse.ml.image.ResizeImageTransformer"
scala="com/microsoft/azure/synapse/ml/image/ResizeImageTransformer.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/image/ResizeImageTransformer.scala" />


### UnrollImage

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.image import *
from azure.storage.blob import *

# images = (spark.read.format("image")
#         .option("dropInvalid", True)
#         .load("wasbs://datasets@mmlspark.blob.core.windows.net/LIME/greyscale.jpg"))

# rit = (ResizeImageTransformer()
#         .setOutputCol("out")
#         .setHeight(15)
#         .setWidth(10))

# preprocessed = rit.transform(images)

unroll = (UnrollImage()
      .setInputCol("out")
      .setOutputCol("final"))

# unroll.transform(preprocessed).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.image._
import spark.implicits._

val images = (spark.read.format("image")
    .option("dropInvalid", true)
    .load("wasbs://datasets@mmlspark.blob.core.windows.net/LIME/greyscale.jpg"))

val rit = (new ResizeImageTransformer()
    .setOutputCol("out")
    .setHeight(15)
    .setWidth(10))

val preprocessed = rit.transform(images)

val unroll = (new UnrollImage()
      .setInputCol(rit.getOutputCol)
      .setOutputCol("final"))

unroll.transform(preprocessed).show()
```

</TabItem>
</Tabs>

<DocTable className="UnrollImage"
py="synapse.ml.image.html#module-synapse.ml.image.UnrollImage"
scala="com/microsoft/azure/synapse/ml/image/UnrollImage.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/image/UnrollImage.scala" />


### UnrollBinaryImage

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.image import *

unroll = (UnrollBinaryImage()
      .setInputCol("input_col")
      .setOutputCol("final"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.image._
import spark.implicits._

val unroll = (new UnrollBinaryImage()
        .setInputCol("input_col")
        .setOutputCol("final"))

```

</TabItem>
</Tabs>

<DocTable className="UnrollBinaryImage"
py="synapse.ml.image.html#module-synapse.ml.image.UnrollBinaryImage"
scala="com/microsoft/azure/synapse/ml/image/UnrollBinaryImage.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/image/UnrollBinaryImage.scala" />
