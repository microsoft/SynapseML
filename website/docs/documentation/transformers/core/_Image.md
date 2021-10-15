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

## ResizeImageTransformer

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

# display(rit.transform(images))
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

// display(rit.transform(images))
```

</TabItem>
</Tabs>

<DocTable className="ResizeImageTransformer"
py="mmlspark.image.html#module-mmlspark.image.ResizeImageTransformer"
scala="com/microsoft/ml/spark/image/ResizeImageTransformer.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/image/ResizeImageTransformer.scala" />


## UnrollImage

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

images = (spark.read.format("image")
        .option("dropInvalid", True)
        .load("wasbs://datasets@mmlspark.blob.core.windows.net/LIME/greyscale.jpg"))

rit = (ResizeImageTransformer()
        .setOutputCol("out")
        .setHeight(15)
        .setWidth(10))

preprocessed = rit.transform(images)

unroll = (UnrollImage()
      .setInputCol(rit.getOutputCol)
      .setOutputCol("final"))

display(unroll.transform(preprocessed))
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

display(unroll.transform(preprocessed))
```

</TabItem>
</Tabs>

<DocTable className="UnrollImage"
py="mmlspark.image.html#module-mmlspark.image.UnrollImage"
scala="com/microsoft/ml/spark/image/UnrollImage.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/image/UnrollImage.scala" />


## UnrollBinaryImage

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
py="mmlspark.image.html#module-mmlspark.image.UnrollBinaryImage"
scala="com/microsoft/ml/spark/image/UnrollBinaryImage.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/image/UnrollBinaryImage.scala" />


