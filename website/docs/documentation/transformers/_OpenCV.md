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

## ImageTransformer

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.opencv import *
from pyspark.sql.types import FloatType

# images = (spark.read.format("image")
#         .option("dropInvalid", True)
#         .load("wasbs://publicwasb@mmlspark.blob.core.windows.net/explainers/images/david-lusvardi-dWcUncxocQY-unsplash.jpg"))

it = (ImageTransformer(inputCol="image", outputCol="features")
    .resize(224, True)
    .centerCrop(height=224, width=224)
    .normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225], color_scale_factor = 1/255)
    .setTensorElementType(FloatType()))

# display(it.transform(images))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.opencv._

val images = (spark.read.format("image")
    .option("dropInvalid", true)
    .load("wasbs://publicwasb@mmlspark.blob.core.windows.net/explainers/images/david-lusvardi-dWcUncxocQY-unsplash.jpg"))

val it = (new ImageTransformer()
      .setOutputCol("out")
      .resize(height = 15, width = 10))

display(it.transform(images))
```

</TabItem>
</Tabs>

<DocTable className="ImageTransformer"
py="mmlspark.opencv.html#module-mmlspark.opencv.ImageTransformer"
scala="com/microsoft/ml/spark/opencv/ImageTransformer.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/opencv/src/main/scala/com/microsoft/azure/synapse/ml/opencv/ImageTransformer.scala" />


## ImageSetAugmenter

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.opencv import *

# images = (spark.read.format("image")
#         .option("dropInvalid", True)
#         .load("wasbs://publicwasb@mmlspark.blob.core.windows.net/explainers/images/david-lusvardi-dWcUncxocQY-unsplash.jpg"))

isa = (ImageSetAugmenter()
    .setInputCol("image")
    .setOutputCol("augmented")
    .setFlipLeftRight(True)
    .setFlipUpDown(True))

# display(it.transform(images))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.opencv._

val images = (spark.read.format("image")
    .option("dropInvalid", true)
    .load("wasbs://publicwasb@mmlspark.blob.core.windows.net/explainers/images/david-lusvardi-dWcUncxocQY-unsplash.jpg"))

val isa = (new ImageSetAugmenter()
    .setInputCol("image")
    .setOutputCol("augmented")
    .setFlipLeftRight(true)
    .setFlipUpDown(true))

display(isa.transform(images))
```

</TabItem>
</Tabs>

<DocTable className="ImageSetAugmenter"
py="mmlspark.opencv.html#module-mmlspark.opencv.ImageSetAugmenter"
scala="com/microsoft/ml/spark/opencv/ImageSetAugmenter.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/opencv/src/main/scala/com/microsoft/azure/synapse/ml/opencv/ImageSetAugmenter.scala" />


