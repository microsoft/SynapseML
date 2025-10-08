import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";




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

# it.transform(images).show()
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

it.transform(images).show()
```

</TabItem>
</Tabs>

<DocTable className="ImageTransformer"
py="synapse.ml.opencv.html#module-synapse.ml.opencv.ImageTransformer"
scala="com/microsoft/azure/synapse/ml/opencv/ImageTransformer.html"
csharp="classSynapse_1_1ML_1_1Opencv_1_1ImageTransformer.html"
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

# it.transform(images).show()
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

isa.transform(images).show()
```

</TabItem>
</Tabs>

<DocTable className="ImageSetAugmenter"
py="synapse.ml.opencv.html#module-synapse.ml.opencv.ImageSetAugmenter"
scala="com/microsoft/azure/synapse/ml/opencv/ImageSetAugmenter.html"
csharp="classSynapse_1_1ML_1_1Opencv_1_1ImageSetAugmenter.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/opencv/src/main/scala/com/microsoft/azure/synapse/ml/opencv/ImageSetAugmenter.scala" />
