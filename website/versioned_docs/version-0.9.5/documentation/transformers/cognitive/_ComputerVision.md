import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";




## Computer Vision

### OCR

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.cognitive import *

cognitiveKey = os.environ.get("COGNITIVE_API_KEY", getSecret("cognitive-api-key"))

df = spark.createDataFrame([
        ("https://mmlspark.blob.core.windows.net/datasets/OCR/test1.jpg", ),
    ], ["url", ])

ocr = (OCR()
        .setSubscriptionKey(cognitiveKey)
        .setLocation("eastus")
        .setImageUrlCol("url")
        .setDetectOrientation(True)
        .setOutputCol("ocr"))

ocr.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val df = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/OCR/test1.jpg"
  ).toDF("url")


val ocr = (new OCR()
        .setSubscriptionKey(cognitiveKey)
        .setLocation("eastus")
        .setImageUrlCol("url")
        .setDetectOrientation(true)
        .setOutputCol("ocr"))

ocr.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="OCR"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.OCR"
scala="com/microsoft/azure/synapse/ml/cognitive/OCR.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/ComputerVision.scala" />


### AnalyzeImage

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.cognitive import *

cognitiveKey = os.environ.get("COGNITIVE_API_KEY", getSecret("cognitive-api-key"))
df = spark.createDataFrame([
        ("https://mmlspark.blob.core.windows.net/datasets/OCR/test1.jpg", "en"),
        ("https://mmlspark.blob.core.windows.net/datasets/OCR/test2.png", None),
        ("https://mmlspark.blob.core.windows.net/datasets/OCR/test3.png", "en")
    ], ["image", "language"])


ai = (AnalyzeImage()
        .setSubscriptionKey(cognitiveKey)
        .setLocation("eastus")
        .setImageUrlCol("image")
        .setLanguageCol("language")
        .setVisualFeatures(["Categories", "Tags", "Description", "Faces", "ImageType", "Color", "Adult", "Objects", "Brands"])
        .setDetails(["Celebrities", "Landmarks"])
        .setOutputCol("features"))

ai.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val df = Seq(
    ("https://mmlspark.blob.core.windows.net/datasets/OCR/test1.jpg", "en"),
    ("https://mmlspark.blob.core.windows.net/datasets/OCR/test2.png", null),
    ("https://mmlspark.blob.core.windows.net/datasets/OCR/test3.png", "en")
  ).toDF("url", "language")

val ai = (new AnalyzeImage()
        .setSubscriptionKey(cognitiveKey)
        .setLocation("eastus")
        .setImageUrlCol("url")
        .setLanguageCol("language")
        .setVisualFeatures(Seq("Categories", "Tags", "Description", "Faces", "ImageType", "Color", "Adult", "Objects", "Brands"))
        .setDetails(Seq("Celebrities", "Landmarks"))
        .setOutputCol("features"))

ai.transform(df).select("url", "features").show()
```

</TabItem>
</Tabs>

<DocTable className="AnalyzeImage"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.AnalyzeImage"
scala="com/microsoft/azure/synapse/ml/cognitive/AnalyzeImage.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/ComputerVision.scala" />


### RecognizeText

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.cognitive import *

cognitiveKey = os.environ.get("COGNITIVE_API_KEY", getSecret("cognitive-api-key"))
df = spark.createDataFrame([
        ("https://mmlspark.blob.core.windows.net/datasets/OCR/test1.jpg", ),
        ("https://mmlspark.blob.core.windows.net/datasets/OCR/test2.png", ),
        ("https://mmlspark.blob.core.windows.net/datasets/OCR/test3.png", )
    ], ["url", ])

rt = (RecognizeText()
        .setSubscriptionKey(cognitiveKey)
        .setLocation("eastus")
        .setImageUrlCol("url")
        .setMode("Printed")
        .setOutputCol("ocr")
        .setConcurrency(5))

rt.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val df = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/OCR/test1.jpg",
    "https://mmlspark.blob.core.windows.net/datasets/OCR/test2.png",
    "https://mmlspark.blob.core.windows.net/datasets/OCR/test3.png"
  ).toDF("url")

val rt = (new RecognizeText()
        .setSubscriptionKey(cognitiveKey)
        .setLocation("eastus")
        .setImageUrlCol("url")
        .setMode("Printed")
        .setOutputCol("ocr")
        .setConcurrency(5))

rt.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="RecognizeText"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.RecognizeText"
scala="com/microsoft/azure/synapse/ml/cognitive/RecognizeText.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/ComputerVision.scala" />


### ReadImage

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.cognitive import *

cognitiveKey = os.environ.get("COGNITIVE_API_KEY", getSecret("cognitive-api-key"))
df = spark.createDataFrame([
        ("https://mmlspark.blob.core.windows.net/datasets/OCR/test1.jpg", ),
        ("https://mmlspark.blob.core.windows.net/datasets/OCR/test2.png", ),
        ("https://mmlspark.blob.core.windows.net/datasets/OCR/test3.png", )
    ], ["url", ])

ri = (ReadImage()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setImageUrlCol("url")
    .setOutputCol("ocr")
    .setConcurrency(5))

ri.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val df = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/OCR/test1.jpg",
    "https://mmlspark.blob.core.windows.net/datasets/OCR/test2.png",
    "https://mmlspark.blob.core.windows.net/datasets/OCR/test3.png"
  ).toDF("url")

val ri = (new ReadImage()
        .setSubscriptionKey(cognitiveKey)
        .setLocation("eastus")
        .setImageUrlCol("url")
        .setOutputCol("ocr")
        .setConcurrency(5))

ri.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="ReadImage"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.ReadImage"
scala="com/microsoft/azure/synapse/ml/cognitive/ReadImage.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/ComputerVision.scala" />


### RecognizeDomainSpecificContent

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.cognitive import *

cognitiveKey = os.environ.get("COGNITIVE_API_KEY", getSecret("cognitive-api-key"))
df = spark.createDataFrame([
        ("https://mmlspark.blob.core.windows.net/datasets/DSIR/test2.jpg", )
    ], ["url", ])

celeb = (RecognizeDomainSpecificContent()
        .setSubscriptionKey(cognitiveKey)
        .setModel("celebrities")
        .setLocation("eastus")
        .setImageUrlCol("url")
        .setOutputCol("celebs"))

celeb.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val df = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/DSIR/test2.jpg"
  ).toDF("url")

val celeb = (new RecognizeDomainSpecificContent()
                .setSubscriptionKey(cognitiveKey)
                .setModel("celebrities")
                .setLocation("eastus")
                .setImageUrlCol("url")
                .setOutputCol("celebs"))

celeb.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="RecognizeDomainSpecificContent"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.RecognizeDomainSpecificContent"
scala="com/microsoft/azure/synapse/ml/cognitive/RecognizeDomainSpecificContent.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/ComputerVision.scala" />


### GenerateThumbnails

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.cognitive import *

cognitiveKey = os.environ.get("COGNITIVE_API_KEY", getSecret("cognitive-api-key"))
df = spark.createDataFrame([
        ("https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg", )
    ], ["url", ])

gt = (GenerateThumbnails()
        .setSubscriptionKey(cognitiveKey)
        .setLocation("eastus")
        .setHeight(50)
        .setWidth(50)
        .setSmartCropping(True)
        .setImageUrlCol("url")
        .setOutputCol("thumbnails"))

gt.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val df: DataFrame = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg"
  ).toDF("url")

val gt = (new GenerateThumbnails()
        .setSubscriptionKey(cognitiveKey)
        .setLocation("eastus")
        .setHeight(50)
        .setWidth(50)
        .setSmartCropping(true)
        .setImageUrlCol("url")
        .setOutputCol("thumbnails"))

gt.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="GenerateThumbnails"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.GenerateThumbnails"
scala="com/microsoft/azure/synapse/ml/cognitive/GenerateThumbnails.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/ComputerVision.scala" />


### TagImage

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.cognitive import *

cognitiveKey = os.environ.get("COGNITIVE_API_KEY", getSecret("cognitive-api-key"))
df = spark.createDataFrame([
        ("https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg", )
    ], ["url", ])

ti = (TagImage()
        .setSubscriptionKey(cognitiveKey)
        .setLocation("eastus")
        .setImageUrlCol("url")
        .setOutputCol("tags"))

ti.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val df = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg"
  ).toDF("url")

val ti = (new TagImage()
        .setSubscriptionKey(cognitiveKey)
        .setLocation("eastus")
        .setImageUrlCol("url")
        .setOutputCol("tags"))

ti.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="TagImage"
py="synapse.ml.cognitive.html#module-mmlspark.cognitive.TagImage"
scala="com/microsoft/azure/synapse/ml/cognitive/TagImage.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/ComputerVision.scala" />


### DescribeImage

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.cognitive import *

cognitiveKey = os.environ.get("COGNITIVE_API_KEY", getSecret("cognitive-api-key"))
df = spark.createDataFrame([
        ("https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg", )
    ], ["url", ])

di = (DescribeImage()
        .setSubscriptionKey(cognitiveKey)
        .setLocation("eastus")
        .setMaxCandidates(3)
        .setImageUrlCol("url")
        .setOutputCol("descriptions"))

di.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val df = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg"
  ).toDF("url")

val di = (new DescribeImage()
        .setSubscriptionKey(cognitiveKey)
        .setLocation("eastus")
        .setMaxCandidates(3)
        .setImageUrlCol("url")
        .setOutputCol("descriptions"))

di.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="DescribeImage"
py="synapse.ml.cognitive.html#module-mmlspark.cognitive.DescribeImage"
scala="com/microsoft/azure/synapse/ml/cognitive/DescribeImage.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/ComputerVision.scala" />
