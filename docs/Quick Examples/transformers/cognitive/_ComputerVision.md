import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";




## Computer Vision

### Service endpoints

`AnalyzeImage`, `OCR`, `ReadImage`, `GenerateThumbnails`, `TagImage`,
`DescribeImage`, and `RecognizeDomainSpecificContent` use Computer Vision v3.2
by default.

To specify an endpoint explicitly, set the full operation URL with `setUrl`
after calling `setLocation`, `setEndpoint`, `setCustomServiceName`, or
`setLinkedService`:

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.services import AnalyzeImage

analyzer = (AnalyzeImage()
            .setLocation("eastus")
            .setUrl("https://eastus.api.cognitive.microsoft.com/vision/v3.2/analyze"))
```

Use the endpoint for your own resource and configure authentication, input,
output, and visual features as shown below. `RecognizeDomainSpecificContent`
instead takes the API root, ending in `/vision/v3.2`, and appends
`/models/{model}/analyze`.

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
from synapse.ml.services import *

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
import com.microsoft.azure.synapse.ml.services.vision.OCR
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
csharp="classSynapse_1_1ML_1_1Cognitive_1_1OCR.html"
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
from synapse.ml.services import *

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
import com.microsoft.azure.synapse.ml.services.vision.AnalyzeImage
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
csharp="classSynapse_1_1ML_1_1Cognitive_1_1AnalyzeImage.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/ComputerVision.scala" />


### ReadImage

`ReadImage` extracts text asynchronously and returns results under
`analyzeResult.readResults`. Before using `ReadImage.flatten` in Scala or
extracting text in Python, require `status == "succeeded"` and a non-null
`analyzeResult`. A failed Read operation can return HTTP 200 with
`status == "failed"` and no `analyzeResult`, so check the operation status as
well as the HTTP error column. Keep failed rows for error handling rather than
passing them to `ReadImage.flatten`, which requires a non-null `analyzeResult`.
See the [Read v3.2 guide](https://learn.microsoft.com/en-us/azure/ai-services/computer-vision/how-to/call-read-api).

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.services import *

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
import com.microsoft.azure.synapse.ml.services.vision.ReadImage
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
csharp="classSynapse_1_1ML_1_1Cognitive_1_1ReadImage.html"
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
from synapse.ml.services import *

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
import com.microsoft.azure.synapse.ml.services.vision.RecognizeDomainSpecificContent
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
csharp="classSynapse_1_1ML_1_1Cognitive_1_1RecognizeDomainSpecificContent.html"
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
from synapse.ml.services import *

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
import com.microsoft.azure.synapse.ml.services.vision.GenerateThumbnails
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
csharp="classSynapse_1_1ML_1_1Cognitive_1_1GenerateThumbnails.html"
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
from synapse.ml.services import *

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
import com.microsoft.azure.synapse.ml.services.vision.TagImage
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
csharp="classSynapse_1_1ML_1_1Cognitive_1_1TagImage.html"
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
from synapse.ml.services import *

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
import com.microsoft.azure.synapse.ml.services.vision.DescribeImage
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
csharp="classSynapse_1_1ML_1_1Cognitive_1_1DescribeImage.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/ComputerVision.scala" />
