import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";




## Computer Vision

### API versions and migration

`AnalyzeImage`, `OCR`, `ReadImage`, `GenerateThumbnails`, `TagImage`,
`DescribeImage`, and `RecognizeDomainSpecificContent` use Computer Vision v3.2
when constructing endpoints. Microsoft retired Computer Vision v1.0, v2.0, v2.1,
v3.0, and v3.1 on September 13, 2026. See the
[retirement announcement](https://www.microsoft.com/releasecommunications/api/v2/azure/rss/computer-vision-api-retirements-13-9-2026).
HTTP 410 `ApiVersionRetired` requires migration, not throttling retries.

The v3.2 migration keeps the existing transformer parameters and output schemas,
including `AnalyzeImage`'s `description.tags` array of strings. It does not adopt
the different Image Analysis v4.0 response format. Extra service response fields,
such as `modelVersion`, are not added to the existing Spark schemas.

Explicit `setUrl` values and URLs in saved models are preserved. Upgrade the
library to get the new endpoint defaults. For an older library or a previously
saved model, set the full operation URL after calling `setLocation`,
`setEndpoint`, `setCustomServiceName`, or `setLinkedService`:

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.services import AnalyzeImage

analyzer = (AnalyzeImage()
            .setLocation("eastus")
            .setUrl("https://eastus.api.cognitive.microsoft.com/vision/v3.2/analyze"))
```

Use the endpoint for your own resource and keep your existing authentication,
input, output, and visual-feature settings. `RecognizeDomainSpecificContent`
instead takes the API root, ending in `/vision/v3.2`, and appends
`/models/{model}/analyze`.

`RecognizeText` is a legacy v2.0 operation without a matching v3.2 endpoint.
Its public API remains available for compatibility, but its retired hosted
endpoint no longer works. Migrate text extraction to `ReadImage` and use
`ReadImage.flatten` in Scala, or extract text from `analyzeResult.readResults`
in Python. This is not a drop-in URL substitution: `RecognizeText`
uses `mode` and `recognitionResult`, whereas `ReadImage` uses asynchronous Read
results under `analyzeResult.readResults`. See the
[Read v3.2 guide](https://learn.microsoft.com/en-us/azure/ai-services/computer-vision/how-to/call-read-api).

Image Analysis v3.2 and v4.0 are both scheduled to retire on September 25, 2028.
Review Microsoft's [migration options](https://learn.microsoft.com/en-us/azure/ai-services/computer-vision/migration-options)
when planning beyond that date.

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


### RecognizeText

This legacy operation's hosted v2.0 endpoint has retired. Use [ReadImage](#readimage)
for text extraction and update consumers of `recognitionResult` to use
`analyzeResult.readResults`. The public API remains available for loading legacy
models and using explicit compatible endpoints, but there is no working hosted
service example for this operation.

<DocTable className="RecognizeText"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.RecognizeText"
scala="com/microsoft/azure/synapse/ml/cognitive/RecognizeText.html"
csharp="classSynapse_1_1ML_1_1Cognitive_1_1RecognizeText.html"
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
