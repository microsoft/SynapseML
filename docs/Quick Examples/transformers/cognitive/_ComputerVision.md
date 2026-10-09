import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";




## Computer Vision

### Image Analysis 4.0

Use `AnalyzeImageV4` for the Image Analysis 4.0 GA API. It sends synchronous
requests to `/computervision/imageanalysis:analyze?api-version=2024-02-01`.
Select `tags`, `objects`, `caption`, `denseCaptions`, `read`, `smartCrops`, or
`people` with `setFeatures`. The default is `tags`. Caption and dense-caption
features require a supported Azure region; they are not enabled by default.
See Microsoft's [Image Analysis 4.0 guide](https://learn.microsoft.com/en-us/azure/ai-services/computer-vision/how-to/call-analyze-image-40).

This is a separate transformer, not a URL override for `AnalyzeImage`.
Its typed output uses `tagsResult.values`, `objectsResult.values`,
`captionResult`, and `readResult.blocks`. Unrequested features are null.
`read` extracts text from images synchronously; it is not the asynchronous,
multi-page document API used by `ReadImage`. HTTP failures remain in `errorCol`.
The shared JSON parser is permissive, so malformed successful responses can
contain null fields without an HTTP error. Check required result fields before
treating them as a successful analysis.

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.services.vision import AnalyzeImageV4

analyzer = (AnalyzeImageV4()
            .setLocation("eastus")
            .setFeatures(["tags", "objects", "read"]))
```

Configure authentication, image input and output as shown in the
[AnalyzeImageV4 example](#analyzeimagev4) below. `setEndpoint` accepts your
resource endpoint with or without a trailing slash. `setUrl` instead accepts
a complete operation URL. Location, resource-name and endpoint setters replace
an earlier URL override. The stage merges query parameters, pins `api-version`
to `2024-02-01`, and gives its configured parameters precedence over URL query
values. Use `setFeaturesCol` for per-row selections.

`setSmartCropsAspectRatiosCol` accepts numeric array columns, including inferred
Python integer arrays. Values are encoded as doubles and must be between 0.75
and 1.8 inclusive. Empty arrays, null elements, non-numeric values, and values
outside that range fail validation before an HTTP request. A null array omits
the optional parameter for that row.

The v4 API has no equivalent for legacy Categories, Color, ImageType, Adult,
Brands, face age/gender, celebrity recognition, or landmark recognition.
`people` is person detection, not face analysis, and `smartCrops` returns crop
coordinates, not a thumbnail image. Existing transformers and saved models
retain their own schemas and semantics; they are not silently converted to v4.

### Legacy API compatibility

`AnalyzeImage`, `OCR`, `ReadImage`, `GenerateThumbnails`, `TagImage`,
`DescribeImage`, and `RecognizeDomainSpecificContent` use Computer Vision v3.2
when constructing endpoints. Microsoft retired Computer Vision v1.0, v2.0, v2.1,
v3.0, and v3.1 on September 13, 2026. See the
[retirement announcement](https://www.microsoft.com/releasecommunications/api/v2/azure/rss/computer-vision-api-retirements-13-9-2026).
HTTP 410 `ApiVersionRetired` requires migration, not throttling retries.

The legacy v3.2 migration keeps the existing transformer parameters and output schemas,
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

legacyAnalyzer = (AnalyzeImage()
            .setLocation("eastus")
            .setUrl("https://eastus.api.cognitive.microsoft.com/vision/v3.2/analyze"))
```

Use the endpoint for your own resource and keep your existing authentication,
input, output, and visual-feature settings. `RecognizeDomainSpecificContent`
instead takes the API root, ending in `/vision/v3.2`, and appends
`/models/{model}/analyze`.

`RecognizeText` is a legacy v2.0 operation without a matching v3.2 endpoint.
Its public API remains available for compatibility, but its retired hosted
endpoint no longer works. For images, use `AnalyzeImageV4` with the `read`
feature and consume `readResult.blocks`. For the legacy document-reading
contract, use `ReadImage` and extract `analyzeResult.readResults`.
This is not a drop-in URL substitution: `RecognizeText`
uses `mode` and `recognitionResult`, whereas `ReadImage` uses asynchronous Read
results under `analyzeResult.readResults`. See the
[Read v3.2 guide](https://learn.microsoft.com/en-us/azure/ai-services/computer-vision/how-to/call-read-api).

Image Analysis v3.2 and v4.0 are both scheduled to retire on September 25, 2028.
Review Microsoft's [migration options](https://learn.microsoft.com/en-us/azure/ai-services/computer-vision/migration-options)
when planning beyond that date.

### OCR (legacy v3.2)

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


### AnalyzeImageV4

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


ai = (AnalyzeImageV4()
        .setSubscriptionKey(cognitiveKey)
        .setLocation("eastus")
        .setImageUrlCol("image")
        .setLanguageCol("language")
        .setFeatures(["tags", "objects", "read"])
        .setOutputCol("features")
        .setErrorCol("analysisError"))

ai.transform(df).select("image", "features.tagsResult.values",
                        "features.objectsResult.values", "features.readResult",
                        "analysisError").show(truncate=False)
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.services.vision.AnalyzeImageV4
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val df = Seq(
  ("https://mmlspark.blob.core.windows.net/datasets/OCR/test1.jpg", "en"),
  ("https://mmlspark.blob.core.windows.net/datasets/OCR/test2.png", null),
  ("https://mmlspark.blob.core.windows.net/datasets/OCR/test3.png", "en")
).toDF("url", "language")

val ai = (new AnalyzeImageV4()
  .setSubscriptionKey(cognitiveKey)
  .setLocation("eastus")
  .setImageUrlCol("url")
  .setLanguageCol("language")
  .setFeatures(Seq("tags", "objects", "read"))
  .setOutputCol("features")
  .setErrorCol("analysisError"))

ai.transform(df).select("url", "features", "analysisError").show(false)
```

</TabItem>
</Tabs>

Source: [AnalyzeImageV4.scala](https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/vision/AnalyzeImageV4.scala).

The legacy `AnalyzeImage` stage remains available for its v3.2 feature set and
response schema. It is not an alias for `AnalyzeImageV4`.


### RecognizeText

This legacy operation's hosted v2.0 endpoint has retired. Use
[AnalyzeImageV4](#analyzeimagev4) with `read` for image text extraction and
update consumers of `recognitionResult` to use `readResult.blocks`.
The public API remains available for loading legacy
models and using explicit compatible endpoints, but there is no working hosted
service example for this operation.

<DocTable className="RecognizeText"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.RecognizeText"
scala="com/microsoft/azure/synapse/ml/cognitive/RecognizeText.html"
csharp="classSynapse_1_1ML_1_1Cognitive_1_1RecognizeText.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/ComputerVision.scala" />


### ReadImage (legacy v3.2 document Read)

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


### RecognizeDomainSpecificContent (legacy v3.2)

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


### GenerateThumbnails (legacy v3.2)

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


### TagImage (legacy v3.2)

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


### DescribeImage (legacy v3.2)

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
