import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";

<!-- 
```python
import pyspark
import os
import json
import mmlspark
from IPython.display import display

spark = (pyspark.sql.SparkSession.builder.appName("MyApp")
        .config("spark.jars.packages", "com.microsoft.ml.spark:mmlspark:1.0.0-rc4")
        .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven")
        .getOrCreate())

def getSecret(secretName):
        get_secret_cmd = 'az keyvault secret show --vault-name mmlspark-build-keys --name {}'.format(secretName)
        value = json.loads(os.popen(get_secret_cmd).read())["value"]
        return value

``` 
-->

## OCR

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from mmlspark.cognitive import *

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

display(ocr.transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
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

display(ocr.transform(df))
```

</TabItem>
</Tabs>

<DocTable className="OCR"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.OCR"
scala="com/microsoft/ml/spark/cognitive/OCR.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/ComputerVision.scala" />


## AnalyzeImage

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from mmlspark.cognitive import *

cognitiveKey = os.environ.get("COGNITIVE_API_KEY", getSecret("cognitive-api-key"))
df = spark.createDataFrame([
        ("https://raw.githubusercontent.com/Azure-Samples/cognitive-services-sample-data-files/master/ComputerVision/Images/objects.jpg", ),
        ("https://raw.githubusercontent.com/Azure-Samples/cognitive-services-sample-data-files/master/ComputerVision/Images/dog.jpg", ),
        ("https://raw.githubusercontent.com/Azure-Samples/cognitive-services-sample-data-files/master/ComputerVision/Images/house.jpg", )
    ], ["image", ])


ai = (AnalyzeImage()
        .setSubscriptionKey(cognitiveKey)
        .setLocation("eastus")
        .setImageUrlCol("url")
        .setLanguageCol("language")
        .setVisualFeatures(["Categories", "Tags", "Description", "Faces", "ImageType", "Color", "Adult", "Objects", "Brands"])
        .setDetails(["Celebrities", "Landmarks"])
        .setOutputCol("features"))

display(ai.transform(df).select("url", "features.description.tags"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
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

display(ai.transform(df).select("url", "features"))
```

</TabItem>
</Tabs>

<DocTable className="AnalyzeImage"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.AnalyzeImage"
scala="com/microsoft/ml/spark/cognitive/AnalyzeImage.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/ComputerVision.scala" />


## RecognizeText

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from mmlspark.cognitive import *

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

display(rt.transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
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

display(rt.transform(df))
```

</TabItem>
</Tabs>

<DocTable className="RecognizeText"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.RecognizeText"
scala="com/microsoft/ml/spark/cognitive/RecognizeText.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/ComputerVision.scala" />


## ReadImage

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from mmlspark.cognitive import *

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

display(ri.transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
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

display(ri.transform(df))
```

</TabItem>
</Tabs>

<DocTable className="ReadImage"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.ReadImage"
scala="com/microsoft/ml/spark/cognitive/ReadImage.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/ComputerVision.scala" />


## RecognizeDomainSpecificContent

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from mmlspark.cognitive import *

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

display(celeb.transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
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

display(celeb.transform(df))
```

</TabItem>
</Tabs>

<DocTable className="RecognizeDomainSpecificContent"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.RecognizeDomainSpecificContent"
scala="com/microsoft/ml/spark/cognitive/RecognizeDomainSpecificContent.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/ComputerVision.scala" />


## GenerateThumbnails

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from mmlspark.cognitive import *

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

display(gt.transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
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

display(gt.transform(df))
```

</TabItem>
</Tabs>

<DocTable className="GenerateThumbnails"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.GenerateThumbnails"
scala="com/microsoft/ml/spark/cognitive/GenerateThumbnails.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/ComputerVision.scala" />


## TagImage

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from mmlspark.cognitive import *

cognitiveKey = os.environ.get("COGNITIVE_API_KEY", getSecret("cognitive-api-key"))
df = spark.createDataFrame([
        ("https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg", )
    ], ["url", ])

ti = (TagImage()
        .setSubscriptionKey(cognitiveKey)
        .setLocation("eastus")
        .setImageUrlCol("url")
        .setOutputCol("tags"))

display(ti.transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
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

display(ti.transform(df))
```

</TabItem>
</Tabs>

<DocTable className="TagImage"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.TagImage"
scala="com/microsoft/ml/spark/cognitive/TagImage.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/ComputerVision.scala" />


## DescribeImage

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from mmlspark.cognitive import *

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

display(di.transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
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

display(di.transform(df))
```

</TabItem>
</Tabs>

<DocTable className="DescribeImage"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.DescribeImage"
scala="com/microsoft/ml/spark/cognitive/DescribeImage.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/ComputerVision.scala" />



