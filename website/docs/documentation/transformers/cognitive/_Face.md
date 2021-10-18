import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";

<!-- 
```python
import pyspark
import os
import json
from IPython.display import display
from pyspark.sql.functions import col, collect_list, lit, sort_array, struct

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

## Face

### DetectFace

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
  ("https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg",),
], ["url"])

face = (DetectFace()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setImageUrlCol("url")
    .setOutputCol("detected_faces")
    .setReturnFaceId(True)
    .setReturnFaceLandmarks(False)
    .setReturnFaceAttributes(["age", "gender", "headPose", "smile", "facialHair", "glasses", "emotion",
      "hair", "makeup", "occlusion", "accessories", "blur", "exposure", "noise"]))

display(face.transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val df: DataFrame = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/DSIR/test2.jpg"
  ).toDF("url")

val face = (new DetectFace()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setImageUrlCol("url")
    .setOutputCol("face")
    .setReturnFaceId(true)
    .setReturnFaceLandmarks(true)
    .setReturnFaceAttributes(Seq(
      "age", "gender", "headPose", "smile", "facialHair", "glasses", "emotion",
      "hair", "makeup", "occlusion", "accessories", "blur", "exposure", "noise")))

display(face.transform(df))
```

</TabItem>
</Tabs>

<DocTable className="DetectFace"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.DetectFace"
scala="com/microsoft/azure/synapse/ml/cognitive/DetectFace.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/Face.scala" />


### FindSimilarFace

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
  ("https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg",),
  ("https://mmlspark.blob.core.windows.net/datasets/DSIR/test2.jpg",),
  ("https://mmlspark.blob.core.windows.net/datasets/DSIR/test3.jpg",)
], ["url"])

detector = (DetectFace()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setImageUrlCol("url")
    .setOutputCol("detected_faces")
    .setReturnFaceId(True)
    .setReturnFaceLandmarks(False)
    .setReturnFaceAttributes([]))

faceIdDF = detector.transform(df).select("detected_faces").select(col("detected_faces").getItem(0).getItem("faceId").alias("id"))
faceIds = [row.asDict()['id'] for row in faceIdDF.collect()]

findSimilar = (FindSimilarFace()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setOutputCol("similar")
    .setFaceIdCol("id")
    .setFaceIds(faceIds))

display(findSimilar.transform(faceIdDF))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val df: DataFrame = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg",
    "https://mmlspark.blob.core.windows.net/datasets/DSIR/test2.jpg",
    "https://mmlspark.blob.core.windows.net/datasets/DSIR/test3.jpg"
  ).toDF("url")
val detector = (new DetectFace()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setImageUrlCol("url")
    .setOutputCol("detected_faces")
    .setReturnFaceId(true)
    .setReturnFaceLandmarks(false)
    .setReturnFaceAttributes(Seq()))

val faceIdDF = (detector.transform(df)
    .select(col("detected_faces").getItem(0).getItem("faceId").alias("id"))
    .cache())
val faceIds = faceIdDF.collect().map(row => row.getAs[String]("id"))

val findSimilar = (new FindSimilarFace()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setOutputCol("similar")
    .setFaceIdCol("id")
    .setFaceIds(faceIds))

display(findSimilar.transform(faceIdDF))
```

</TabItem>
</Tabs>

<DocTable className="FindSimilarFace"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.FindSimilarFace"
scala="com/microsoft/azure/synapse/ml/cognitive/FindSimilarFace.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/Face.scala" />


### GroupFaces

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
  ("https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg",),
  ("https://mmlspark.blob.core.windows.net/datasets/DSIR/test2.jpg",),
  ("https://mmlspark.blob.core.windows.net/datasets/DSIR/test3.jpg",)
], ["url"])

detector = (DetectFace()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setImageUrlCol("url")
    .setOutputCol("detected_faces")
    .setReturnFaceId(True)
    .setReturnFaceLandmarks(False)
    .setReturnFaceAttributes([]))

faceIdDF = detector.transform(df).select("detected_faces").select(col("detected_faces").getItem(0).getItem("faceId").alias("id"))
faceIds = [row.asDict()['id'] for row in faceIdDF.collect()]

group = (GroupFaces()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setOutputCol("grouping")
    .setFaceIds(faceIds))

display(group.transform(faceIdDF))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val df: DataFrame = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg",
    "https://mmlspark.blob.core.windows.net/datasets/DSIR/test2.jpg",
    "https://mmlspark.blob.core.windows.net/datasets/DSIR/test3.jpg"
  ).toDF("url")
val detector = (new DetectFace()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setImageUrlCol("url")
    .setOutputCol("detected_faces")
    .setReturnFaceId(true)
    .setReturnFaceLandmarks(false)
    .setReturnFaceAttributes(Seq()))

val faceIdDF = (detector.transform(df)
    .select(col("detected_faces").getItem(0).getItem("faceId").alias("id"))
    .cache())
val faceIds = faceIdDF.collect().map(row => row.getAs[String]("id"))

val group = (new GroupFaces()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setOutputCol("grouping")
    .setFaceIds(faceIds))

display(group.transform(faceIdDF))
```

</TabItem>
</Tabs>

<DocTable className="GroupFaces"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.GroupFaces"
scala="com/microsoft/azure/synapse/ml/cognitive/GroupFaces.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/Face.scala" />


### IdentifyFaces

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
pgId = "PUT_YOUR_PERSON_GROUP_ID"

identifyFaces = (IdentifyFaces()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setFaceIdsCol("faces")
    .setPersonGroupId(pgId)
    .setOutputCol("identified_faces"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val pgId = "PUT_YOUR_PERSON_GROUP_ID"

val identifyFaces = (new IdentifyFaces()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setFaceIdsCol("faces")
    .setPersonGroupId(pgId)
    .setOutputCol("identified_faces"))
```

</TabItem>
</Tabs>

<DocTable className="IdentifyFaces"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.IdentifyFaces"
scala="com/microsoft/azure/synapse/ml/cognitive/IdentifyFaces.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/Face.scala" />


### VerifyFaces

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
  ("https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg",),
  ("https://mmlspark.blob.core.windows.net/datasets/DSIR/test2.jpg",),
  ("https://mmlspark.blob.core.windows.net/datasets/DSIR/test3.jpg",)
], ["url"])

detector = (DetectFace()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setImageUrlCol("url")
    .setOutputCol("detected_faces")
    .setReturnFaceId(True)
    .setReturnFaceLandmarks(False)
    .setReturnFaceAttributes([]))

faceIdDF = detector.transform(df).select("detected_faces").select(col("detected_faces").getItem(0).getItem("faceId").alias("faceId1"))
faceIdDF2 = faceIdDF.withColumn("faceId2", lit(faceIdDF.take(1)[0].asDict()['faceId1']))

verify = (VerifyFaces()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setOutputCol("same")
    .setFaceId1Col("faceId1")
    .setFaceId2Col("faceId2"))

display(verify.transform(faceIdDF2))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val df: DataFrame = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg",
    "https://mmlspark.blob.core.windows.net/datasets/DSIR/test2.jpg",
    "https://mmlspark.blob.core.windows.net/datasets/DSIR/test3.jpg"
  ).toDF("url")

val detector = (new DetectFace()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setImageUrlCol("url")
    .setOutputCol("detected_faces")
    .setReturnFaceId(true)
    .setReturnFaceLandmarks(false)
    .setReturnFaceAttributes(Seq()))

val faceIdDF = (detector.transform(df)
    .select(col("detected_faces").getItem(0).getItem("faceId").alias("faceId1"))
    .cache())
val faceIdDF2 = faceIdDF.withColumn("faceId2", lit(faceIdDF.take(1).head.getString(0)))

val verify = (new VerifyFaces()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setOutputCol("same")
    .setFaceId1Col("faceId1")
    .setFaceId2Col("faceId2"))

display(verify.transform(faceIdDF2))
```

</TabItem>
</Tabs>

<DocTable className="VerifyFaces"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.VerifyFaces"
scala="com/microsoft/azure/synapse/ml/cognitive/VerifyFaces.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/Face.scala" />


