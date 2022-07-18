import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";


## Form Recognizer

### AnalyzeLayout

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
imageDf = spark.createDataFrame([
  ("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/layout1.jpg",)
], ["source",])

analyzeLayout = (AnalyzeLayout()
            .setSubscriptionKey(cognitiveKey)
            .setLocation("eastus")
            .setImageUrlCol("source")
            .setOutputCol("layout")
            .setConcurrency(5))

(analyzeLayout.transform(imageDf)
        .withColumn("lines", flatten(col("layout.analyzeResult.readResults.lines")))
        .withColumn("readLayout", col("lines.text"))
        .withColumn("tables", flatten(col("layout.analyzeResult.pageResults.tables")))
        .withColumn("cells", flatten(col("tables.cells")))
        .withColumn("pageLayout", col("cells.text"))
        .select("source", "readLayout", "pageLayout")).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val imageDf = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/layout1.jpg"
  ).toDF("source")

val analyzeLayout = (new AnalyzeLayout()
                        .setSubscriptionKey(cognitiveKey)
                        .setLocation("eastus")
                        .setImageUrlCol("source")
                        .setOutputCol("layout")
                        .setConcurrency(5))

analyzeLayout.transform(imageDf).show()
```

</TabItem>
</Tabs>

<DocTable className="AnalyzeLayout"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.AnalyzeLayout"
scala="com/microsoft/azure/synapse/ml/cognitive/AnalyzeLayout.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/FormRecognizer.scala" />


### AnalyzeReceipts

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
imageDf = spark.createDataFrame([
  ("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/receipt1.png",),
  ("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/receipt1.png",)
], ["image",])

analyzeReceipts = (AnalyzeReceipts()
                  .setSubscriptionKey(cognitiveKey)
                  .setLocation("eastus")
                  .setImageUrlCol("image")
                  .setOutputCol("receipts")
                  .setConcurrency(5))

analyzeReceipts.transform(imageDf).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val imageDf = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/receipt1.png",
    "https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/receipt1.png"
  ).toDF("source")

val analyzeReceipts = (new AnalyzeReceipts()
                        .setSubscriptionKey(cognitiveKey)
                        .setLocation("eastus")
                        .setImageUrlCol("source")
                        .setOutputCol("receipts")
                        .setConcurrency(5))

analyzeReceipts.transform(imageDf).show()
```

</TabItem>
</Tabs>

<DocTable className="AnalyzeReceipts"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.AnalyzeReceipts"
scala="com/microsoft/azure/synapse/ml/cognitive/AnalyzeReceipts.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/FormRecognizer.scala" />


### AnalyzeBusinessCards

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
imageDf = spark.createDataFrame([
  ("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/business_card.jpg",)
], ["source",])

analyzeBusinessCards = (AnalyzeBusinessCards()
                        .setSubscriptionKey(cognitiveKey)
                        .setLocation("eastus")
                        .setImageUrlCol("source")
                        .setOutputCol("businessCards")
                        .setConcurrency(5))

analyzeBusinessCards.transform(imageDf).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val imageDf = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/business_card.jpg"
  ).toDF("source")

val analyzeBusinessCards = (new AnalyzeBusinessCards()
                              .setSubscriptionKey(cognitiveKey)
                              .setLocation("eastus")
                              .setImageUrlCol("source")
                              .setOutputCol("businessCards")
                              .setConcurrency(5))

analyzeBusinessCards.transform(imageDf).show()
```

</TabItem>
</Tabs>

<DocTable className="AnalyzeBusinessCards"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.AnalyzeBusinessCards"
scala="com/microsoft/azure/synapse/ml/cognitive/AnalyzeBusinessCards.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/FormRecognizer.scala" />


### AnalyzeInvoices

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
imageDf = spark.createDataFrame([
  ("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/invoice2.png",)
], ["source",])

analyzeInvoices = (AnalyzeInvoices()
                  .setSubscriptionKey(cognitiveKey)
                  .setLocation("eastus")
                  .setImageUrlCol("source")
                  .setOutputCol("invoices")
                  .setConcurrency(5))

(analyzeInvoices
        .transform(imageDf)
        .withColumn("documents", explode(col("invoices.analyzeResult.documentResults.fields")))
        .select("source", "documents")).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val imageDf = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/invoice2.png"
  ).toDF("source")

val analyzeInvoices = (new AnalyzeInvoices()
                        .setSubscriptionKey(cognitiveKey)
                        .setLocation("eastus")
                        .setImageUrlCol("source")
                        .setOutputCol("invoices")
                        .setConcurrency(5))

analyzeInvoices.transform(imageD4).show()
```

</TabItem>
</Tabs>

<DocTable className="AnalyzeInvoices"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.AnalyzeInvoices"
scala="com/microsoft/azure/synapse/ml/cognitive/AnalyzeInvoices.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/FormRecognizer.scala" />


### AnalyzeIDDocuments

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
imageDf = spark.createDataFrame([
  ("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/id1.jpg",)
], ["source",])

analyzeIDDocuments = (AnalyzeIDDocuments()
                  .setSubscriptionKey(cognitiveKey)
                  .setLocation("eastus")
                  .setImageUrlCol("source")
                  .setOutputCol("ids")
                  .setConcurrency(5))

(analyzeIDDocuments
        .transform(imageDf)
        .withColumn("documents", explode(col("ids.analyzeResult.documentResults.fields")))
        .select("source", "documents")).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val imageDf = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/id1.jpg"
  ).toDF("source")

val analyzeIDDocuments = (new AnalyzeIDDocuments()
                        .setSubscriptionKey(cognitiveKey)
                        .setLocation("eastus")
                        .setImageUrlCol("source")
                        .setOutputCol("ids")
                        .setConcurrency(5))

analyzeIDDocuments.transform(imageDf).show()
```

</TabItem>
</Tabs>

<DocTable className="AnalyzeIDDocuments"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.AnalyzeIDDocuments"
scala="com/microsoft/azure/synapse/ml/cognitive/AnalyzeIDDocuments.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/FormRecognizer.scala" />


### AnalyzeCustomModel

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
modelId = "02bc2f58-2beb-4ae3-84fb-08f011b2f7b8" # put your own modelId here
imageDf = spark.createDataFrame([
  ("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/invoice2.png",)
], ["source",])

analyzeCustomModel = (AnalyzeCustomModel()
                 .setSubscriptionKey(cognitiveKey)
                 .setLocation("eastus")
                 .setModelId(modelId)
                 .setImageUrlCol("source")
                 .setOutputCol("output")
                 .setConcurrency(5))

(analyzeCustomModel
        .transform(imageDf)
        .withColumn("keyValuePairs", flatten(col("output.analyzeResult.pageResults.keyValuePairs")))
        .withColumn("keys", col("keyValuePairs.key.text"))
        .withColumn("values", col("keyValuePairs.value.text"))
        .withColumn("keyValuePairs", create_map(lit("key"), col("keys"), lit("value"), col("values")))
        .select("source", "keyValuePairs")).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val modelId = "02bc2f58-2beb-4ae3-84fb-08f011b2f7b8" // put your own modelId here
val imageDf = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/invoice2.png"
  ).toDF("source")

val analyzeCustomModel = (new AnalyzeCustomModel()
                        .setSubscriptionKey(cognitiveKey)
                        .setLocation("eastus")
                        .setModelId(modelId)
                        .setImageUrlCol("source")
                        .setOutputCol("output")
                        .setConcurrency(5))

analyzeCustomModel.transform(imageDf).show()
```

</TabItem>
</Tabs>

<DocTable className="AnalyzeCustomModel"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.AnalyzeCustomModel"
scala="com/microsoft/azure/synapse/ml/cognitive/AnalyzeCustomModel.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/FormRecognizer.scala" />


### GetCustomModel

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
modelId = "02bc2f58-2beb-4ae3-84fb-08f011b2f7b8" # put your own modelId here
emptyDf = spark.createDataFrame([("",)])

getCustomModel = (GetCustomModel()
                  .setSubscriptionKey(cognitiveKey)
                  .setLocation("eastus")
                  .setModelId(modelId)
                  .setIncludeKeys(True)
                  .setOutputCol("model")
                  .setConcurrency(5))

(getCustomModel
        .transform(emptyDf)
        .withColumn("modelInfo", col("model.ModelInfo"))
        .withColumn("trainResult", col("model.TrainResult"))
        .select("modelInfo", "trainResult")).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val modelId = "02bc2f58-2beb-4ae3-84fb-08f011b2f7b8" // put your own modelId here
val emptyDf = Seq("").toDF()

val getCustomModel = (new GetCustomModel()
                        .setSubscriptionKey(cognitiveKey)
                        .setLocation("eastus")
                        .setModelId(modelId)
                        .setIncludeKeys(true)
                        .setOutputCol("model")
                        .setConcurrency(5))

getCustomModel.transform(emptyDf).show()
```

</TabItem>
</Tabs>

<DocTable className="GetCustomModel"
py="synapse.ml.cognitive.html#module-mmlspark.cognitive.GetCustomModel"
scala="com/microsoft/azure/synapse/ml/cognitive/GetCustomModel.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/FormRecognizer.scala" />


### ListCustomModels

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
emptyDf = spark.createDataFrame([("",)])

listCustomModels = (ListCustomModels()
                  .setSubscriptionKey(cognitiveKey)
                  .setLocation("eastus")
                  .setOp("full")
                  .setOutputCol("models")
                  .setConcurrency(5))

(listCustomModels
       .transform(emptyDf)
       .withColumn("modelIds", col("models.modelList.modelId"))
       .select("modelIds")).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val emptyDf = Seq("").toDF()

val listCustomModels = (new ListCustomModels()
                        .setSubscriptionKey(cognitiveKey)
                        .setLocation("eastus")
                        .setOp("full")
                        .setOutputCol("models")
                        .setConcurrency(5))

listCustomModels.transform(emptyDf).show()
```

</TabItem>
</Tabs>

<DocTable className="ListCustomModels"
py="synapse.ml.cognitive.html#module-mmlspark.cognitive.ListCustomModels"
scala="com/microsoft/azure/synapse/ml/cognitive/ListCustomModels.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/FormRecognizer.scala" />


## Form Recognizer V3

### AnalyzeDocument

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
imageDf = spark.createDataFrame([
  ("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/layout1.jpg",)
], ["source",])

analyzeDocument = (AnalyzeDocument()
            # For supported prebuilt models, please go to documentation page for details
            .setPrebuiltModelId("prebuilt-layout")
            .setSubscriptionKey(cognitiveKey)
            .setLocation("eastus")
            .setImageUrlCol("source")
            .setOutputCol("result")
            .setConcurrency(5))

(analyzeDocument.transform(imageDf)
        .withColumn("content", col("result.analyzeResult.content"))
        .withColumn("cells", flatten(col("result.analyzeResult.tables.cells")))
        .withColumn("cells", col("cells.content"))
        .select("source", "result", "content", "cells")).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val imageDf = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/layout1.jpg"
  ).toDF("source")

val analyzeDocument = (new AnalyzeDocument()
                        .setPrebuiltModelId("prebuilt-layout")
                        .setSubscriptionKey(cognitiveKey)
                        .setLocation("eastus")
                        .setImageUrlCol("source")
                        .setOutputCol("result")
                        .setConcurrency(5))

analyzeDocument.transform(imageDf).show()
```

</TabItem>
</Tabs>

<DocTable className="AnalyzeDocument"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.AnalyzeDocument"
scala="com/microsoft/azure/synapse/ml/cognitive/AnalyzeDocument.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/FormRecognizerV3.scala" />
