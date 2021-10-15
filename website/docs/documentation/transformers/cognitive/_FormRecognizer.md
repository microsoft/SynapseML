import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";

<!-- 
```python
import pyspark
import os
import json
from IPython.display import display
from pyspark.sql.functions import col, flatten, regexp_replace, explode, create_map, lit

spark = (pyspark.sql.SparkSession.builder.appName("MyApp")
        .config("spark.jars.packages", "com.microsoft.ml.spark:mmlspark:1.0.0-rc4")
        .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven")
        .getOrCreate())

def getSecret(secretName):
        get_secret_cmd = 'az keyvault secret show --vault-name mmlspark-build-keys --name {}'.format(secretName)
        value = json.loads(os.popen(get_secret_cmd).read())["value"]
        return value

import mmlspark
``` 
-->

## AnalyzeLayout

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
imageDf = spark.createDataFrame([
  ("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/layout1.jpg",)
], ["source",])

analyzeLayout = (AnalyzeLayout()
            .setSubscriptionKey(cognitiveKey)
            .setLocation("eastus")
            .setImageUrlCol("source")
            .setOutputCol("layout")
            .setConcurrency(5))

display(analyzeLayout
        .transform(imageDf)
        .withColumn("lines", flatten(col("layout.analyzeResult.readResults.lines")))
        .withColumn("readLayout", col("lines.text"))
        .withColumn("tables", flatten(col("layout.analyzeResult.pageResults.tables")))
        .withColumn("cells", flatten(col("tables.cells")))
        .withColumn("pageLayout", col("cells.text"))
        .select("source", "readLayout", "pageLayout"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
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

display(analyzeLayout.transform(imageDf)
```

</TabItem>
</Tabs>

<DocTable className="AnalyzeLayout"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.AnalyzeLayout"
scala="com/microsoft/ml/spark/cognitive/AnalyzeLayout.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/FormRecognizer.scala" />


## AnalyzeReceipts

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
imageDf = spark.createDataFrame([
  ("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/receipt1.png",),
  ("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/receipt1.png",)
], ["image",])

analyzeReceipts = (AnalyzeReceipts()
                  .setSubscriptionKey(cognitiveKey)
                  .setLocation("eastus")
                  .setImageUrlCol("source")
                  .setOutputCol("receipts")
                  .setConcurrency(5))

display(analyzeReceipts.transform(imageDf))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
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

display(analyzeReceipts.transform(imageDf))
```

</TabItem>
</Tabs>

<DocTable className="AnalyzeReceipts"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.AnalyzeReceipts"
scala="com/microsoft/ml/spark/cognitive/AnalyzeReceipts.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/FormRecognizer.scala" />


## AnalyzeBusinessCards

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
imageDf = spark.createDataFrame([
  ("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/business_card.jpg",)
], ["source",])

analyzeBusinessCards = (AnalyzeBusinessCards()
                        .setSubscriptionKey(cognitiveKey)
                        .setLocation("eastus")
                        .setImageUrlCol("source")
                        .setOutputCol("businessCards")
                        .setConcurrency(5))

display(analyzeBusinessCards.transform(imageDf)
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
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

display(analyzeBusinessCards.transform(imageDf)
```

</TabItem>
</Tabs>

<DocTable className="AnalyzeBusinessCards"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.AnalyzeBusinessCards"
scala="com/microsoft/ml/spark/cognitive/AnalyzeBusinessCards.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/FormRecognizer.scala" />


## AnalyzeInvoices

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
imageDf = spark.createDataFrame([
  ("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/invoice2.png",)
], ["source",])

analyzeInvoices = (AnalyzeInvoices()
                  .setSubscriptionKey(cognitiveKey)
                  .setLocation("eastus")
                  .setImageUrlCol("source")
                  .setOutputCol("invoices")
                  .setConcurrency(5))

display(analyzeInvoices
        .transform(imageDf)
        .withColumn("documents", explode(col("invoices.analyzeResult.documentResults.fields")))
        .select("source", "documents"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
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

display(analyzeInvoices.transform(imageD4))
```

</TabItem>
</Tabs>

<DocTable className="AnalyzeInvoices"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.AnalyzeInvoices"
scala="com/microsoft/ml/spark/cognitive/AnalyzeInvoices.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/FormRecognizer.scala" />


## AnalyzeIDDocuments

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
imageDf = spark.createDataFrame([
  ("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/id1.jpg",)
], ["source",])

analyzeIDDocuments = (AnalyzeIDDocuments()
                  .setSubscriptionKey(cognitiveKey)
                  .setLocation("eastus")
                  .setImageUrlCol("source")
                  .setOutputCol("ids")
                  .setConcurrency(5))

display(analyzeIDDocuments
        .transform(imageDf)
        .withColumn("documents", explode(col("ids.analyzeResult.documentResults.fields")))
        .select("source", "documents"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
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

display(analyzeIDDocuments.transform(imageDf))
```

</TabItem>
</Tabs>

<DocTable className="AnalyzeIDDocuments"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.AnalyzeIDDocuments"
scala="com/microsoft/ml/spark/cognitive/AnalyzeIDDocuments.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/FormRecognizer.scala" />


## AnalyzeCustomModel

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

display(analyzeCustomModel
        .transform(imageDf)
        .withColumn("keyValuePairs", flatten(col("output.analyzeResult.pageResults.keyValuePairs")))
        .withColumn("keys", col("keyValuePairs.key.text"))
        .withColumn("values", col("keyValuePairs.value.text"))
        .withColumn("keyValuePairs", create_map(lit("key"), col("keys"), lit("value"), col("values")))
        .select("source", "keyValuePairs"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
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

display(analyzeCustomModel.transform(imageDf))
```

</TabItem>
</Tabs>

<DocTable className="AnalyzeCustomModel"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.AnalyzeCustomModel"
scala="com/microsoft/ml/spark/cognitive/AnalyzeCustomModel.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/FormRecognizer.scala" />


## GetCustomModel

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
modelId = "02bc2f58-2beb-4ae3-84fb-08f011b2f7b8" # put your own modelId here
emptyDf = spark.createDataFrame([("",)])

getCustomModel = (GetCustomModel()
                  .setSubscriptionKey(cognitiveKey)
                  .setLocation("eastus")
                  .setModelId(modelId)
                  .setIncludeKeys(true)
                  .setOutputCol("model")
                  .setConcurrency(5))

display(getCustomModel
        .transform(emptyDf)
        .withColumn("modelInfo", col("model.ModelInfo"))
        .withColumn("trainResult", col("model.TrainResult"))
        .select("modelInfo", "trainResult"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
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

display(getCustomModel.transform(emptyDf))
```

</TabItem>
</Tabs>

<DocTable className="GetCustomModel"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.GetCustomModel"
scala="com/microsoft/ml/spark/cognitive/GetCustomModel.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/FormRecognizer.scala" />


## ListCustomModels

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
emptyDf = spark.createDataFrame([("",)])

listCustomModels = (ListCustomModels()
                  .setSubscriptionKey(cognitiveKey)
                  .setLocation("eastus")
                  .setOp("full")
                  .setOutputCol("models")
                  .setConcurrency(5))

display(listCustomModels
       .transform(emptyDf)
       .withColumn("modelIds", col("models.modelList.modelId"))
       .select("modelIds"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
import spark.implicits._

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val emptyDf = Seq("").toDF()

val listCustomModels = (new ListCustomModels()
                        .setSubscriptionKey(cognitiveKey)
                        .setLocation("eastus")
                        .setOp("full")
                        .setOutputCol("models")
                        .setConcurrency(5))

display(listCustomModels.transform(emptyDf))
```

</TabItem>
</Tabs>

<DocTable className="ListCustomModels"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.ListCustomModels"
scala="com/microsoft/ml/spark/cognitive/ListCustomModels.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/FormRecognizer.scala" />


