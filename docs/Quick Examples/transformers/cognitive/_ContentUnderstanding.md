import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";

## Content Understanding

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.services.contentunderstanding import ContentUnderstanding

documents = spark.createDataFrame(
    [("invoice-001", "https://example.com/invoice.pdf")],
    ["documentId", "documentUrl"],
)

analyzer = (
    ContentUnderstanding()
    .setEndpoint("https://your-resource.services.ai.azure.com")
    .setAnalyzerId("prebuilt-read")
    .setDocumentUrlCol("documentUrl")
    .setOutputCol("analysis")
    .setErrorCol("error")
    .setConcurrency(2)
)

results = analyzer.transform(documents)
results.printSchema()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.services.contentunderstanding.ContentUnderstanding
import spark.implicits._

val documents = Seq(
  ("invoice-001", "https://example.com/invoice.pdf")
).toDF("documentId", "documentUrl")

val analyzer = new ContentUnderstanding()
  .setEndpoint("https://your-resource.services.ai.azure.com")
  .setAnalyzerId("prebuilt-read")
  .setDocumentUrlCol("documentUrl")
  .setOutputCol("analysis")
  .setErrorCol("error")
  .setConcurrency(2)

val results = analyzer.transform(documents)
results.printSchema()
```

</TabItem>
</Tabs>

<DocTable className="ContentUnderstanding"
docsVersion="1.1.3-328-9964d523-SNAPSHOT"
py="synapse.ml.services.contentunderstanding.html#module-synapse.ml.services.contentunderstanding.ContentUnderstanding"
scala="com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/bc1371aa9f1a668accb054ffa45bf85d571968ab/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala" />
