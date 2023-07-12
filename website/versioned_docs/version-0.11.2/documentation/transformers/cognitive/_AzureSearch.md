import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";


## Azure Search

### AzureSearch

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

azureSearchKey = os.environ.get("AZURE_SEARCH_KEY", getSecret("azure-search-key"))
testServiceName = "mmlspark-azure-search"

indexName = "test-website"

def createSimpleIndexJson(indexName):
    json_str = """
       {
           "name": "%s",
           "fields": [
               {
                   "name": "id",
                   "type": "Edm.String",
                   "key": true,
                   "facetable": false
                },
                {
                    "name": "fileName",
                    "type": "Edm.String",
                    "searchable": false,
                    "sortable": false,
                    "facetable": false
                },
                {
                    "name": "text",
                    "type": "Edm.String",
                    "filterable": false,
                    "sortable": false,
                    "facetable": false
                }
            ]
        }
    """

    return json_str % indexName

df = (spark.createDataFrame([
    ("upload", "0", "file0", "text0"),
    ("upload", "1", "file1", "text1"),
    ("upload", "2", "file2", "text2"),
    ("upload", "3", "file3", "text3")
], ["searchAction", "id", "fileName", "text"]))

ad = (AddDocuments()
      .setSubscriptionKey(azureSearchKey)
      .setServiceName(testServiceName)
      .setOutputCol("out")
      .setErrorCol("err")
      .setIndexName(indexName)
      .setActionCol("searchAction"))

ad.transform(df).show()

AzureSearchWriter.writeToAzureSearch(df,
    subscriptionKey=azureSearchKey,
    actionCol="searchAction",
    serviceName=testServiceName,
    indexJson=createSimpleIndexJson(indexName))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive.search.{AddDocuments, AzureSearchWriter}
import spark.implicits._

val azureSearchKey = sys.env.getOrElse("AZURE_SEARCH_KEY", None)
val testServiceName = "mmlspark-azure-search"

val indexName = "test-website"

def createSimpleIndexJson(indexName: String) = {
    s"""
       |{
       |    "name": "$indexName",
       |    "fields": [
       |      {
       |        "name": "id",
       |        "type": "Edm.String",
       |        "key": true,
       |        "facetable": false
       |      },
       |    {
       |      "name": "fileName",
       |      "type": "Edm.String",
       |      "searchable": false,
       |      "sortable": false,
       |      "facetable": false
       |    },
       |    {
       |      "name": "text",
       |      "type": "Edm.String",
       |      "filterable": false,
       |      "sortable": false,
       |      "facetable": false
       |    }
       |    ]
       |  }
    """.stripMargin
}

val df = ((0 until 4)
      .map(i => ("upload", s"$i", s"file$i", s"text$i"))
      .toDF("searchAction", "id", "fileName", "text"))

val ad = (new AddDocuments()
      .setSubscriptionKey(azureSearchKey)
      .setServiceName(testServiceName)
      .setOutputCol("out")
      .setErrorCol("err")
      .setIndexName(indexName)
      .setActionCol("searchAction"))

ad.transform(df).show()

AzureSearchWriter.write(df,
      Map("subscriptionKey" -> azureSearchKey,
        "actionCol" -> "searchAction",
        "serviceName" -> testServiceName,
        "indexJson" -> createSimpleIndexJson(indexName)))
```

</TabItem>
</Tabs>

<DocTable className="AzureSearch"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.AzureSearch"
scala="com/microsoft/azure/synapse/ml/cognitive/AzureSearch.html"
csharp="classSynapse_1_1ML_1_1Cognitive_1_1AddDocuments.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/AzureSearch.scala" />
