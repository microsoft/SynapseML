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

os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "jupyter"
os.environ["PYSPARK_DRIVER_PYTHON_OPTS"] = "notebook"

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

## AzureSearch

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

display(ad.transform(df))

AzureSearchWriter.writeToAzureSearch(df,
    subscriptionKey=azureSearchKey,
    actionCol="searchAction",
    serviceName=testServiceName,
    indexJson=createSimpleIndexJson(indexName))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
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

display(ad.transform(df))

AzureSearchWriter.write(df,
      Map("subscriptionKey" -> azureSearchKey,
        "actionCol" -> "searchAction",
        "serviceName" -> testServiceName,
        "indexJson" -> createSimpleIndexJson(indexName)))
```

</TabItem>
</Tabs>

<DocTable className="AzureSearch"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.AzureSearch"
scala="com/microsoft/ml/spark/cognitive/AzureSearch.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/AzureSearch.scala" />
