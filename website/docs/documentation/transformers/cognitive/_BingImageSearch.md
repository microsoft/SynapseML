import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";

<!--
```python
import pyspark
import os
import json
from IPython.display import display

os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "jupyter"
os.environ["PYSPARK_DRIVER_PYTHON_OPTS"] = "notebook"

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

## BingImageSearch

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

bingSearchKey = os.environ.get("BING_SEARCH_KEY", getSecret("bing-search-key"))

# Number of images Bing will return per query
imgsPerBatch = 10
# A list of offsets, used to page into the search results
offsets = [(i*imgsPerBatch,) for i in range(100)]
# Since web content is our data, we create a dataframe with options on that data: offsets
bingParameters = spark.createDataFrame(offsets, ["offset"])

# Run the Bing Image Search service with our text query
bingSearch = (BingImageSearch()
              .setSubscriptionKey(bingSearchKey)
              .setOffsetCol("offset")
              .setQuery("Martin Luther King Jr. quotes")
              .setCount(imgsPerBatch)
              .setOutputCol("images"))

# Transformer that extracts and flattens the richly structured output of Bing Image Search into a simple URL column
getUrls = BingImageSearch.getUrlTransformer("images", "url")

# This displays the full results returned
display(bingSearch.transform(bingParameters))

# Since we have two services, they are put into a pipeline
pipeline = PipelineModel(stages=[bingSearch, getUrls])

# Show the results of your search: image URLs
display(pipeline.transform(bingParameters))

```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val bingSearchKey = sys.env.getOrElse("BING_SEARCH_KEY", None)

// Number of images Bing will return per query
val imgsPerBatch = 10
// A list of offsets, used to page into the search results
val offsets = (0 until 100).map(i => i*imgsPerBatch)
// Since web content is our data, we create a dataframe with options on that data: offsets
val bingParameters = Seq(offsets).toDF("offset")

// Run the Bing Image Search service with our text query
val bingSearch = (new BingImageSearch()
              .setSubscriptionKey(bingSearchKey)
              .setOffsetCol("offset")
              .setQuery("Martin Luther King Jr. quotes")
              .setCount(imgsPerBatch)
              .setOutputCol("images"))

// Transformer that extracts and flattens the richly structured output of Bing Image Search into a simple URL column
val getUrls = BingImageSearch.getUrlTransformer("images", "url")

// This displays the full results returned
display(bingSearch.transform(bingParameters))

// Show the results of your search: image URLs
display(getUrls.transform(bingSearch.transform(bingParameters)))
```

</TabItem>
</Tabs>

<DocTable className="BingImageSearch"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.BingImageSearch"
scala="com/microsoft/azure/synapse/ml/cognitive/BingImageSearch.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/com/microsoft/azure/synapse/ml/cognitive/BingImageSearch.scala" />
