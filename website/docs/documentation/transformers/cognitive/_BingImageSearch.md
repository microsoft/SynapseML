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
from mmlspark.cognitive import *

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
import com.microsoft.ml.spark.cognitive._
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
py="mmlspark.cognitive.html#module-mmlspark.cognitive.BingImageSearch"
scala="com/microsoft/ml/spark/cognitive/BingImageSearch.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/BingImageSearch.scala" />
