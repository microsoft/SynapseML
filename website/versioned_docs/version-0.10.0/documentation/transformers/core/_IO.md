import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";




## IO

### HTTPTransformer

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.io.http import *
from pyspark.sql.functions import udf, col
from requests import Request

def world_bank_request(country):
    return Request("GET", "http://api.worldbank.org/v2/country/{}?format=json".format(country))

df = (spark.createDataFrame([("br",), ("usa",)], ["country"])
      .withColumn("request", http_udf(world_bank_request)(col("country"))))

ht = (HTTPTransformer()
      .setConcurrency(3)
      .setInputCol("request")
      .setOutputCol("response"))

ht.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.io.http._

val ht = (new HTTPTransformer()
      .setConcurrency(3)
      .setInputCol("request")
      .setOutputCol("response"))
```

</TabItem>
</Tabs>

<DocTable className="HTTPTransformer"
py="synapse.ml.io.http.html#module-synapse.ml.io.http.HTTPTransformer"
scala="com/microsoft/azure/synapse/ml/io/http/HTTPTransformer.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/HTTPTransformer.scala" />


### SimpleHTTPTransformer

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.io.http import *
from pyspark.sql.types import StringType, StructType

sht = (SimpleHTTPTransformer()
        .setInputCol("data")
        .setOutputParser(JSONOutputParser()
            .setDataType(StructType().add("blah", StringType())))
        .setUrl("PUT_YOUR_URL")
        .setOutputCol("results")
        .setConcurrency(3))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.io.http._
import org.apache.spark.sql.types.{StringType, StructType}

val sht = (new SimpleHTTPTransformer()
        .setInputCol("data")
        .setOutputParser(new JSONOutputParser()
            .setDataType(new StructType().add("blah", StringType)))
        .setUrl("PUT_YOUR_URL")
        .setOutputCol("results")
        .setConcurrency(3))
```

</TabItem>
</Tabs>

<DocTable className="SimpleHTTPTransformer"
py="synapse.ml.io.http.html#module-synapse.ml.io.http.SimpleHTTPTransformer"
scala="com/microsoft/azure/synapse/ml/io/http/SimpleHTTPTransformer.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/SimpleHTTPTransformer.scala" />


### JSONInputParser

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.io.http import *

jsonIP = (JSONInputParser()
      .setInputCol("data")
      .setOutputCol("out")
      .setUrl("PUT_YOUR_URL"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.io.http._

val jsonIP = (new JSONInputParser()
      .setInputCol("data")
      .setOutputCol("out")
      .setUrl("PUT_YOUR_URL"))
```

</TabItem>
</Tabs>

<DocTable className="JSONInputParser"
py="synapse.ml.io.http.html#module-synapse.ml.io.http.JSONInputParser"
scala="com/microsoft/azure/synapse/ml/io/http/JSONInputParser.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/JSONInputParser.scala" />


### JSONOutputParser

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.io.http import *
from pyspark.sql.types import StringType, StructType

jsonOP = (JSONOutputParser()
      .setDataType(StructType().add("foo", StringType()))
      .setInputCol("unparsedOutput")
      .setOutputCol("parsedOutput"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.io.http._
import org.apache.spark.sql.types.{StringType, StructType}

val jsonOP = (new JSONOutputParser()
      .setDataType(new StructType().add("foo", StringType))
      .setInputCol("unparsedOutput")
      .setOutputCol("parsedOutput"))
```

</TabItem>
</Tabs>

<DocTable className="JSONOutputParser"
py="synapse.ml.io.http.html#module-synapse.ml.io.http.JSONOutputParser"
scala="com/microsoft/azure/synapse/ml/io/http/JSONOutputParser.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/JSONOutputParser.scala" />


### StringOutputParser

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.io.http import *

sop = (StringOutputParser()
      .setInputCol("unparsedOutput")
      .setOutputCol("out"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.io.http._

val sop = (new StringOutputParser()
      .setInputCol("unparsedOutput")
      .setOutputCol("out"))
```

</TabItem>
</Tabs>

<DocTable className="StringOutputParser"
py="synapse.ml.io.http.html#module-synapse.ml.io.http.StringOutputParser"
scala="com/microsoft/azure/synapse/ml/io/http/StringOutputParser.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/StringOutputParser.scala" />


### CustomInputParser

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.io.http import *

cip = (CustomInputParser()
      .setInputCol("data")
      .setOutputCol("out"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.io.http._

val cip = (new CustomInputParser()
      .setInputCol("data")
      .setOutputCol("out")
      .setUDF({ x: Int => new HttpPost(s"http://$x") }))
```

</TabItem>
</Tabs>

<DocTable className="CustomInputParser"
py="synapse.ml.io.http.html#module-synapse.ml.io.http.CustomInputParser"
scala="com/microsoft/azure/synapse/ml/io/http/CustomInputParser.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/CustomInputParser.scala" />


### CustomOutputParser

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.io.http import *

cop = (CustomOutputParser()
      .setInputCol("unparsedOutput")
      .setOutputCol("out"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.io.http._

val cop = (new CustomOutputParser()
      .setInputCol("unparsedOutput")
      .setOutputCol("out"))
```

</TabItem>
</Tabs>

<DocTable className="CustomOutputParser"
py="synapse.ml.io.http.html#module-synapse.ml.io.http.CustomOutputParser"
scala="com/microsoft/azure/synapse/ml/io/http/CustomOutputParser.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/io/http/CustomOutputParser.scala" />
