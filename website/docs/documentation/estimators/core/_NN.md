import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";

<!-- 
```python
import pyspark
import os
import json
from IPython.display import display

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

## NN

### ConditionalKNN

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.nn import *

cknn = (ConditionalKNN()
      .setOutputCol("matches")
      .setFeaturesCol("features"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.nn._
import spark.implicits._

val cknn = (new ConditionalKNN()
            .setOutputCol("matches")
            .setFeaturesCol("features"))
```

</TabItem>
</Tabs>

<DocTable className="ConditionalKNN"
py="mmlspark.nn.html#module-mmlspark.nn.ConditionalKNN"
scala="com/microsoft/azure/synapse/ml/nn/ConditionalKNN.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/nn/ConditionalKNN.scala" />


### KNN

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.nn import *

knn = (KNN()
      .setOutputCol("matches"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.nn._
import spark.implicits._

val knn = (new KNN()
      .setOutputCol("matches"))
```

</TabItem>
</Tabs>

<DocTable className="KNN"
py="mmlspark.nn.html#module-mmlspark.nn.KNN"
scala="com/microsoft/azure/synapse/ml/nn/KNN.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/nn/KNN.scala" />



