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

## SuperpixelTransformer

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from mmlspark.lime import *

spt = (SuperpixelTransformer()
      .setInputCol("images"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.lime._

val spt = (new SuperpixelTransformer()
      .setInputCol("images"))
```

</TabItem>
</Tabs>

<DocTable className="SuperpixelTransformer"
py="mmlspark.lime.html#module-mmlspark.lime.SuperpixelTransformer"
scala="com/microsoft/ml/spark/lime/SuperpixelTransformer.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/ml/spark/lime/SuperpixelTransformer.scala" />



