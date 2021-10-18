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

## LightGBMClassifier

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.lightgbm import *

lgbmClassifier = (LightGBMClassifier()
      .setFeaturesCol("features")
      .setRawPredictionCol("rawPrediction")
      .setDefaultListenPort(12402)
      .setNumLeaves(5)
      .setNumIterations(10)
      .setObjective("binary")
      .setLabelCol("labels")
      .setLeafPredictionCol("leafPrediction")
      .setFeaturesShapCol("featuresShap"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.lightgbm._

val lgbmClassifier = (new LightGBMClassifier()
      .setFeaturesCol("features")
      .setRawPredictionCol("rawPrediction")
      .setDefaultListenPort(12402)
      .setNumLeaves(5)
      .setNumIterations(10)
      .setObjective("binary")
      .setLabelCol("labels")
      .setLeafPredictionCol("leafPrediction")
      .setFeaturesShapCol("featuresShap"))
```

</TabItem>
</Tabs>

<DocTable className="LightGBMClassifier"
py="synapse.ml.lightgbm.html#module-synapse.ml.lightgbm.LightGBMClassifier"
scala="com/microsoft/azure/synapse/ml/lightgbm/LightGBMClassifier.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMClassifier.scala" />


## LightGBMRanker

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.lightgbm import *

lgbmRanker = (LightGBMRanker()
      .setLabelCol("labels")
      .setFeaturesCol("features")
      .setGroupCol("query")
      .setDefaultListenPort(12402)
      .setRepartitionByGroupingColumn(False)
      .setNumLeaves(5)
      .setNumIterations(10))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.lightgbm._

val lgbmRanker = (new LightGBMRanker()
      .setLabelCol("labels")
      .setFeaturesCol("features")
      .setGroupCol("query")
      .setDefaultListenPort(12402)
      .setRepartitionByGroupingColumn(false)
      .setNumLeaves(5)
      .setNumIterations(10))
```

</TabItem>
</Tabs>

<DocTable className="LightGBMRanker"
py="synapse.ml.lightgbm.html#module-synapse.ml.lightgbm.LightGBMRanker"
scala="com/microsoft/azure/synapse/ml/lightgbm/LightGBMRanker.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMRanker.scala" />


## LightGBMRegressor

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.lightgbm import *

lgbmRegressor = (LightGBMRegressor()
      .setLabelCol("labels")
      .setFeaturesCol("features")
      .setDefaultListenPort(12402)
      .setNumLeaves(5)
      .setNumIterations(10))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.lightgbm._

val lgbmRegressor = (new LightGBMRegressor()
      .setLabelCol("labels")
      .setFeaturesCol("features")
      .setDefaultListenPort(12402)
      .setNumLeaves(5)
      .setNumIterations(10))
```

</TabItem>
</Tabs>

<DocTable className="LightGBMRegressor"
py="synapse.ml.lightgbm.html#module-synapse.ml.lightgbm.LightGBMRegressor"
scala="com/microsoft/azure/synapse/ml/lightgbm/LightGBMRegressor.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/lightgbm/src/main/scala/com/microsoft/azure/synapse/ml/lightgbm/LightGBMRegressor.scala" />





