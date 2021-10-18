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

## VowpalWabbitRegressor

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.vw import *

vw = (VowpalWabbitRegressor()
      .setLabelCol("Y1")
      .setFeaturesCol("features")
      .setPredictionCol("pred"))

vwRegressor = (VowpalWabbitRegressor()
      .setNumPasses(20)
      .setArgs("--holdout_off --loss_function quantile -q :: -l 0.1"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.vw._

val vw = (new VowpalWabbitRegressor()
      .setLabelCol("Y1")
      .setFeaturesCol("features")
      .setPredictionCol("pred"))

val vwRegressor = (new VowpalWabbitRegressor()
      .setNumPasses(20)
      .setArgs("--holdout_off --loss_function quantile -q :: -l 0.1"))

```

</TabItem>
</Tabs>

<DocTable className="VowpalWabbitRegressor"
py="mmlspark.vw.html#module-mmlspark.vw.VowpalWabbitRegressor"
scala="com/microsoft/ml/spark/vw/VowpalWabbitRegressor.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/vw/src/main/scala/com/microsoft/azure/synapse/ml/vw/VowpalWabbitRegressor.scala" />


## VowpalWabbitContextualBandit

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.vw import *

cb = (VowpalWabbitContextualBandit()
      .setArgs("--cb_explore_adf --epsilon 0.2 --quiet")
      .setLabelCol("cost")
      .setProbabilityCol("prob")
      .setChosenActionCol("chosen_action")
      .setSharedCol("shared_features")
      .setFeaturesCol("action_features")
      .setUseBarrierExecutionMode(False))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.vw._

val cb = (new VowpalWabbitContextualBandit()
      .setArgs("--cb_explore_adf --epsilon 0.2 --quiet")
      .setLabelCol("cost")
      .setProbabilityCol("prob")
      .setChosenActionCol("chosen_action")
      .setSharedCol("shared_features")
      .setFeaturesCol("action_features")
      .setUseBarrierExecutionMode(false))

```

</TabItem>
</Tabs>

<DocTable className="VowpalWabbitContextualBandit"
py="mmlspark.vw.html#module-mmlspark.vw.VowpalWabbitContextualBandit"
scala="com/microsoft/ml/spark/vw/VowpalWabbitContextualBandit.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/vw/src/main/scala/com/microsoft/azure/synapse/ml/vw/VowpalWabbitContextualBandit.scala" />

