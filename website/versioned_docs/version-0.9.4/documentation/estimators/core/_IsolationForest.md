import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";




## Isolation Forest

### IsolationForest

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.isolationforest import *

isolationForest = (IsolationForest()
      .setNumEstimators(100)
      .setBootstrap(False)
      .setMaxSamples(256)
      .setMaxFeatures(1.0)
      .setFeaturesCol("features")
      .setPredictionCol("predictedLabel")
      .setScoreCol("outlierScore")
      .setContamination(0.02)
      .setContaminationError(0.02 * 0.01)
      .setRandomSeed(1))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.isolationforest._
import spark.implicits._

val isolationForest = (new IsolationForest()
      .setNumEstimators(100)
      .setBootstrap(false)
      .setMaxSamples(256)
      .setMaxFeatures(1.0)
      .setFeaturesCol("features")
      .setPredictionCol("predictedLabel")
      .setScoreCol("outlierScore")
      .setContamination(0.02)
      .setContaminationError(0.02 * 0.01)
      .setRandomSeed(1))
```

</TabItem>
</Tabs>

<DocTable className="IsolationForest"
py="synapse.ml.isolationforest.html#module-synapse.ml.isolationforest.IsolationForest"
scala="com/microsoft/azure/synapse/ml/isolationforest/IsolationForest.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/isolationforest/IsolationForest.scala" />
