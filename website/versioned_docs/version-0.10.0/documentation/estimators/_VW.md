import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";




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
      .setPassThroughArgs("--holdout_off --loss_function quantile -q :: -l 0.1"))
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
  .setPassThroughArgs("--holdout_off --loss_function quantile -q :: -l 0.1"))

```

</TabItem>
</Tabs>

<DocTable className="VowpalWabbitRegressor"
py="synapse.ml.vw.html#module-synapse.ml.vw.VowpalWabbitRegressor"
scala="com/microsoft/azure/synapse/ml/vw/VowpalWabbitRegressor.html"
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
      .setPassThroughArgs("--cb_explore_adf --epsilon 0.2 --quiet")
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
  .setPassThroughArgs("--cb_explore_adf --epsilon 0.2 --quiet")
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
py="synapse.ml.vw.html#module-synapse.ml.vw.VowpalWabbitContextualBandit"
scala="com/microsoft/azure/synapse/ml/vw/VowpalWabbitContextualBandit.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/vw/src/main/scala/com/microsoft/azure/synapse/ml/vw/VowpalWabbitContextualBandit.scala" />
