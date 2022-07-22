import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";




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
py="synapse.ml.nn.html#module-synapse.ml.nn.ConditionalKNN"
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
py="synapse.ml.nn.html#module-synapse.ml.nn.KNN"
scala="com/microsoft/azure/synapse/ml/nn/KNN.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/nn/KNN.scala" />
