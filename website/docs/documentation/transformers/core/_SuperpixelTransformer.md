import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";




## LIME

### SuperpixelTransformer

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.lime import *

spt = (SuperpixelTransformer()
      .setInputCol("images"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.lime._

val spt = (new SuperpixelTransformer()
      .setInputCol("images"))
```

</TabItem>
</Tabs>

<DocTable className="SuperpixelTransformer"
py="synapse.ml.lime.html#module-synapse.ml.lime.SuperpixelTransformer"
scala="com/microsoft/azure/synapse/ml/lime/SuperpixelTransformer.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/lime/SuperpixelTransformer.scala" />
