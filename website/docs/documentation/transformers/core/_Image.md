import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";




## Image

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
from synapse.ml.image import *

spt = (SuperpixelTransformer()
      .setInputCol("images"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.image._

val spt = (new SuperpixelTransformer()
      .setInputCol("images"))
```

</TabItem>
</Tabs>

<DocTable className="SuperpixelTransformer"
py="synapse.ml.lime.html#module-synapse.ml.image.SuperpixelTransformer"
scala="com/microsoft/azure/synapse/ml/image/SuperpixelTransformer.html"
csharp="classSynapse_1_1ML_1_1Image_1_1SuperpixelTransformer.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/image/SuperpixelTransformer.scala" />
