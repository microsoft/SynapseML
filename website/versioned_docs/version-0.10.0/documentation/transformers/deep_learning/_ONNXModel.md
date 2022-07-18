import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";


## ONNXModel

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

```py
from synapse.ml.onnx import ONNXModel

model_path = "PUT_YOUR_MODEL_PATH"
onnx_ml = (ONNXModel()
            .setModelLocation(model_path)
            .setFeedDict({"float_input": "features"})
            .setFetchDict({"prediction": "output_label", "rawProbability": "output_probability"}))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.onnx._

val model_path = "PUT_YOUR_MODEL_PATH"
val onnx_ml = (new ONNXModel()
                  .setModelLocation(model_path)
                  .setFeedDict(Map("float_input" -> "features"))
                  .setFetchDict(Map("prediction" -> "output_label", "rawProbability" -> "output_probability")))
```

</TabItem>
</Tabs>

<DocTable className="ONNXModel"
py="synapse.ml.onnx.html#module-synapse.ml.onnx.ONNXModel"
scala="com/microsoft/azure/synapse/ml/onnx/ONNXModel.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/deep-learning/src/main/scala/com/microsoft/azure/synapse/ml/onnx/ONNXModel.scala" />
