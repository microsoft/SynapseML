import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";

<!-- 
```python
import pyspark
import os
spark = (pyspark.sql.SparkSession.builder.appName("MyApp")
        .config("spark.jars.packages", "com.microsoft.ml.spark:mmlspark:1.0.0-rc4")
        .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven")
        .getOrCreate())
``` 
-->

## ONNXModel

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

```py
import mmlspark
from mmlspark.onnx import ONNXModel

model_path = "PUT_YOUR_MODEL_PATH"
onnx_ml = (ONNXModel()
            .setModelLocation(model_path)
            .setFeedDict({"float_input": "features"})
            .setFetchDict({"prediction": "output_label", "rawProbability": "output_probability"})
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.onnx._

val model_path = "PUT_YOUR_MODEL_PATH"
val onnx_ml = new ONNXModel()
                  .setModelLocation(model_path)
                  .setFeedDict(Map("float_input" -> "features"))
                  .setFetchDict(Map("prediction" -> "output_label", "rawProbability" -> "output_probability"))
```

</TabItem>
</Tabs>

<DocTable className="ONNXModel"
py="mmlspark.onnx.html#module-mmlspark.onnx.ONNXModel"
scala="com/microsoft/ml/spark/onnx/ONNXModel.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/deep-learning/src/main/scala/com/microsoft/ml/spark/onnx/ONNXModel.scala" />

