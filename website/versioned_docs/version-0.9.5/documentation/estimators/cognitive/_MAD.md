import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";




## FitMultivariateAnomaly

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.cognitive import *

anomalyKey = os.environ.get("ANOMALY_API_KEY", getSecret("anomaly-api-key"))
startTime = "2021-01-01T00:00:00Z"
endTime = "2021-01-03T01:59:00Z"
timestampColumn = "timestamp"
inputColumns = ["feature0", "feature1", "feature2"]
containerName = "madtest"
intermediateSaveDir = "intermediateData"
connectionString = os.environ.get("MADTEST_CONNECTION_STRING", getSecret("madtest-connection-string"))

fitMultivariateAnomaly = (FitMultivariateAnomaly()
    .setSubscriptionKey(anomalyKey)
    .setLocation("westus2")
    .setOutputCol("result")
    .setStartTime(startTime)
    .setEndTime(endTime)
    .setContainerName(containerName)
    .setIntermediateSaveDir(intermediateSaveDir)
    .setTimestampCol(timestampColumn)
    .setInputCols(inputColumns)
    .setSlidingWindow(200)
    .setConnectionString(connectionString))

# uncomment below for fitting your own dataframe
# model = fitMultivariateAnomaly.fit(df)
# fitMultivariateAnomaly.cleanUpIntermediateData()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._

val startTime: String = "2021-01-01T00:00:00Z"
val endTime: String = "2021-01-02T12:00:00Z"
val timestampColumn: String = "timestamp"
val inputColumns: Array[String] = Array("feature0", "feature1", "feature2")
val containerName: String = "madtest"
val intermediateSaveDir: String = "intermediateData"
val anomalyKey = sys.env.getOrElse("ANOMALY_API_KEY", None)
val connectionString = sys.env.getOrElse("MADTEST_CONNECTION_STRING", None)

val fitMultivariateAnomaly = (new FitMultivariateAnomaly()
    .setSubscriptionKey(anomalyKey)
    .setLocation("westus2")
    .setOutputCol("result")
    .setStartTime(startTime)
    .setEndTime(endTime)
    .setContainerName(containerName)
    .setIntermediateSaveDir(intermediateSaveDir)
    .setTimestampCol(timestampColumn)
    .setInputCols(inputColumns)
    .setSlidingWindow(200)
    .setConnectionString(connectionString))

val df = (spark.read.format("csv")
      .option("header", True)
      .load("wasbs://datasets@mmlspark.blob.core.windows.net/MAD/mad_example.csv"))

val model = fitMultivariateAnomaly.fit(df)

val result = (model
      .setStartTime(startTime)
      .setEndTime(endTime)
      .setOutputCol("result")
      .setTimestampCol(timestampColumn)
      .setInputCols(inputColumns)
      .transform(df))

result.show()

fitMultivariateAnomaly.cleanUpIntermediateData()
model.cleanUpIntermediateData()
```

</TabItem>
</Tabs>

<DocTable className="FitMultivariateAnomaly"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.FitMultivariateAnomaly"
scala="com/microsoft/azure/synapse/ml/cognitive/FitMultivariateAnomaly.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/MultivariateAnomalyDetection.scala" />
