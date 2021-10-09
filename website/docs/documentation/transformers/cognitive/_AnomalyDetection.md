import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";

<!-- 
```python
import pyspark
import os
import json
import mmlspark
from IPython.display import display
from pyspark.sql.functions import col, collect_list, lit, sort_array, struct

spark = (pyspark.sql.SparkSession.builder.appName("MyApp")
        .config("spark.jars.packages", "com.microsoft.ml.spark:mmlspark:1.0.0-rc3-179-327be83c-SNAPSHOT")
        .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven")
        .getOrCreate())

def getSecret(secretName):
        get_secret_cmd = 'az keyvault secret show --vault-name mmlspark-build-keys --name {}'.format(secretName)
        value = json.loads(os.popen(get_secret_cmd).read())["value"]
        return value
``` 
-->

## DetectLastAnomaly

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from mmlspark.cognitive import *

anomalyKey = os.environ.get("ANOMALY_API_KEY", getSecret("anomaly-api-key"))
df = (spark.createDataFrame([
    ("1972-01-01T00:00:00Z", 826.0),
    ("1972-02-01T00:00:00Z", 799.0),
    ("1972-03-01T00:00:00Z", 890.0),
    ("1972-04-01T00:00:00Z", 900.0),
    ("1972-05-01T00:00:00Z", 766.0),
    ("1972-06-01T00:00:00Z", 805.0),
    ("1972-07-01T00:00:00Z", 821.0),
    ("1972-08-01T00:00:00Z", 20000.0),
    ("1972-09-01T00:00:00Z", 883.0),
    ("1972-10-01T00:00:00Z", 898.0),
    ("1972-11-01T00:00:00Z", 957.0),
    ("1972-12-01T00:00:00Z", 924.0),
    ("1973-01-01T00:00:00Z", 881.0),
    ("1973-02-01T00:00:00Z", 837.0),
    ("1973-03-01T00:00:00Z", 90000.0)
], ["timestamp", "value"])
      .withColumn("group", lit(1))
      .withColumn("inputs", struct(col("timestamp"), col("value")))
      .groupBy(col("group"))
      .agg(sort_array(collect_list(col("inputs"))).alias("inputs")))

dla = (DetectLastAnomaly()
      .setSubscriptionKey(anomalyKey)
      .setLocation("westus2")
      .setOutputCol("anomalies")
      .setSeriesCol("inputs")
      .setGranularity("monthly")
      .setErrorCol("errors"))

display(dla.transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
import spark.implicits._
import org.apache.spark.sql.functions.{col, collect_list, lit, sort_array, struct}

val anomalyKey = sys.env.getOrElse("ANOMALY_API_KEY", None)
val df = Seq(
    ("1972-01-01T00:00:00Z", 826.0),
    ("1972-02-01T00:00:00Z", 799.0),
    ("1972-03-01T00:00:00Z", 890.0),
    ("1972-04-01T00:00:00Z", 900.0),
    ("1972-05-01T00:00:00Z", 766.0),
    ("1972-06-01T00:00:00Z", 805.0),
    ("1972-07-01T00:00:00Z", 821.0),
    ("1972-08-01T00:00:00Z", 20000.0),
    ("1972-09-01T00:00:00Z", 883.0),
    ("1972-10-01T00:00:00Z", 898.0),
    ("1972-11-01T00:00:00Z", 957.0),
    ("1972-12-01T00:00:00Z", 924.0),
    ("1973-01-01T00:00:00Z", 881.0),
    ("1973-02-01T00:00:00Z", 837.0),
    ("1973-03-01T00:00:00Z", 90000.0)
  ).toDF("timestamp","value")
    .withColumn("group", lit(1))
    .withColumn("inputs", struct(col("timestamp"), col("value")))
    .groupBy(col("group"))
    .agg(sort_array(collect_list(col("inputs"))).alias("inputs"))

val dla = new DetectLastAnomaly()
            .setSubscriptionKey(anomalyKey)
            .setLocation("westus2")
            .setOutputCol("anomalies")
            .setSeriesCol("inputs")
            .setGranularity("monthly")
            .setErrorCol("errors")

display(dla.transform(df))
```

</TabItem>
</Tabs>

<DocTable className="DetectLastAnomaly"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.DetectLastAnomaly"
scala="com/microsoft/ml/spark/cognitive/DetectLastAnomaly.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/AnomalyDetection.scala" />

## DetectAnomalies

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from mmlspark.cognitive import *

anomalyKey = os.environ.get("ANOMALY_API_KEY", getSecret("anomaly-api-key"))
df = (spark.createDataFrame([
    ("1972-01-01T00:00:00Z", 826.0),
    ("1972-02-01T00:00:00Z", 799.0),
    ("1972-03-01T00:00:00Z", 890.0),
    ("1972-04-01T00:00:00Z", 900.0),
    ("1972-05-01T00:00:00Z", 766.0),
    ("1972-06-01T00:00:00Z", 805.0),
    ("1972-07-01T00:00:00Z", 821.0),
    ("1972-08-01T00:00:00Z", 20000.0),
    ("1972-09-01T00:00:00Z", 883.0),
    ("1972-10-01T00:00:00Z", 898.0),
    ("1972-11-01T00:00:00Z", 957.0),
    ("1972-12-01T00:00:00Z", 924.0),
    ("1973-01-01T00:00:00Z", 881.0),
    ("1973-02-01T00:00:00Z", 837.0),
    ("1973-03-01T00:00:00Z", 90000.0)
], ["timestamp", "value"])
      .withColumn("group", lit(1))
      .withColumn("inputs", struct(col("timestamp"), col("value")))
      .groupBy(col("group"))
      .agg(sort_array(collect_list(col("inputs"))).alias("inputs")))

da = (DetectAnomalies()
      .setSubscriptionKey(anomalyKey)
      .setLocation("westus2")
      .setOutputCol("anomalies")
      .setSeriesCol("inputs")
      .setGranularity("monthly"))

display(da.transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
import spark.implicits._

val anomalyKey = sys.env.getOrElse("ANOMALY_API_KEY", None)
val df = Seq(
    ("1972-01-01T00:00:00Z", 826.0),
    ("1972-02-01T00:00:00Z", 799.0),
    ("1972-03-01T00:00:00Z", 890.0),
    ("1972-04-01T00:00:00Z", 900.0),
    ("1972-05-01T00:00:00Z", 766.0),
    ("1972-06-01T00:00:00Z", 805.0),
    ("1972-07-01T00:00:00Z", 821.0),
    ("1972-08-01T00:00:00Z", 20000.0),
    ("1972-09-01T00:00:00Z", 883.0),
    ("1972-10-01T00:00:00Z", 898.0),
    ("1972-11-01T00:00:00Z", 957.0),
    ("1972-12-01T00:00:00Z", 924.0),
    ("1973-01-01T00:00:00Z", 881.0),
    ("1973-02-01T00:00:00Z", 837.0),
    ("1973-03-01T00:00:00Z", 90000.0)
  ).toDF("timestamp","value")
    .withColumn("group", lit(1))
    .withColumn("inputs", struct(col("timestamp"), col("value")))
    .groupBy(col("group"))
    .agg(sort_array(collect_list(col("inputs"))).alias("inputs"))

val da = new DetectAnomalies()
            .setSubscriptionKey(anomalyKey)
            .setLocation("westus2")
            .setOutputCol("anomalies")
            .setSeriesCol("inputs")
            .setGranularity("monthly")

display(da.transform(df))
```

</TabItem>
</Tabs>

<DocTable className="DetectAnomalies" 
py="mmlspark.cognitive.html#module-mmlspark.cognitive.DetectAnomalies"
scala="com/microsoft/ml/spark/cognitive/DetectAnomalies.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/AnomalyDetection.scala" />

## SimpleDetectAnomalies

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from mmlspark.cognitive import *

anomalyKey = os.environ.get("ANOMALY_API_KEY", getSecret("anomaly-api-key"))
df = (spark.createDataFrame([
    ("1972-01-01T00:00:00Z", 826.0, 1.0),
    ("1972-02-01T00:00:00Z", 799.0, 1.0),
    ("1972-03-01T00:00:00Z", 890.0, 1.0),
    ("1972-04-01T00:00:00Z", 900.0, 1.0),
    ("1972-05-01T00:00:00Z", 766.0, 1.0),
    ("1972-06-01T00:00:00Z", 805.0, 1.0),
    ("1972-07-01T00:00:00Z", 821.0, 1.0),
    ("1972-08-01T00:00:00Z", 20000.0, 1.0),
    ("1972-09-01T00:00:00Z", 883.0, 1.0),
    ("1972-10-01T00:00:00Z", 898.0, 1.0),
    ("1972-11-01T00:00:00Z", 957.0, 1.0),
    ("1972-12-01T00:00:00Z", 924.0, 1.0),
    ("1973-01-01T00:00:00Z", 881.0, 1.0),
    ("1973-02-01T00:00:00Z", 837.0, 1.0),
    ("1973-03-01T00:00:00Z", 90000.0, 1.0),
    ("1972-01-01T00:00:00Z", 826.0, 2.0),
    ("1972-02-01T00:00:00Z", 799.0, 2.0),
    ("1972-03-01T00:00:00Z", 890.0, 2.0),
    ("1972-04-01T00:00:00Z", 900.0, 2.0),
    ("1972-05-01T00:00:00Z", 766.0, 2.0),
    ("1972-06-01T00:00:00Z", 805.0, 2.0),
    ("1972-07-01T00:00:00Z", 821.0, 2.0),
    ("1972-08-01T00:00:00Z", 20000.0, 2.0),
    ("1972-09-01T00:00:00Z", 883.0, 2.0),
    ("1972-10-01T00:00:00Z", 898.0, 2.0),
    ("1972-11-01T00:00:00Z", 957.0, 2.0),
    ("1972-12-01T00:00:00Z", 924.0, 2.0),
    ("1973-01-01T00:00:00Z", 881.0, 2.0),
    ("1973-02-01T00:00:00Z", 837.0, 2.0),
    ("1973-03-01T00:00:00Z", 90000.0, 2.0)
], ["timestamp", "value", "group"])

sda = (SimpleDetectAnomalies()
      .setSubscriptionKey(anomalyKey)
      .setLocation("westus2")
      .setOutputCol("anomalies")
      .setSeriesCol("inputs")
      .setGranularity("monthly"))

display(sda.transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
import spark.implicits._

val anomalyKey = sys.env.getOrElse("ANOMALY_API_KEY", None)
val baseSeq = Seq(
    ("1972-01-01T00:00:00Z", 826.0),
    ("1972-02-01T00:00:00Z", 799.0),
    ("1972-03-01T00:00:00Z", 890.0),
    ("1972-04-01T00:00:00Z", 900.0),
    ("1972-05-01T00:00:00Z", 766.0),
    ("1972-06-01T00:00:00Z", 805.0),
    ("1972-07-01T00:00:00Z", 821.0),
    ("1972-08-01T00:00:00Z", 20000.0),
    ("1972-09-01T00:00:00Z", 883.0),
    ("1972-10-01T00:00:00Z", 898.0),
    ("1972-11-01T00:00:00Z", 957.0),
    ("1972-12-01T00:00:00Z", 924.0),
    ("1973-01-01T00:00:00Z", 881.0),
    ("1973-02-01T00:00:00Z", 837.0),
    ("1973-03-01T00:00:00Z", 9000.0)
  )
val df = baseSeq.map(p => (p._1,p._2,1.0))
    .++(baseSeq.map(p => (p._1,p._2,2.0)))
    .toDF("timestamp","value","group")

val sda = new SimpleDetectAnomalies()
            .setSubscriptionKey(anomalyKey)
            .setLocation("westus2")
            .setOutputCol("anomalies")
            .setGroupbyCol("group")
            .setGranularity("monthly")

display(sda.transform(df))
```

</TabItem>
</Tabs>

<DocTable className="SimpleDetectAnomalies" 
py="mmlspark.cognitive.html#module-mmlspark.cognitive.SimpleDetectAnomalies"
scala="com/microsoft/ml/spark/cognitive/SimpleDetectAnomalies.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/AnomalyDetection.scala" />
