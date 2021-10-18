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

## Recommendation

### RecommendationIndexer, RankingEvaluator, RankingAdapter and RankingTrainValidationSplit

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.recommendation import *
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import *

ratings = (spark.createDataFrame([
      ("11", "Movie 01", 2),
      ("11", "Movie 03", 1),
      ("11", "Movie 04", 5),
      ("11", "Movie 05", 3),
      ("11", "Movie 06", 4),
      ("11", "Movie 07", 1),
      ("11", "Movie 08", 5),
      ("11", "Movie 09", 3),
      ("22", "Movie 01", 4),
      ("22", "Movie 02", 5),
      ("22", "Movie 03", 1),
      ("22", "Movie 05", 3),
      ("22", "Movie 06", 3),
      ("22", "Movie 07", 5),
      ("22", "Movie 08", 1),
      ("22", "Movie 10", 3),
      ("33", "Movie 01", 4),
      ("33", "Movie 03", 1),
      ("33", "Movie 04", 5),
      ("33", "Movie 05", 3),
      ("33", "Movie 06", 4),
      ("33", "Movie 08", 1),
      ("33", "Movie 09", 5),
      ("33", "Movie 10", 3),
      ("44", "Movie 01", 4),
      ("44", "Movie 02", 5),
      ("44", "Movie 03", 1),
      ("44", "Movie 05", 3),
      ("44", "Movie 06", 4),
      ("44", "Movie 07", 5),
      ("44", "Movie 08", 1),
      ("44", "Movie 10", 3)
      ], ["customerIDOrg", "itemIDOrg", "rating"])
    .dropDuplicates()
    .cache())

recommendationIndexer = (RecommendationIndexer()
    .setUserInputCol("customerIDOrg")
    .setUserOutputCol("customerID")
    .setItemInputCol("itemIDOrg")
    .setItemOutputCol("itemID")
    .setRatingCol("rating"))

transformedDf = (recommendationIndexer.fit(ratings)
    .transform(ratings).cache())

als = (ALS()
    .setNumUserBlocks(1)
    .setNumItemBlocks(1)
    .setUserCol("customerID")
    .setItemCol("itemID")
    .setRatingCol("rating")
    .setSeed(0))

evaluator = (RankingEvaluator()
    .setK(3)
    .setNItems(10))

adapter = (RankingAdapter()
    .setK(evaluator.getK())
    .setRecommender(als))

display(adapter.fit(transformedDf).transform(transformedDf))

paramGrid = (ParamGridBuilder()
    .addGrid(als.regParam, [1.0])
    .build())

tvRecommendationSplit = (RankingTrainValidationSplit()
      .setEstimator(als)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setUserCol(recommendationIndexer.getUserOutputCol())
      .setItemCol(recommendationIndexer.getItemOutputCol())
      .setRatingCol("rating"))

display(tvRecommendationSplit.fit(transformedDf).transform(transformedDf))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.recommendation._
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.tuning._
import spark.implicits._

val ratings = (Seq(
      ("11", "Movie 01", 2),
      ("11", "Movie 03", 1),
      ("11", "Movie 04", 5),
      ("11", "Movie 05", 3),
      ("11", "Movie 06", 4),
      ("11", "Movie 07", 1),
      ("11", "Movie 08", 5),
      ("11", "Movie 09", 3),
      ("22", "Movie 01", 4),
      ("22", "Movie 02", 5),
      ("22", "Movie 03", 1),
      ("22", "Movie 05", 3),
      ("22", "Movie 06", 3),
      ("22", "Movie 07", 5),
      ("22", "Movie 08", 1),
      ("22", "Movie 10", 3),
      ("33", "Movie 01", 4),
      ("33", "Movie 03", 1),
      ("33", "Movie 04", 5),
      ("33", "Movie 05", 3),
      ("33", "Movie 06", 4),
      ("33", "Movie 08", 1),
      ("33", "Movie 09", 5),
      ("33", "Movie 10", 3),
      ("44", "Movie 01", 4),
      ("44", "Movie 02", 5),
      ("44", "Movie 03", 1),
      ("44", "Movie 05", 3),
      ("44", "Movie 06", 4),
      ("44", "Movie 07", 5),
      ("44", "Movie 08", 1),
      ("44", "Movie 10", 3))
    .toDF("customerIDOrg", "itemIDOrg", "rating")
    .dropDuplicates()
    .cache())

val recommendationIndexer = (new RecommendationIndexer()
    .setUserInputCol("customerIDOrg")
    .setUserOutputCol("customerID")
    .setItemInputCol("itemIDOrg")
    .setItemOutputCol("itemID")
    .setRatingCol("rating"))

val transformedDf = (recommendationIndexer.fit(ratings)
    .transform(ratings).cache())

val als = (new ALS()
    .setNumUserBlocks(1)
    .setNumItemBlocks(1)
    .setUserCol("customerID")
    .setItemCol("itemID")
    .setRatingCol("rating")
    .setSeed(0))

val evaluator = (new RankingEvaluator()
    .setK(3)
    .setNItems(10))

val adapter = (new RankingAdapter()
    .setK(evaluator.getK)
    .setRecommender(als))

display(adapter.fit(transformedDf).transform(transformedDf))

val paramGrid = (new ParamGridBuilder()
    .addGrid(als.regParam, Array(1.0))
    .build())

val tvRecommendationSplit = (new RankingTrainValidationSplit()
      .setEstimator(als)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setUserCol(recommendationIndexer.getUserOutputCol)
      .setItemCol(recommendationIndexer.getItemOutputCol)
      .setRatingCol("rating"))

display(tvRecommendationSplit.fit(transformedDf).transform(transformedDf))
```

</TabItem>
</Tabs>

<DocTable className="RecommendationIndexer"
py="mmlspark.recommendation.html#module-mmlspark.recommendation.RecommendationIndexer"
scala="com/microsoft/azure/synapse/ml/recommendation/RecommendationIndexer.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/RecommendationIndexer.scala" />
<DocTable className="RankingEvaluator"
py="mmlspark.recommendation.html#module-mmlspark.recommendation.RankingEvaluator"
scala="com/microsoft/azure/synapse/ml/recommendation/RankingEvaluator.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/RankingEvaluator.scala" />
<DocTable className="RankingAdapter"
py="mmlspark.recommendation.html#module-mmlspark.recommendation.RankingAdapter"
scala="com/microsoft/azure/synapse/ml/recommendation/RankingAdapter.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/RankingAdapter.scala" />
<DocTable className="RankingTrainValidationSplit"
py="mmlspark.recommendation.html#module-mmlspark.recommendation.RankingTrainValidationSplit"
scala="com/microsoft/azure/synapse/ml/recommendation/RankingTrainValidationSplit.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/RankingTrainValidationSplit.scala" />


### SAR

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.recommendation import *

ratings = (spark.createDataFrame([
      ("11", "Movie 01", 2),
      ("11", "Movie 03", 1),
      ("11", "Movie 04", 5),
      ("11", "Movie 05", 3),
      ("11", "Movie 06", 4),
      ("11", "Movie 07", 1),
      ("11", "Movie 08", 5),
      ("11", "Movie 09", 3),
      ("22", "Movie 01", 4),
      ("22", "Movie 02", 5),
      ("22", "Movie 03", 1),
      ("22", "Movie 05", 3),
      ("22", "Movie 06", 3),
      ("22", "Movie 07", 5),
      ("22", "Movie 08", 1),
      ("22", "Movie 10", 3),
      ("33", "Movie 01", 4),
      ("33", "Movie 03", 1),
      ("33", "Movie 04", 5),
      ("33", "Movie 05", 3),
      ("33", "Movie 06", 4),
      ("33", "Movie 08", 1),
      ("33", "Movie 09", 5),
      ("33", "Movie 10", 3),
      ("44", "Movie 01", 4),
      ("44", "Movie 02", 5),
      ("44", "Movie 03", 1),
      ("44", "Movie 05", 3),
      ("44", "Movie 06", 4),
      ("44", "Movie 07", 5),
      ("44", "Movie 08", 1),
      ("44", "Movie 10", 3)
      ], ["customerIDOrg", "itemIDOrg", "rating"])
    .dropDuplicates()
    .cache())

recommendationIndexer = (RecommendationIndexer()
    .setUserInputCol("customerIDOrg")
    .setUserOutputCol("customerID")
    .setItemInputCol("itemIDOrg")
    .setItemOutputCol("itemID")
    .setRatingCol("rating"))

algo = (SAR()
      .setUserCol("customerID")
      .setItemCol("itemID")
      .setRatingCol("rating")
      .setTimeCol("timestamp")
      .setSupportThreshold(1)
      .setSimilarityFunction("jacccard")
      .setActivityTimeFormat("EEE MMM dd HH:mm:ss Z yyyy"))

adapter = (RankingAdapter()
      .setK(5)
      .setRecommender(algo))

res1 = recommendationIndexer.fit(ratings).transform(ratings).cache()

display(adapter.fit(res1).transform(res1))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.recommendation._
import spark.implicits._

val ratings = (Seq(
      ("11", "Movie 01", 2),
      ("11", "Movie 03", 1),
      ("11", "Movie 04", 5),
      ("11", "Movie 05", 3),
      ("11", "Movie 06", 4),
      ("11", "Movie 07", 1),
      ("11", "Movie 08", 5),
      ("11", "Movie 09", 3),
      ("22", "Movie 01", 4),
      ("22", "Movie 02", 5),
      ("22", "Movie 03", 1),
      ("22", "Movie 05", 3),
      ("22", "Movie 06", 3),
      ("22", "Movie 07", 5),
      ("22", "Movie 08", 1),
      ("22", "Movie 10", 3),
      ("33", "Movie 01", 4),
      ("33", "Movie 03", 1),
      ("33", "Movie 04", 5),
      ("33", "Movie 05", 3),
      ("33", "Movie 06", 4),
      ("33", "Movie 08", 1),
      ("33", "Movie 09", 5),
      ("33", "Movie 10", 3),
      ("44", "Movie 01", 4),
      ("44", "Movie 02", 5),
      ("44", "Movie 03", 1),
      ("44", "Movie 05", 3),
      ("44", "Movie 06", 4),
      ("44", "Movie 07", 5),
      ("44", "Movie 08", 1),
      ("44", "Movie 10", 3))
    .toDF("customerIDOrg", "itemIDOrg", "rating")
    .dropDuplicates()
    .cache())

val recommendationIndexer = (new RecommendationIndexer()
    .setUserInputCol("customerIDOrg")
    .setUserOutputCol("customerID")
    .setItemInputCol("itemIDOrg")
    .setItemOutputCol("itemID")
    .setRatingCol("rating"))

val algo = (new SAR()
      .setUserCol("customerID")
      .setItemCol("itemID")
      .setRatingCol("rating")
      .setTimeCol("timestamp")
      .setSupportThreshold(1)
      .setSimilarityFunction("jacccard")
      .setActivityTimeFormat("EEE MMM dd HH:mm:ss Z yyyy"))

val adapter = (new RankingAdapter()
      .setK(5)
      .setRecommender(algo))

val res1 = recommendationIndexer.fit(ratings).transform(ratings).cache()

display(adapter.fit(res1).transform(res1))
```

</TabItem>
</Tabs>

<DocTable className="SAR"
py="mmlspark.recommendation.html#module-mmlspark.recommendation.SAR"
scala="com/microsoft/azure/synapse/ml/recommendation/SAR.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala" />

