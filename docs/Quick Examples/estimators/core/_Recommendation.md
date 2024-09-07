import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";




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

ratings_with_strings = (spark.createDataFrame([
      ("user0", "item1", 4, 4),
      ("user0", "item3", 1, 1),
      ("user0", "item4", 5, 5),
      ("user0", "item5", 3, 3),
      ("user0", "item7", 3, 3),
      ("user0", "item9", 3, 3),
      ("user0", "item10", 3, 3),
      ("user1", "item1", 4, 4),
      ("user1", "item2", 5, 5),
      ("user1", "item3", 1, 1),
      ("user1", "item6", 4, 4),
      ("user1", "item7", 5, 5),
      ("user1", "item8", 1, 1),
      ("user1", "item10", 3, 3),
      ("user2", "item1", 4, 4),
      ("user2", "item2", 1, 1),
      ("user2", "item3", 1, 1),
      ("user2", "item4", 5, 5),
      ("user2", "item5", 3, 3),
      ("user2", "item6", 4, 4),
      ("user2", "item8", 1, 1),
      ("user2", "item9", 5, 5),
      ("user2", "item10", 3, 3),
      ("user3", "item2", 5, 5),
      ("user3", "item3", 1, 1),
      ("user3", "item4", 5, 5),
      ("user3", "item5", 3, 3),
      ("user3", "item6", 4, 4),
      ("user3", "item7", 5, 5),
      ("user3", "item8", 1, 1),
      ("user3", "item9", 5, 5),
      ("user3", "item10", 3, 3)
      ], ["originalCustomerID", "newCategoryID", "rating", "notTime"])
    .coalesce(1)
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

adapter.fit(transformedDf).transform(transformedDf).show()

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

tvRecommendationSplit.fit(transformedDf).transform(transformedDf).show()
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

adapter.fit(transformedDf).transform(transformedDf).show()

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

tvRecommendationSplit.fit(transformedDf).transform(transformedDf).show()
```

</TabItem>
</Tabs>

<DocTable className="RecommendationIndexer"
py="synapse.ml.recommendation.html#module-synapse.ml.recommendation.RecommendationIndexer"
scala="com/microsoft/azure/synapse/ml/recommendation/RecommendationIndexer.html"
csharp="classSynapse_1_1ML_1_1Recommendation_1_1RecommendationIndexer.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/RecommendationIndexer.scala" />
<DocTable className="RankingEvaluator"
py="synapse.ml.recommendation.html#module-synapse.ml.recommendation.RankingEvaluator"
scala="com/microsoft/azure/synapse/ml/recommendation/RankingEvaluator.html"
csharp="classSynapse_1_1ML_1_1Recommendation_1_1RankingEvaluator.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/RankingEvaluator.scala" />
<DocTable className="RankingAdapter"
py="synapse.ml.recommendation.html#module-synapse.ml.recommendation.RankingAdapter"
scala="com/microsoft/azure/synapse/ml/recommendation/RankingAdapter.html"
csharp="classSynapse_1_1ML_1_1Recommendation_1_1RankingAdapter.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/RankingAdapter.scala" />
<DocTable className="RankingTrainValidationSplit"
py="synapse.ml.recommendation.html#module-synapse.ml.recommendation.RankingTrainValidationSplit"
scala="com/microsoft/azure/synapse/ml/recommendation/RankingTrainValidationSplit.html"
csharp="classSynapse_1_1ML_1_1Recommendation_1_1RankingTrainValidationSplit.html"
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

ratings_with_strings = (spark.createDataFrame([
      ("user0", "item1", 4, 4),
      ("user0", "item3", 1, 1),
      ("user0", "item4", 5, 5),
      ("user0", "item5", 3, 3),
      ("user0", "item7", 3, 3),
      ("user0", "item9", 3, 3),
      ("user0", "item10", 3, 3),
      ("user1", "item1", 4, 4),
      ("user1", "item2", 5, 5),
      ("user1", "item3", 1, 1),
      ("user1", "item6", 4, 4),
      ("user1", "item7", 5, 5),
      ("user1", "item8", 1, 1),
      ("user1", "item10", 3, 3),
      ("user2", "item1", 4, 4),
      ("user2", "item2", 1, 1),
      ("user2", "item3", 1, 1),
      ("user2", "item4", 5, 5),
      ("user2", "item5", 3, 3),
      ("user2", "item6", 4, 4),
      ("user2", "item8", 1, 1),
      ("user2", "item9", 5, 5),
      ("user2", "item10", 3, 3),
      ("user3", "item2", 5, 5),
      ("user3", "item3", 1, 1),
      ("user3", "item4", 5, 5),
      ("user3", "item5", 3, 3),
      ("user3", "item6", 4, 4),
      ("user3", "item7", 5, 5),
      ("user3", "item8", 1, 1),
      ("user3", "item9", 5, 5),
      ("user3", "item10", 3, 3)
      ], ["originalCustomerID", "newCategoryID", "rating", "notTime"])
    .coalesce(1)
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

adapter.fit(res1).transform(res1).show()

res2 = recommendationIndexer.fit(ratings_with_strings).transform(ratings_with_strings).cache()

adapter.fit(res2).transform(res2).show()
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

val ratings_with_strings = (Seq(
      ("user0", "item1", 4, 4),
      ("user0", "item3", 1, 1),
      ("user0", "item4", 5, 5),
      ("user0", "item5", 3, 3),
      ("user0", "item7", 3, 3),
      ("user0", "item9", 3, 3),
      ("user0", "item10", 3, 3),
      ("user1", "item1", 4, 4),
      ("user1", "item2", 5, 5),
      ("user1", "item3", 1, 1),
      ("user1", "item6", 4, 4),
      ("user1", "item7", 5, 5),
      ("user1", "item8", 1, 1),
      ("user1", "item10", 3, 3),
      ("user2", "item1", 4, 4),
      ("user2", "item2", 1, 1),
      ("user2", "item3", 1, 1),
      ("user2", "item4", 5, 5),
      ("user2", "item5", 3, 3),
      ("user2", "item6", 4, 4),
      ("user2", "item8", 1, 1),
      ("user2", "item9", 5, 5),
      ("user2", "item10", 3, 3),
      ("user3", "item2", 5, 5),
      ("user3", "item3", 1, 1),
      ("user3", "item4", 5, 5),
      ("user3", "item5", 3, 3),
      ("user3", "item6", 4, 4),
      ("user3", "item7", 5, 5),
      ("user3", "item8", 1, 1),
      ("user3", "item9", 5, 5),
      ("user3", "item10", 3, 3))
    .toDF("originalCustomerID", "newCategoryID", "rating", "notTime")
    .coalesce(1)
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

adapter.fit(res1).transform(res1).show()

val res2 = recommendationIndexer.fit(ratings_with_strings).transform(ratings_with_strings).cache()

adapter.fit(res2).transform(res2).show()
```

</TabItem>
</Tabs>

<DocTable className="SAR"
py="synapse.ml.recommendation.html#module-synapse.ml.recommendation.SAR"
scala="com/microsoft/azure/synapse/ml/recommendation/SAR.html"
csharp="classSynapse_1_1ML_1_1Recommendation_1_1SAR.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/recommendation/SAR.scala" />
