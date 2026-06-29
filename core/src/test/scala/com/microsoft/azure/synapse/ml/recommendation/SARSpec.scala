package com.microsoft.azure.synapse.ml.recommendation

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject, TransformerFuzzing}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}

import scala.language.existentials

class SARSpec extends RankingTestBase with EstimatorFuzzing[SAR] {
  override def testObjects(): List[TestObject[SAR]] = {
    List(
      new TestObject(new SAR()
        .setUserCol(recommendationIndexer.getUserOutputCol)
        .setItemCol(recommendationIndexer.getItemOutputCol)
        .setRatingCol(ratingCol), transformedDf)
    )
  }

  override def reader: SAR.type = SAR

  override val epsilon = .3

  override def modelReader: SARModel.type = SARModel

  test("SAR") {

    val algo = sar
      .setSupportThreshold(1)
      .setSimilarityFunction("jacccard")
      .setActivityTimeFormat("EEE MMM dd HH:mm:ss Z yyyy")

    val adapter: RankingAdapter = new RankingAdapter()
      .setK(5)
      .setRecommender(algo)

    val recopipeline = new Pipeline()
      .setStages(Array(recommendationIndexer, adapter))
      .fit(ratings)

    val output = recopipeline.transform(ratings)

    val evaluator: RankingEvaluator = new RankingEvaluator()
      .setK(5)
      .setNItems(10)

    assert(evaluator.setMetricName("ndcgAt").evaluate(output) === 0.602819875812812)
    assert(evaluator.setMetricName("fcp").evaluate(output) === 0.05 ||
      evaluator.setMetricName("fcp").evaluate(output) === 0.1)
    assert(evaluator.setMetricName("mrr").evaluate(output) === 1.0)

    val users: DataFrame = spark
      .createDataFrame(Seq(("0","0"),("1","1")))
      .toDF(userColIndex, itemColIndex)

    val recs = recopipeline.stages(1).asInstanceOf[RankingAdapterModel].getRecommenderModel
      .asInstanceOf[SARModel].recommendForUserSubset(users, 10)
    assert(recs.count == 2)
  }

  lazy val testFile: String = getClass.getResource("/demoUsage.csv.gz").getPath
  lazy val simCount1: String = getClass.getResource("/sim_count1.csv.gz").getPath
  lazy val simLift1: String = getClass.getResource("/sim_lift1.csv.gz").getPath
  lazy val simJac1: String = getClass.getResource("/sim_jac1.csv.gz").getPath
  lazy val simCount3: String = getClass.getResource("/sim_count3.csv.gz").getPath
  lazy val simLift3: String = getClass.getResource("/sim_lift3.csv.gz").getPath
  lazy val simJac3: String = getClass.getResource("/sim_jac3.csv.gz").getPath
  lazy val userAff: String = getClass.getResource("/user_aff.csv.gz").getPath
  lazy val userpredCount3: String = getClass.getResource("/userpred_count3_userid_only.csv.gz").getPath
  lazy val userpredLift3: String = getClass.getResource("/userpred_lift3_userid_only.csv.gz").getPath
  lazy val userpredJac3: String = getClass.getResource("/userpred_jac3_userid_only.csv.gz").getPath

  private lazy val tlcSampleData: DataFrame = spark.read
    .option("header", "true") //reading the headers
    .option("inferSchema", "true")
    .csv(testFile).na.drop.cache

  test("tlc test sim count1")(
    SarTLCSpec.testAffinityMatrices(tlcSampleData, 1, "cooc", simCount1, userAff))

  test("tlc test sim lift1")(
    SarTLCSpec.testAffinityMatrices(tlcSampleData, 1, "lift", simLift1, userAff))

  test("tlc test sim jac1")(
    SarTLCSpec.testAffinityMatrices(tlcSampleData, 1, "jaccard", simJac1, userAff))

  test("tlc test sim count3")(
    SarTLCSpec.testAffinityMatrices(tlcSampleData, 3, "cooc", simCount3, userAff))

  test("tlc test sim lift3")(
    SarTLCSpec.testAffinityMatrices(tlcSampleData, 3, "lift", simLift3, userAff))

  test("tlc test sim jac3")(
    SarTLCSpec.testAffinityMatrices(tlcSampleData, 3, "jaccard", simJac3, userAff))

  test("tlc test userpred count3 userid only")(
    SarTLCSpec.testProductRecommendations(tlcSampleData, 3, "cooc", simCount3, userAff, userpredCount3))

  test("tlc test userpred lift3 userid only")(
    SarTLCSpec.testProductRecommendations(tlcSampleData, 3, "lift", simLift3, userAff, userpredLift3))

  test("tlc test userpred jac3 userid only")(
    SarTLCSpec.testProductRecommendations(tlcSampleData, 3, "jaccard", simJac3, userAff, userpredJac3))

  test("SAR with String User Column") {
    val stringUserCol = "stringUserId"
    val stringItemCol = "stringItemId"

    val stringRatings: DataFrame = spark
      .createDataFrame(Seq(
        ("user1", "item1", 2),
        ("user1", "item3", 1),
        ("user1", "item4", 5),
        ("user2", "item1", 4),
        ("user2", "item2", 5),
        ("user2", "item3", 1),
        ("user3", "item1", 4),
        ("user3", "item3", 1),
        ("user3", "item4", 5)
      ))
      .toDF(stringUserCol, stringItemCol, ratingCol)
      .dropDuplicates()
      .cache()

    val stringRecommendationIndexer: RecommendationIndexer = new RecommendationIndexer()
      .setUserInputCol(stringUserCol)
      .setUserOutputCol(userColIndex)
      .setItemInputCol(stringItemCol)
      .setItemOutputCol(itemColIndex)
      .setRatingCol(ratingCol)

    val transformedStringDf: DataFrame = stringRecommendationIndexer.fit(stringRatings)
      .transform(stringRatings).cache()

    val algo = new SAR()
      .setUserCol(stringRecommendationIndexer.getUserOutputCol)
      .setItemCol(stringRecommendationIndexer.getItemOutputCol)
      .setRatingCol(ratingCol)
      .setSupportThreshold(1)
      .setSimilarityFunction("jaccard")
      .setActivityTimeFormat("EEE MMM dd HH:mm:ss Z yyyy")

    val adapter: RankingAdapter = new RankingAdapter()
      .setK(5)
      .setRecommender(algo)

    val recopipeline = new Pipeline()
      .setStages(Array(stringRecommendationIndexer, adapter))
      .fit(stringRatings)

    val output = recopipeline.transform(stringRatings)

    val evaluator: RankingEvaluator = new RankingEvaluator()
      .setK(5)
      .setNItems(10)

    assert(evaluator.setMetricName("ndcgAt").evaluate(output) > 0.0)
    assert(evaluator.setMetricName("fcp").evaluate(output) > 0.0)
    assert(evaluator.setMetricName("mrr").evaluate(output) > 0.0)

    val users: DataFrame = spark
      .createDataFrame(Seq(("user1", "item1"), ("user2", "item2")))
      .toDF(stringUserCol, stringItemCol)

    val recs = recopipeline.stages(1).asInstanceOf[RankingAdapterModel].getRecommenderModel
      .asInstanceOf[SARModel].recommendForUserSubset(users, 10)
    assert(recs.count == 2)
  }

  test("SAR with Different DataTypes in User Column") {
    val mixedUserCol = "mixedUserId"
    val mixedItemCol = "mixedItemId"

    val mixedRatings: DataFrame = spark
      .createDataFrame(Seq(
        (1, "item1", 2),
        (1, "item3", 1),
        (1, "item4", 5),
        (2, "item1", 4),
        (2, "item2", 5),
        (2, "item3", 1),
        (3, "item1", 4),
        (3, "item3", 1),
        (3, "item4", 5),
        ("user4", "item1", 3),
        ("user4", "item2", 2),
        ("user4", "item3", 4)
      ))
      .toDF(mixedUserCol, mixedItemCol, ratingCol)
      .dropDuplicates()
      .cache()

    val algo = new SAR()
      .setUserCol(mixedUserCol)
      .setItemCol(mixedItemCol)
      .setRatingCol(ratingCol)
      .setSupportThreshold(1)
      .setSimilarityFunction("jaccard")
      .setActivityTimeFormat("EEE MMM dd HH:mm:ss Z yyyy")

    val adapter: RankingAdapter = new RankingAdapter()
      .setK(5)
      .setRecommender(algo)

    val recopipeline = new Pipeline()
      .setStages(Array(adapter))
      .fit(mixedRatings)

    val output = recopipeline.transform(mixedRatings)

    val evaluator: RankingEvaluator = new RankingEvaluator()
      .setK(5)
      .setNItems(10)

    assert(evaluator.setMetricName("ndcgAt").evaluate(output) > 0.0)
    assert(evaluator.setMetricName("fcp").evaluate(output) > 0.0)
    assert(evaluator.setMetricName("mrr").evaluate(output) > 0.0)

    val users: DataFrame = spark
      .createDataFrame(Seq((1, "item1"), (2, "item2"), ("user4", "item3")))
      .toDF(mixedUserCol, mixedItemCol)

    val recs = recopipeline.stages(0).asInstanceOf[RankingAdapterModel].getRecommenderModel
      .asInstanceOf[SARModel].recommendForUserSubset(users, 10)
    assert(recs.count == 3)
  }
}

class SARModelSpec extends RankingTestBase with TransformerFuzzing[SARModel] {
  override def testObjects(): Seq[TestObject[SARModel]] = {
    List(
      new TestObject(new SAR()
        .setUserCol(recommendationIndexer.getUserOutputCol)
        .setItemCol(recommendationIndexer.getItemOutputCol)
        .setRatingCol(ratingCol)
        .fit(transformedDf), transformedDf)
    )
  }

  override def reader: MLReadable[_] = SARModel

}

object SarTLCSpec extends RankingTestBase {
  //scalastyle:off field.name
  override lazy val userCol = "userId"
  override lazy val itemCol = "productId"
  override lazy val ratingCol = "rating"
  override lazy val userColIndex = "customerID"
  override lazy val itemColIndex = "itemID"
  //scalastyle:on field.name

  def testAffinityMatrices(tlcSampleData: DataFrame,
                           threshold: Int,
                           similarityFunction: String,
                           simFile: String,
                           user_aff: String):
  (SARModel, RecommendationIndexerModel) = {

    val ratings = tlcSampleData

    val recommendationIndexerModel = recommendationIndexer.fit(ratings)
    val transformedDf = recommendationIndexerModel.transform(ratings)

    val itemMap = recommendationIndexerModel.getItemIndex

    val model = sar
      .setSupportThreshold(threshold)
      .setSimilarityFunction(similarityFunction)
      .setStartTime("2015/06/09T19:39:37")
      .setStartTimeFormat("yyyy/MM/dd'T'h:mm:ss")
      .fit(transformedDf)

    val simMap = model.getItemDataFrame.collect().map(row => {
      val itemI = itemMap.getOrElse(row.getDouble(0).toInt, "-1")
      val similarityVectorMap = row.getList(1).toArray.zipWithIndex.map(t => (itemMap.getOrElse(t._2, "-1"), t._1))
        .toMap
      itemI -> similarityVectorMap
    }).toMap

    val itemAff = spark.read.option("header", "true").csv(simFile)
    itemAff.collect().foreach(row => {
      val itemI = row.getString(0)
      itemAff.drop("_c0").schema.fieldNames.foreach(itemJ => {
        val groundTrueScore = row.getAs[String](itemJ).toFloat
        val sparkSarScore = simMap.getOrElse(itemI, Map()).getOrElse(itemJ, "-1.0")
        assert(groundTrueScore == sparkSarScore)
      })
    })
    (model, recommendationIndexerModel)
  }

  // scalastyle:off method.length
  def testProductRecommendations(tlcSampleData: DataFrame,
                                 threshold: Int,
                                 similarityFunction: String,
                                 simFile: String,
                                 user_aff: String,
                                 userPredFile: String): Unit = {

    val (model, recommendationIndexerModel) = testAffinityMatrices(tlcSampleData, threshold, similarityFunction,
      simFile,
      user_aff)

    val recoverUser = recommendationIndexerModel.recoverUser()
    val recoverItem = recommendationIndexerModel.recoverItem()

    val usersProducts = tlcSampleData
      .filter(col("userId") === "0003000098E85347")
      .select("productId")
      .distinct()
      .collect()
      .map(_.getString(0))

    val usersProductsBC = spark.sparkContext.broadcast(usersProducts)

    val itemMapBC = spark.sparkContext.broadcast(recommendationIndexerModel.getItemIndex)

    val filterScore = udf((items: Seq[Int], ratings: Seq[Float]) => {
      items.zipWithIndex
        .filter(p => {
          val itemId = itemMapBC.value.getOrElse[String](p._1, "-1")
          val bol = usersProductsBC.value.contains(itemId)
          !bol
        }).map(p => (p._1, ratings.toList(p._2)))
    })

    val row = model.recommendForAllUsers(10 + usersProducts.length)
      .select(col("customerID"), filterScore(col("recommendations.itemID"), col("recommendations.rating")) as
        "recommendations")
      .select(col("customerID"), col("recommendations._1") as "itemID", col("recommendations._2") as "rating")
      .select(
        recoverUser(col("customerID")) as "customerID",
        recoverItem(col("itemID")(0)) as "rec1",
        recoverItem(col("itemID")(1)) as "rec2",
        recoverItem(col("itemID")(2)) as "rec3",
        recoverItem(col("itemID")(3)) as "rec4",
        recoverItem(col("itemID")(4)) as "rec5",
        recoverItem(col("itemID")(5)) as "rec6",
        recoverItem(col("itemID")(6)) as "rec7",
        recoverItem(col("itemID")(7)) as "rec8",
        recoverItem(col("itemID")(8)) as "rec9",
        recoverItem(col("itemID")(9)) as "rec10",
        col("rating")(0) as "score1",
        col("rating")(1) as "score2",
        col("rating")(2) as "score3",
        col("rating")(3) as "score4",
        col("rating")(4) as "score5",
        col("rating")(5) as "score6",
        col("rating")(6) as "score7",
        col("rating")(7) as "score8",
        col("rating")(8) as "score9",
        col("rating")(9) as "score10")
      .filter(col("customerID") === "0003000098E85347")
      .take(1)

    val answer = spark.read.option("header", "true").csv(userPredFile).collect()

    assert(row(0).getString(0) == "0003000098E85347", "Assert Customer ID's Match")
    (0 to 10).foreach(i => assert(row(0).getString(i) == answer(0).getString(i)))
    (11 to 20).foreach(i => assert("%.3f".format(row(0).getFloat(i)) == "%.3f".format(answer(0).getString(i).toFloat)))
    ()
  }
  // scalastyle:on method.length
}
