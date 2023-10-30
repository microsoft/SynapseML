// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.recommendation

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.{ModelParam, TypedDoubleArrayParam}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.recommendation._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Model, _}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{collect_list, rank => r, _}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

class RankingTrainValidationSplit(override val uid: String) extends Estimator[RankingTrainValidationSplitModel]
  with RankingTrainValidationSplitParams with Wrappable with ComplexParamsWritable
  with RecommendationParams with SynapseMLLogging {
  logClass(FeatureNames.Recommendation)

  override lazy val pyInternalWrapper: Boolean = true

  def this() = this(Identifiable.randomUID("RankingTrainValidationSplit"))

  /** @group setParam */
  def setUserCol(value: String): this.type = set(userCol, value)

  /** @group setParam */
  def setItemCol(value: String): this.type = set(itemCol, value)

  /** @group setParam */
  def setRatingCol(value: String): this.type = set(ratingCol, value)

  /** @group setParam */
  def setEstimator(value: Estimator[_ <: Model[_]]): this.type = set(estimator, value)

  /** @group setParam */
  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

  def setEstimatorParamMaps(value: java.util.ArrayList[ParamMap]): this.type =
    set(estimatorParamMaps, value.asScala.toArray)

  /** @group setParam */
  def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

  /** @group setParam */
  def setTrainRatio(value: Double): this.type = set(trainRatio, value)

  /** @group setParam */
  def setMinRatingsU(value: Int): this.type = set(minRatingsU, value)

  /** @group setParam */
  def setMinRatingsI(value: Int): this.type = set(minRatingsI, value)

  override def transformSchema(schema: StructType): StructType = transformSchemaImpl(schema)

  /**
    * The number of threads to use when running parallel algorithms.
    * Default is 1 for serial execution
    *
    * @group expertParam
    */
  val parallelism = new IntParam(this, "parallelism",
    "the number of threads to use when running parallel algorithms", ParamValidators.gtEq(1))

  setDefault(parallelism -> 1)

  /** @group expertGetParam */
  def getParallelism: Int = $(parallelism)

  /** @group expertSetParam */
  def setParallelism(value: Int): this.type = set(parallelism, value)

  private[ml] def getExecutionContext: ExecutionContext = {

    getParallelism match {
      case 1 =>
        SparkHelpers.getThreadUtils.sameThread
      case n =>
        ExecutionContext.fromExecutorService(SparkHelpers.getThreadUtils
          .newDaemonCachedThreadPool(s"${this.getClass.getSimpleName}-thread-pool", n))
    }
  }

  override def fit(dataset: Dataset[_]): RankingTrainValidationSplitModel = {
    logFit({
      val schema = dataset.schema
      transformSchema(schema, logging = true)
      val est = getEstimator
      val eval = getEvaluator.asInstanceOf[RankingEvaluator]
      val epm = getEstimatorParamMaps

      dataset.cache()
      eval.setNItems(dataset.agg(countDistinct(col(getItemCol))).take(1)(0).getLong(0))
      val filteredDataset = filterRatings(dataset.dropDuplicates())

      //Stratified Split of Dataset
      val Array(trainingDataset, validationDataset): Array[DataFrame] = splitDF(filteredDataset)
      trainingDataset.cache()
      validationDataset.cache()

      val executionContext = getExecutionContext

      @tailrec
      def calculateMetrics(model: Transformer, validationDataset: Dataset[_]): Double = model match {
        case pm: PipelineModel =>
          //Assume Rec Algo is last stage of pipeline
          val modelTemp = pm.stages.last
          calculateMetrics(modelTemp, validationDataset)
        case alsm: ALSModel =>
          val recs = alsm.recommendForAllUsers(eval.getK)
          val preparedTest: Dataset[_] = prepareTestData(validationDataset.toDF(), recs, eval.getK)
          eval.evaluate(preparedTest)
      }

      val metricFutures = epm.zipWithIndex.map { case (paramMap, _) =>
        Future[Double] {
          val model = est.fit(trainingDataset, paramMap)
          calculateMetrics(model, validationDataset)
        }(executionContext)
      }

      val metrics = metricFutures.map(SparkHelpers.getThreadUtils.awaitResult(_, Duration.Inf))

      trainingDataset.unpersist()
      validationDataset.unpersist()

      val (_, bestIndex) =
        if (eval.isLargerBetter) metrics.zipWithIndex.maxBy(_._1)
        else metrics.zipWithIndex.minBy(_._1)

      copyValues(new RankingTrainValidationSplitModel(uid)
        .setBestModel(est.fit(dataset, epm(bestIndex)))
        .setValidationMetrics(metrics)
        .setParent(this))
    }, dataset.columns.length)
  }

  override def copy(extra: ParamMap): RankingTrainValidationSplit = defaultCopy(extra)

  private def filterByItemCount(dataset: Dataset[_]): DataFrame = {
    dataset
      .groupBy(getUserCol)
      .agg(col(getUserCol), count(col(getItemCol)))
      .withColumnRenamed(s"count($getItemCol)", "nitems")
      .where(col("nitems") >= getMinRatingsU)
      .drop("nitems")
      .cache()
  }

  private def filterByUserRatingCount(dataset: Dataset[_]): DataFrame = dataset
    .groupBy(getItemCol)
    .agg(col(getItemCol), count(col(getUserCol)).alias("ncustomers"))
    .where(col("ncustomers") >= getMinRatingsI)
    .join(dataset, getItemCol)
    .drop("ncustomers")
    .cache()

  def filterRatings(dataset: Dataset[_]): DataFrame = filterByUserRatingCount(dataset)
    .join(filterByItemCount(dataset), $(userCol))

  def splitDF(dataset: DataFrame): Array[DataFrame] = {  //scalastyle:ignore method.length
    val shuffleFlag = true
    val shuffleBC = dataset.sparkSession.sparkContext.broadcast(shuffleFlag)

    if (dataset.columns.contains(getRatingCol)) {
      val wrapColumn = udf((itemId: Double, rating: Double) => Array(itemId, rating))

      val sliceudf = udf(
        (r: Seq[Array[Double]]) => r.slice(0, math.round(r.length * $(trainRatio)).toInt))

      val shuffle = udf((r: Seq[Array[Double]]) =>
        if (shuffleBC.value) Random.shuffle(r)
        else r
      )
      val dropudf = udf((r: Seq[Array[Double]]) => r.drop(math.round(r.length * $(trainRatio)).toInt))

      val testds = dataset
        .withColumn("itemIDRating", wrapColumn(col(getItemCol), col(getRatingCol)))
        .groupBy(col(getUserCol))
        .agg(collect_list(col("itemIDRating")))
        .withColumn("shuffle", shuffle(col("collect_list(itemIDRating)")))
        .withColumn("train", sliceudf(col("shuffle")))
        .withColumn("test", dropudf(col("shuffle")))
        .drop(col("collect_list(itemIDRating)")).drop(col("shuffle"))
      //.cache()

      val train = testds
        .select(getUserCol, "train")
        .withColumn("itemIdRating", explode(col("train")))
        .drop("train")
        .withColumn(getItemCol, col("itemIdRating").getItem(0))
        .withColumn(getRatingCol, col("itemIdRating").getItem(1))
        .drop("itemIdRating")

      val test = testds
        .select(getUserCol, "test")
        .withColumn("itemIdRating", explode(col("test")))
        .drop("test")
        .withColumn(getItemCol, col("itemIdRating").getItem(0))
        .withColumn(getRatingCol, col("itemIdRating").getItem(1))
        .drop("itemIdRating")

      Array(train, test)
    }
    else {
      val sliceudf = udf(
        (r: Seq[Double]) => r.slice(0, math.round(r.length * $(trainRatio)).toInt))
      val dropudf = udf((r: Seq[Double]) => r.drop(math.round(r.length * $(trainRatio)).toInt))

      val testDS = dataset
        .groupBy(col(getUserCol))
        .agg(collect_list(col(getItemCol)).alias("shuffle"))
        .withColumn("train", sliceudf(col("shuffle")))
        .withColumn("test", dropudf(col("shuffle")))
        .drop(col(s"collect_list($getItemCol")).drop(col("shuffle"))
        .cache()

      val train = testDS
        .select(getUserCol, "train")
        .withColumn(getItemCol, explode(col("train")))
        .drop("train")

      val test = testDS
        .select(getUserCol, "test")
        .withColumn(getItemCol, explode(col("test")))
        .drop("test")

      Array(train, test)
    }
  }

  def prepareTestData(validationDataset: DataFrame, recs: DataFrame, k: Int): Dataset[_] = {
    val est = getEstimator match {
      case p: Pipeline =>
        //Assume Rec is last stage of pipeline
        p.getStages.last match {
          case a: ALS => a
        }
      case a: ALS => a
    }

    val userColumn = est.getUserCol
    val itemColumn = est.getItemCol

    val perUserRecommendedItemsDF: DataFrame = recs
      .select(userColumn, "recommendations." + itemColumn)
      .withColumnRenamed(itemColumn, "prediction")

    val perUserActualItemsDF = if (validationDataset.columns.contains($(ratingCol))) {
      val windowSpec = Window.partitionBy(userColumn).orderBy(col($(ratingCol)).desc)

      validationDataset
        .select(userColumn, itemColumn, $(ratingCol))
        .withColumn("rank", r().over(windowSpec).alias("rank"))
        .where(col("rank") <= k)
        .groupBy(userColumn)
        .agg(col(userColumn), collect_list(col(itemColumn)))
        .withColumnRenamed("collect_list(" + itemColumn + ")", "label")
        .select(userColumn, "label")
    } else {
      val windowSpec = Window.partitionBy(userColumn).orderBy(col($(itemCol)).desc)

      validationDataset
        .select(userColumn, itemColumn)
        .withColumn("rank", r().over(windowSpec).alias("rank"))
        .where(col("rank") <= k)
        .groupBy(userColumn)
        .agg(col(userColumn), collect_list(col(itemColumn)))
        .withColumnRenamed("collect_list(" + itemColumn + ")", "label")
        .select(userColumn, "label")
    }
    val joinedRecActual = perUserRecommendedItemsDF
      .join(perUserActualItemsDF, userColumn)
      .drop(userColumn)

    joinedRecActual
  }
}

object RankingTrainValidationSplit extends ComplexParamsReadable[RankingTrainValidationSplit]

class RankingTrainValidationSplitModel(
                                        override val uid: String)
  extends Model[RankingTrainValidationSplitModel] with Wrappable
    with ComplexParamsWritable with SynapseMLLogging {
  logClass(FeatureNames.Recommendation)

  override protected lazy val pyInternalWrapper = true

  def setValidationMetrics(value: Seq[Double]): this.type = set(validationMetrics, value)

  val validationMetrics = new TypedDoubleArrayParam(this, "validationMetrics", "Best Model")

  /** @group getParam */
  def getValidationMetrics: Seq[_] = $(validationMetrics)

  def setBestModel(value: Model[_]): this.type = set(bestModel, value.asInstanceOf[Model[_ <: Model[_]]])

  val bestModel: ModelParam =
    new ModelParam(
      this,
      "bestModel", "The internal ALS model used splitter")

  /** @group getParam */
  def getBestModel: Model[_ <: Model[_]] = $(bestModel)

  def this() = this(Identifiable.randomUID("RankingTrainValidationSplitModel"))

  override def copy(extra: ParamMap): RankingTrainValidationSplitModel = {
    val copied = new RankingTrainValidationSplitModel(uid)
    copyValues(copied, extra).setParent(parent)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      transformSchema(dataset.schema, logging = true)

      //sort to pass unit test
      getBestModel.transform(dataset).sort("prediction")
    }, dataset.columns.length)
  }

  override def transformSchema(schema: StructType): StructType = {
    getBestModel.transformSchema(schema)
  }

  def recommendForAllUsers(numItems: Int): DataFrame = {
    getBestModel match {
      case als: ALSModel => als.recommendForAllUsers(numItems)
      case br: BaseRecommendationModel => br.recommendForAllUsers(numItems)
    }
  }

  def recommendForAllItems(numUsers: Int): DataFrame = {
    getBestModel match {
      case als: ALSModel => als.recommendForAllItems(numUsers)
      case br: BaseRecommendationModel => br.recommendForAllItems(numUsers)
    }
  }

}

object RankingTrainValidationSplitModel extends ComplexParamsReadable[RankingTrainValidationSplitModel]
