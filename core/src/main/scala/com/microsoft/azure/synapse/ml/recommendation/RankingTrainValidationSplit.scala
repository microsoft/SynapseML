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
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

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

  def splitDF(dataset: DataFrame): Array[DataFrame] = { //scalastyle:ignore method.length
    val usedColumnNames = scala.collection.mutable.Set(dataset.columns: _*)
    def unusedColumnName(baseName: String): String = {
      val name = Iterator.from(0)
        .map(index => if (index == 0) baseName else s"${baseName}_$index")
        .find(candidate => !usedColumnNames.contains(candidate))
        .get
      usedColumnNames += name
      name
    }

    val entryCol = unusedColumnName("__ranking_split_entry")
    val entriesCol = unusedColumnName("__ranking_split_entries")
    val trainCol = unusedColumnName("__ranking_split_train")
    val testCol = unusedColumnName("__ranking_split_test")
    val expandedCol = unusedColumnName("__ranking_split_expanded")
    val orderField = "__ranking_split_order"
    val itemField = "__ranking_split_item"
    val ratingField = "__ranking_split_rating"
    val hasRating = dataset.columns.contains(getRatingCol)
    val entryFields = Seq(
      rand().as(orderField),
      col(getItemCol).as(itemField)
    ) ++ (if (hasRating) Seq(col(getRatingCol).as(ratingField)) else Seq.empty)

    val groupedEntries = dataset
      .select(col(getUserCol), struct(entryFields: _*).as(entryCol))
      .groupBy(col(getUserCol))
      .agg(sort_array(collect_list(col(entryCol))).as(entriesCol))
    val trainLength = round(size(col(entriesCol)) * lit($(trainRatio))).cast(IntegerType)
    val splitEntries = groupedEntries
      .withColumn(trainCol, slice(col(entriesCol), lit(1), trainLength))
      .withColumn(
        testCol,
        slice(col(entriesCol), trainLength + lit(1), size(col(entriesCol)) - trainLength)
      )

    def expand(partitionCol: String): DataFrame = {
      val expanded = splitEntries
        .select(col(getUserCol), explode(col(partitionCol)).as(expandedCol))
      val outputColumns = Seq(
        col(getUserCol),
        col(expandedCol).getField(itemField).as(getItemCol)
      ) ++ (if (hasRating) {
        Seq(col(expandedCol).getField(ratingField).as(getRatingCol))
      } else {
        Seq.empty
      })
      expanded.select(outputColumns: _*)
    }

    Array(expand(trainCol), expand(testCol))
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
