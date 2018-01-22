// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml._

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.{ALS, ALSModel, MsftRecommendationParams, TrainValidRecommendSplitParams}
import org.apache.spark.ml.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.util.Random

@InternalWrapper
class TrainValidRecommendSplit(override val uid: String) extends Estimator[TrainValidRecommendSplitModel]
  with TrainValidRecommendSplitParams with ComplexParamsWritable with MsftRecommendationParams {

  /** @group setParam */
  def setUserCol(value: String): this.type = set(userCol, value)

  /** @group setParam */
  def setItemCol(value: String): this.type = set(itemCol, value)

  /** @group setParam */
  def setRatingCol(value: String): this.type = set(ratingCol, value)

  def this() = this(Identifiable.randomUID("TrainValidRecommendSplit"))

  /** @group setParam */
  def setEstimator(value: Estimator[_ <: Model[_]]): this.type = set(estimator, value)

  /** @group setParam */
  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

  /** @group setParam */
  def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

  /** @group setParam */
  def setTrainRatio(value: Double): this.type = set(trainRatio, value)

  /** @group setParam */
  def setSeed(value: Long): this.type = set(seed, value)

  /** @group setParam */
  def setMinRatingsU(value: Int): this.type = set(minRatingsU, value)

  /** @group setParam */
  def setMinRatingsI(value: Int): this.type = set(minRatingsI, value)

  override def transformSchema(schema: StructType): StructType = transformSchemaImpl(schema)

  override def fit(dataset: Dataset[_]): TrainValidRecommendSplitModel = {
    val schema = dataset.schema
    transformSchema(schema, logging = true)
    val est = $(estimator)
    val eval = $(evaluator).asInstanceOf[MsftRecommendationEvaluator]
    val epm = $(estimatorParamMaps)
    val numModels = epm.length
    val metrics = new Array[Double](epm.length)

    val filteredDataset = filterRatings(dataset.dropDuplicates()).cache()

    val Array(trainingDataset, validationDataset): Array[DataFrame] = splitDF(filteredDataset)
    trainingDataset.cache()
    validationDataset.cache()
    filteredDataset.unpersist()

    val models = est.fit(trainingDataset, epm).asInstanceOf[Seq[Model[_]]]
    trainingDataset.unpersist()

    eval.setNItems(validationDataset.rdd.map(r => r(1)).distinct().count())
    //noinspection ScalaStyle
    def calculateMetrics(model: Transformer): Double = model match {
      case p: PipelineModel => {
        //Assume Rec is last stage of pipeline
        val modelTemp = model.asInstanceOf[PipelineModel].stages.last
        calculateMetrics(modelTemp)
      }
      case m: MsftRecommendationModel => {
        val recs = model.asInstanceOf[MsftRecommendationModel].recommendForAllUsers(eval.getK)
        val preparedTest: Dataset[_] = prepareTestData(model.transform(validationDataset), recs)
        eval.evaluate(preparedTest)
      }
      case a: ALSModel => {
        val recs = model.asInstanceOf[ALSModel].recommendForAllUsers(eval.getK)
        val preparedTest: Dataset[_] = prepareTestData(model.transform(validationDataset), recs)
        eval.evaluate(preparedTest)
      }
    }

    var i = 0
    while (i < numModels) {
      // TODO: duplicate evaluator to take extra params from input
      val metric = calculateMetrics(models(i))
      logDebug(s"Got metric $metric for model trained with ${epm(i)}.")
      metrics(i) += metric
      i += 1
    }

    validationDataset.unpersist()

    val (bestMetric, bestIndex) =
      if (eval.isLargerBetter) metrics.zipWithIndex.maxBy(_._1)
      else metrics.zipWithIndex.minBy(_._1)

    val bestModel = est.fit(dataset, epm(bestIndex)).asInstanceOf[Model[_]]
    copyValues(new TrainValidRecommendSplitModel(uid, bestModel, metrics).setParent(this))
  }

  override def copy(extra: ParamMap): TrainValidRecommendSplit = defaultCopy(extra)

  private def filterByItemCount(dataset: Dataset[_]): DataFrame = dataset
    .groupBy($(userCol))
    .agg(col($(userCol)), count(col($(itemCol))))
    .withColumnRenamed("count(" + $(itemCol) + ")", "nitems")
    .where(col("nitems") >= $(minRatingsU))
    .drop("nitems")

  private def filterByUserRatingCount(dataset: Dataset[_]): DataFrame = dataset
    .groupBy($(itemCol))
    .agg(col($(itemCol)), count(col($(userCol))))
    .withColumnRenamed("count(" + $(userCol) + ")", "ncustomers")
    .where(col("ncustomers") >= $(minRatingsI))
    .join(dataset, $(itemCol))
    .drop("ncustomers")

  private def filterRatings(dataset: Dataset[_]): DataFrame = filterByUserRatingCount(dataset)
    .join(filterByItemCount(dataset), $(userCol))

  def splitDF(dataset: DataFrame): Array[DataFrame] = {
    import dataset.sqlContext.implicits._

    val (tr_idx: RDD[(Any, List[Any])], testIndex: RDD[(Any, List[Any])]) = {
      def permIndicesLambda(r: Row): (Any, List[Any], List[Any]) =
        (r(0), Random.shuffle(List(r(1))), List(r(1)))

      def firstN(r: (Any, List[Any], List[Any])): (Any, List[Any]) =
        (r._1, r._2.slice(0, math.round(r._3.size.toDouble * $(trainRatio)).toInt))

      def lastN(r: (Any, List[Any], List[Any])): (Any, List[Any]) =
        (r._1, r._2.drop(math.round(r._3.size.toDouble * $(trainRatio)).toInt))

      val perm_indices: RDD[(Any, List[Any], List[Any])] = dataset
        .groupBy($(itemCol))
        .agg(col($(itemCol)), count($(userCol)))
        .withColumnRenamed("count(" + $(userCol) + ")", "nusers").rdd
        .map(permIndicesLambda)
        .cache()

      val tr_idx: RDD[(Any, List[Any])] = perm_indices.map(firstN)
      val testIndex: RDD[(Any, List[Any])] = perm_indices.map(lastN)
      perm_indices.unpersist()

      (tr_idx, testIndex)
    }

    val (trainingDataset: DataFrame, validationDataset: DataFrame) = {
      def validationFlaptMapLambda(r: (Any, (Iterable[Row], List[Any]))): Iterable[Row] =
        r._2._1.drop(r._2._2.size)

      def mapLambda(r: Row): (Int, Int, String, String, Int) =
        (r.getDouble(0).toInt, r.getDouble(1).toInt, r.getString(2), r.getString(3), r.getInt(4))

      def flatMapLambda(r: (Any, (Iterable[Row], List[Any]))): Iterable[Row] =
        r._2._1.slice(0, r._2._2.size)

      val trainingDataset = dataset.rdd
        .groupBy(_ (0))
        .join(tr_idx)
        .flatMap(flatMapLambda)
        .map(mapLambda)
        .toDF($(userCol), $(itemCol), $(userCol) + "Org", $(itemCol) + "Org", $(ratingCol))

      val validationDataset: DataFrame = dataset.rdd
        .groupBy(_ (1))
        .join(testIndex)
        .flatMap(validationFlaptMapLambda)
        .map(mapLambda)
        .toDF($(userCol), $(itemCol), $(userCol) + "Org", $(itemCol) + "Org", $(ratingCol))

      (trainingDataset, validationDataset)
    }

    Array(trainingDataset, validationDataset)
  }

  def prepareTestData(validationDataset: DataFrame, recs: DataFrame): Dataset[_] = {
    import org.apache.spark.sql.functions.{collect_list, rank => r}

    val est = $(estimator) match {
      case p: Pipeline => {
        //Assume Rec is last stage of pipeline
        val pipe = $(estimator).asInstanceOf[Pipeline].getStages.last
        pipe match {
          case m: MsftRecommendation => {
            pipe.asInstanceOf[MsftRecommendation]
          }
          case a: ALS => {
            pipe.asInstanceOf[ALS]
          }
        }
      }
      case m: MsftRecommendation => {
        $(estimator).asInstanceOf[MsftRecommendation]
      }
      case a: ALS => {
        $(estimator).asInstanceOf[ALS]
      }
    }

    val userColumn = est.getUserCol
    val itemColumn = est.getItemCol

    val perUserRecommendedItemsDF: DataFrame = recs
      .select(userColumn, "recommendations." + itemColumn)
      .withColumnRenamed(itemColumn, "prediction")

    val windowSpec = Window.partitionBy(userColumn).orderBy(col($(ratingCol)).desc)

    val perUserActualItemsDF = validationDataset
      .select(userColumn, itemColumn, $(ratingCol))
      .withColumn("rank", r().over(windowSpec).alias("rank"))
      .where(col("rank") <= 3)
      .groupBy(userColumn)
      .agg(col(userColumn), collect_list(col(itemColumn)))
      .withColumnRenamed("collect_list(" + itemColumn + ")", "label")
      .select(userColumn, "label")

    val joined_rec_actual = perUserRecommendedItemsDF
      .join(perUserActualItemsDF, userColumn)
      .drop(userColumn)

    joined_rec_actual
  }
}

object TrainValidRecommendSplit extends ComplexParamsReadable[TrainValidRecommendSplit] {
  def popRow(r: Row): Any = r.getDouble(1)
}
