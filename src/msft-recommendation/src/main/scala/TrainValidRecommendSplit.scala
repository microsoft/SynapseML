// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml._
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation._
import org.apache.spark.ml.util._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable
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
    dataset.cache()
    eval.setNItems(dataset.agg(countDistinct(col($(itemCol)))).take(1)(0).getLong(0))
    val filteredDataset = filterRatings(dataset.dropDuplicates())

    val Array(trainingDataset, validationDataset): Array[DataFrame] = splitDF(filteredDataset)
    trainingDataset.cache()
    validationDataset.cache()
    val models = est.fit(trainingDataset, epm).asInstanceOf[Seq[Model[_]]]

    def calculateMetrics(model: Transformer, validationDataset: Dataset[_]): Double = model match {
      case p: PipelineModel => {
        //Assume Rec is last stage of pipeline
        val modelTemp = model.asInstanceOf[PipelineModel].stages.last
        calculateMetrics(modelTemp, validationDataset)
      }
      case m: MsftRecommendationModel => {
        val recs = model.asInstanceOf[MsftRecommendationModel].recommendForAllUsers(5 * eval.getK)
        val preparedTest: Dataset[_] = prepareTestData(model.transform(validationDataset), recs, eval.getK)
        eval.evaluate(preparedTest)
      }
      case a: ALSModel => {
        val recs = model.asInstanceOf[ALSModel].recommendForAllUsers(5 * eval.getK)
        val preparedTest: Dataset[_] = prepareTestData(model.transform(validationDataset), recs, eval.getK)
        eval.evaluate(preparedTest)
      }
      case s: SARModel => {
        val recs = model.asInstanceOf[SARModel].recommendForAllUsers(5 * eval.getK)
        val preparedTest: Dataset[_] = prepareTestData(model.transform(validationDataset), recs, eval.getK)
        eval.evaluate(preparedTest)
      }
      //      case trait: MsftRecommendationModelParams => {
      //        val recs = model.asInstanceOf[MsftRecommendationModelParams].recommendForAllUsers(5 * eval.getK)
      //        val preparedTest: Dataset[_] = prepareTestData(model.transform(validationDataset), recs, eval.getK)
      //        eval.evaluate(preparedTest)
      //      }
    }

    var i = 0
    while (i < numModels) {
      // TODO: duplicate evaluator to take extra params from input
      val metric = calculateMetrics(models(i), validationDataset)
      logDebug(s"Got metric $metric for model trained with ${epm(i)}.")
      metrics(i) += metric
      i += 1
    }

    val (bestMetric, bestIndex) =
      if (eval.isLargerBetter) metrics.zipWithIndex.maxBy(_._1)
      else metrics.zipWithIndex.minBy(_._1)

    val bestModel = est.fit(dataset, epm(bestIndex)).asInstanceOf[Model[_]]
    copyValues(new TrainValidRecommendSplitModel(uid, bestModel, metrics).setParent(this))
  }

  override def copy(extra: ParamMap): TrainValidRecommendSplit = defaultCopy(extra)

  private def filterByItemCount(dataset: Dataset[_]): DataFrame = {
    dataset
      .groupBy($(userCol))
      .agg(col($(userCol)), count(col($(itemCol))))
      .withColumnRenamed("count(" + $(itemCol) + ")", "nitems")
      .where(col("nitems") >= $(minRatingsU))
      .drop("nitems")
      .cache()
  }

  private def filterByUserRatingCount(dataset: Dataset[_]): DataFrame = dataset
    .groupBy($(itemCol))
    .agg(col($(itemCol)), count(col($(userCol))))
    .withColumnRenamed("count(" + $(userCol) + ")", "ncustomers")
    .where(col("ncustomers") >= $(minRatingsI))
    .join(dataset, $(itemCol))
    .drop("ncustomers")
    .cache()

  def filterRatings(dataset: Dataset[_]): DataFrame = filterByUserRatingCount(dataset)
    .join(filterByItemCount(dataset), $(userCol))

  def splitDF(dataset: DataFrame): Array[DataFrame] = {
    val shuffleFlag = true
    val shuffleBC = dataset.sparkSession.sparkContext.broadcast(shuffleFlag)

    val wrapColumn = udf((itemId: Double, rating: Double) => Array(itemId, rating))
    val sliceudf = udf(
      (r: mutable.WrappedArray[Array[Double]]) => r.slice(0, math.round(r.length * $(trainRatio)).toInt))
    val shuffle = udf((r: mutable.WrappedArray[Array[Double]]) =>
      if (shuffleBC.value) Random.shuffle(r.toSeq)
      else r
    )
    val dropudf = udf((r: mutable.WrappedArray[Array[Double]]) => r.drop(math.round(r.length * $(trainRatio)).toInt))
    val popLeft = udf((r: mutable.WrappedArray[Double]) => r(0))
    val popRight = udf((r: mutable.WrappedArray[Double]) => r(1))

    val testds = dataset
      .withColumn("itemIDRating", wrapColumn(col($(itemCol)), col($(ratingCol))))
      .groupBy(col($(userCol)))
      .agg(collect_list(col("itemIDRating")))
      .withColumn("shuffle", shuffle(col("collect_list(itemIDRating)")))
      .withColumn("train", sliceudf(col("shuffle")))
      .withColumn("test", dropudf(col("shuffle")))
      //      .withColumn("train2", explode(sliceudf(col("shuffle"))))
      //      .withColumn($(itemCol) + "train2", popLeft(col("train2")))
      //      .withColumn($(ratingCol) + "train2", popRight(col("train2")))
      //      .withColumn("test2", explode(dropudf(col("shuffle"))))
      //      .withColumn($(itemCol) + "test2", popLeft(col("test2")))
      //      .withColumn($(ratingCol) + "test2", popRight(col("test2")))
      //      .withColumn("train", sliceudf(col("collect_list(itemIDRating)")))
      //      .withColumn("test", dropudf(col("collect_list(itemIDRating)")))
      .drop(col("collect_list(itemIDRating)")).drop(col("shuffle"))
      .cache()

    //    val train2 = testds.select(
    //      col("customerID"),
    //      col($(itemCol) + "train2").as($(itemCol)),
    //      col($(ratingCol) + "train2").as($(ratingCol))
    //    )
    //    val test2 = testds.select(
    //      col("customerID"),
    //      col($(itemCol) + "test2").as($(itemCol)),
    //      col($(ratingCol) + "test2").as($(ratingCol))
    //    )

    val train = testds.select($(userCol), "train")
      .withColumn("itemIdRating", explode(col("train")))
      .drop("train")
      .withColumn($(itemCol), popLeft(col("itemIdRating")))
      .withColumn($(ratingCol), popRight(col("itemIdRating")))
      .drop("itemIdRating")

    val test = testds.select($(userCol), "test")
      .withColumn("itemIdRating", explode(col("test")))
      .drop("test")
      .withColumn($(itemCol), popLeft(col("itemIdRating")))
      .withColumn($(ratingCol), popRight(col("itemIdRating")))
      .drop("itemIdRating")

    Array(train, test)
  }

  def prepareTestData(validationDataset: DataFrame, recs: DataFrame, k: Int): Dataset[_] = {
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
          case s: SAR => {
            pipe.asInstanceOf[SAR]
          }
        }
      }
      case m: MsftRecommendation => {
        $(estimator).asInstanceOf[MsftRecommendation]
      }
      case a: ALS => {
        $(estimator).asInstanceOf[ALS]
      }
      case s: SAR => {
        $(estimator).asInstanceOf[SAR]
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
      .where(col("rank") <= k)
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
