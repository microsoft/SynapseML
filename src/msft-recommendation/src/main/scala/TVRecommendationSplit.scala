// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.{MsftRecommendationParams, TVSplitRecommendationParams}
import org.apache.spark.ml.tuning.TrainValidationSplit
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model, Pipeline, PipelineModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.runtime.universe.{TypeTag, typeTag}
import scala.util.Random

class TVRecommendationSplit(override val uid: String) extends Estimator[TVRecommendationSplitModel]
  with TVSplitRecommendationParams with ComplexParamsWritable with MsftRecommendationParams {

  /** @group setParam */
  def setUserCol(value: String): this.type = set(userCol, value)

  /** @group setParam */
  def setItemCol(value: String): this.type = set(itemCol, value)

  /** @group setParam */
  def setRatingCol(value: String): this.type = set(ratingCol, value)

  def this() = this(Identifiable.randomUID("TVRecommendationSplit"))

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

  def filterRatings(dataset: Dataset[_]): DataFrame = {
    import dataset.sqlContext.implicits._

    val userColumn = $(userCol)
    val tmpDF = dataset
      .groupBy(userColumn)
      .agg(col(userColumn), count(col($(itemCol))))
      .withColumnRenamed("count(" + $(itemCol) + ")", "nitems")
      .where(col("nitems") >= $(minRatingsU))

    val inputDF = dataset.groupBy($(itemCol))
      .agg(col($(itemCol)), count(col($(userCol))))
      .withColumnRenamed("count(" + $(userCol) + ")", "ncustomers")
      .where(col("ncustomers") >= $(minRatingsI))
      .join(dataset, $(itemCol))
      .drop("ncustomers")
      .join(tmpDF, $(userCol))
      .drop("nitems")

    inputDF
  }

  def splitDF(dataset: DataFrame): Array[DataFrame] = {
    import dataset.sqlContext.implicits._

    val nusers_by_item: RDD[Row] = dataset.groupBy($(itemCol))
      .agg(col($(itemCol)), count($(userCol)))
      .withColumnRenamed("count(" + $(userCol) + ")", "nusers")
      .rdd

    def validationFlaptMapLambda(r: (Any, (Iterable[Row], List[Any]))): Iterable[Row] =
      r._2._1.drop(r._2._2.size)

    def testIndexLambda(r: (Any, List[Any], List[Any])): (Any, List[Any]) =
      (r._1, r._2.drop(math.round(r._3.size.toDouble * $(trainRatio)).toInt))

    def mapLambda(r: Row): (Int, Int, String, String, Int) =
      (r.getDouble(0).toInt, r.getDouble(1).toInt, r.getString(2), r.getString(3), r.getInt(4))

    def flatMapLambda(r: (Any, (Iterable[Row], List[Any]))): Iterable[Row] =
      r._2._1.slice(0, r._2._2.size)

    def makeIndexs(r: (Any, List[Any], List[Any])): (Any, List[Any]) =
      (r._1, r._2.slice(0, math.round(r._3.size.toDouble * $(trainRatio)).toInt))

    def permIndicesLambda(r: Row): (Any, List[Any], List[Any]) =
      (r(0), Random.shuffle(List(r(1))), List(r(1)))

    val perm_indices: RDD[(Any, List[Any], List[Any])] = nusers_by_item.map(permIndicesLambda)
    perm_indices.cache()

    val tr_idx: RDD[(Any, List[Any])] = perm_indices.map(makeIndexs)

    val trainingDataset = dataset.rdd
      .groupBy(_ (0))
      .join(tr_idx)
      .flatMap(flatMapLambda)
      .map(mapLambda)
      .toDF($(userCol), $(itemCol), $(userCol) + "Org", $(itemCol) + "Org", $(ratingCol))

    val testIndex: RDD[(Any, List[Any])] = perm_indices.map(testIndexLambda)

    val validationDataset: DataFrame = dataset.rdd
      .groupBy(_ (1))
      .join(testIndex)
      .flatMap(validationFlaptMapLambda)
      .map(mapLambda)
      .toDF($(userCol), $(itemCol), $(userCol) + "Org", $(itemCol) + "Org", $(ratingCol))

    perm_indices.unpersist()
    trainingDataset.count()
    Array(dataset, dataset)
  }

  def prepareTestData(validationDataset: DataFrame, recs: DataFrame): Dataset[_] = {
    import validationDataset.sqlContext.implicits._
    import org.apache.spark.sql.functions.{rank => r, collect_list}
    val est = $(estimator).asInstanceOf[Pipeline]
      .getStages($(estimator).asInstanceOf[Pipeline].getStages.length-1).asInstanceOf[MsftRecommendation]

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

  override def fit(dataset: Dataset[_]): TVRecommendationSplitModel = {
    val schema = dataset.schema
    transformSchema(schema, logging = true)
    val est = $(estimator)
    val eval = $(evaluator)
    val epm = $(estimatorParamMaps)
    val numModels = epm.length
    val metrics = new Array[Double](epm.length)

    val ratings = dataset.dropDuplicates()

    val inputDF = filterRatings(ratings)
    inputDF.cache()

    val Array(trainingDataset, validationDataset): Array[DataFrame] = splitDF(inputDF)
    //    val Array(trainingDataset, validationDataset) = Array(inputDF, inputDF)
    trainingDataset.cache()
    validationDataset.cache()

    inputDF.unpersist()

    val models = est.fit(trainingDataset, epm).asInstanceOf[Seq[Model[_]]]
    trainingDataset.unpersist()
    var i = 0
    //noinspection ScalaStyle
    while (i < numModels) {
      // TODO: duplicate evaluator to take extra params from input
      models(i) match {
        case n: PipelineModel => {
          val recs = models(i).asInstanceOf[PipelineModel]
            .stages(2).asInstanceOf[MsftRecommendationModel].recommendForAllUsers(3)
          val preparedTest: Dataset[_] = prepareTestData(models(i).transform(validationDataset), recs)
          val metric = eval.evaluate(preparedTest)
          logDebug(s"Got metric $metric for model trained with ${epm(i)}.")
          metrics(i) += metric
        }
      }
      i += 1
    }
    validationDataset.unpersist()

    val (bestMetric, bestIndex) =
      if (eval.isLargerBetter) metrics.zipWithIndex.maxBy(_._1)
      else metrics.zipWithIndex.minBy(_._1)

    val bestModel = est.fit(dataset, epm(bestIndex)).asInstanceOf[Model[_]]
    copyValues(new TVRecommendationSplitModel(uid, bestModel, metrics).setParent(this))
  }

  override def copy(extra: ParamMap): TVRecommendationSplit = defaultCopy(extra)
}

object TVRecommendationSplit extends ComplexParamsReadable[TVRecommendationSplit] {
  def popRow(r: Row): Any = r.getDouble(1)
}

class TVRecommendationSplitModel(
                                  override val uid: String,
                                  val bestModel: Model[_],
                                  val validationMetrics: Array[Double])
  extends Model[TVRecommendationSplitModel] with TVSplitRecommendationParams
    with ConstructorWritable[TVRecommendationSplitModel] {

  override def copy(extra: ParamMap): TVRecommendationSplitModel = {
    val copied = new TVRecommendationSplitModel(uid, bestModel, validationMetrics)
    copyValues(copied, extra).setParent(parent)
  }

  override val ttag: TypeTag[TVRecommendationSplitModel] = typeTag[TVRecommendationSplitModel]

  override def objectsToSave: List[AnyRef] =
    List(uid, bestModel, validationMetrics)

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    bestModel.transform(dataset)
  }

  override def transformSchema(schema: StructType): StructType = {
    bestModel.transformSchema(schema)
  }
}

object TVRecommendationSplitModel extends ConstructorReadable[TVRecommendationSplitModel]
