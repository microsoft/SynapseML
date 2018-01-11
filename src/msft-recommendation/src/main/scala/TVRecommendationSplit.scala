// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.TVSplitRecommendationParams
import org.apache.spark.ml.tuning.TrainValidationSplit
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.{TypeTag, typeTag}
import scala.util.Random

class TVRecommendationSplit(override val uid: String) extends Estimator[TVRecommendationSplitModel]
  with TVSplitRecommendationParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("tvrs"))

  /** @group setParam */
  def setEstimator(value: Estimator[_]): this.type = set(estimator, value)

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

    val tmpDF = dataset
      .groupBy("customerID")
      .agg('customerID, count('itemID))
      .withColumnRenamed("count(itemID)", "nitems")
      .where(col("nitems") >= $(minRatingsU))

    val inputDF = dataset.groupBy("itemID")
      .agg('itemID, count('customerID))
      .withColumnRenamed("count(customerID)", "ncustomers")
      .where(col("ncustomers") >= $(minRatingsI))
      .join(dataset, "itemID")
      .drop("ncustomers")
      .join(tmpDF, "customerID")
      .drop("nitems")

    inputDF
  }

  def splitDF(dataset: DataFrame): Array[DataFrame] = {
    import dataset.sqlContext.implicits._

    val nusers_by_item: RDD[Row] = dataset.groupBy("itemID")
      .agg('itemID, count("customerID"))
      .withColumnRenamed("count(customerID)", "nusers")
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

    val trainingDataset: DataFrame = dataset.rdd
      .groupBy(_ (1))
      .join(tr_idx)
      .flatMap(flatMapLambda)
      .map(mapLambda)
      .toDF("customerID", "itemID", "customerIDOrg", "itemIDOrg", "rating")

    val testIndex: RDD[(Any, List[Any])] = perm_indices.map(testIndexLambda)

    val validationDataset: DataFrame = dataset.rdd
      .groupBy(_ (1))
      .join(testIndex)
      .flatMap(validationFlaptMapLambda)
      .map(mapLambda)
      .toDF("customerID", "itemID", "customerIDOrg", "itemIDOrg", "rating")

    perm_indices.unpersist()

    Array(trainingDataset, validationDataset)
  }

  def prepareTestData(validationDataset: DataFrame, recs: DataFrame): Dataset[_] = {
    import validationDataset.sqlContext.implicits._
    import org.apache.spark.sql.functions._

    //    val nUsers = validationDataset.map(_ (0)).distinct().count()
    //
    //    val nItems = validationDataset.map(_ (0)).distinct().count()
    //
    //    val nRatings = validationDataset.count()
//    val perUserRecommendedItemsDF: DataFrame = recs
//      .select("user", "product")
//      .withColumnRenamed("user", "customerID")
//      .groupBy("customerID")
//      .agg('customerID, collect_list('product))
//      .withColumnRenamed("collect_list(product)", "recommended")

    val perUserRecommendedItemsDF: DataFrame = recs
      .select("customerID", "recommendations")
      .groupBy("customerID")
      .agg('customerID, collect_list('recommendations))
      .withColumnRenamed("collect_list(recommendations)", "recommended")

    val windowSpec = Window.partitionBy("customerID").orderBy(col("rating").desc)

//    val perUserActualItemsDF = validationDataset
//      .select("customerID", "itemID", "rating")
//      .withColumn("rank", rank().over(windowSpec).alias("rank"))
//      .where(col("rank") <= 3)
//      .groupBy("customerID")
//      .agg('customerID, collect_list('product))
//      .withColumnRenamed("collect_list(product)", "actual")

    val perUserActualItemsDF = validationDataset
      .select("customerID", "itemID", "rating")
      .withColumn("rank", rank().over(windowSpec).alias("rank"))
      .where(col("rank") <= 3)
      .groupBy("customerID")
      .agg('customerID, collect_list('itemID))
      .withColumnRenamed("collect_list(itemID)", "actual")

    val joined_rec_actual = perUserRecommendedItemsDF
      .join(perUserActualItemsDF, "customerID")
      .drop("customerID")

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

//    val Array(trainingDataset, validationDataset): Array[DataFrame] = splitDF(inputDF)
    val Array(trainingDataset, validationDataset) = Array(inputDF, inputDF)
    trainingDataset.cache()
    validationDataset.cache()

    inputDF.unpersist()

    val models = est.fit(trainingDataset, epm).asInstanceOf[Seq[Model[_]]]
    trainingDataset.unpersist()
    var i = 0
    //noinspection ScalaStyle
    while (i < numModels) {
      // TODO: duplicate evaluator to take extra params from input
      val recs = models(i).asInstanceOf[MsftRecommendationModel].recommendForAllUsers(3)
      val preparedTest: Dataset[_] = prepareTestData(validationDataset, recs)
      val metric = eval.evaluate(preparedTest)
      logDebug(s"Got metric $metric for model trained with ${epm(i)}.")
      metrics(i) += metric
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

object TVRecommendationSplit extends DefaultParamsReadable[TVRecommendationSplit]

class TVRecommendationSplitModel(
                                  override val uid: String,
                                  val bestModel: Model[_],
                                  val validationMetrics: Array[Double])
  extends Model[TVRecommendationSplitModel] with TVSplitRecommendationParams
    with ConstructorWritable[TVRecommendationSplitModel] {

  def recommendForAllItems(i: Int): DataFrame = {
    bestModel.asInstanceOf[MsftRecommendationModel].recommendForAllItems(i)
  }

  def recommendForAllUsers(i: Int): DataFrame = {
    bestModel.asInstanceOf[MsftRecommendationModel].recommendForAllUsers(i)
  }

  override def copy(extra: ParamMap): TVRecommendationSplitModel = {
    val copied = new TVRecommendationSplitModel(uid, bestModel, validationMetrics)
    copyValues(copied, extra).setParent(parent)
  }

  override val ttag: TypeTag[TVRecommendationSplitModel] = typeTag[TVRecommendationSplitModel]

  override def objectsToSave: List[AnyRef] =
    List(uid, bestModel.asInstanceOf[MsftRecommendationModel], validationMetrics)

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    bestModel.transform(dataset)
  }

  override def transformSchema(schema: StructType): StructType = {
    bestModel.transformSchema(schema)
  }
}

object TVRecommendationSplitModel extends ConstructorReadable[TVRecommendationSplitModel]
