// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.recommendation

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import breeze.linalg.{CSCMatrix => BSM, DenseMatrix => BDM, Matrix => BM}
import com.microsoft.ml.spark.codegen.Wrappable
import com.microsoft.ml.spark.logging.BasicLogging
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param._
import org.apache.spark.ml.recommendation.{RecommendationParams, Constants => C}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{DenseVector, Matrices, SparseMatrix}
import org.apache.spark.sql.functions.{col, collect_list, sum, udf, _}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.language.existentials

/**
  * Smart Adaptive Recommendations (SAR) Algorithm
  *
  * https://aka.ms/reco-sar
  *
  * SAR is a fast scalable adaptive algorithm for personalized recommendations based on user transactions history and
  * items description. It produces easily explainable / interpretable recommendations
  *
  * SAR has been show to provide higher ranking measurements when compared to ALS.
  * https://github.com/Microsoft/Recommenders
  *
  * @param uid The id of the module
  */
class SAR(override val uid: String) extends Estimator[SARModel]
  with SARParams with DefaultParamsWritable with BasicLogging {
  logClass()

  /** @group getParam */
  def getSimilarityFunction: String = $(similarityFunction)

  /** @group getParam */
  def getTimeCol: String = $(timeCol)

  /** @group getParam */
  def getSupportThreshold: Int = $(supportThreshold)

  /** @group getParam */
  def getStartTimeFormat: String = $(startTimeFormat)

  /** @group getParam */
  def getActivityTimeFormat: String = $(activityTimeFormat)

  /** @group getParam */
  def getTimeDecayCoeff: Int = $(timeDecayCoeff)

  def this() = this(Identifiable.randomUID("SAR"))

  override def copy(extra: ParamMap): SAR = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def fit(dataset: Dataset[_]): SARModel = {
    logFit({
      new SARModel(uid)
        .setUserDataFrame(calculateUserItemAffinities(dataset))
        .setItemDataFrame(calculateItemItemSimilarity(dataset))
        .setParent(this)
        .setSupportThreshold(getSupportThreshold)
        .setItemCol(getItemCol)
        .setUserCol(getUserCol)
    })
  }

  /**
    * Item-to-Item similarity matrix contains for each pair of items a numerical value of similarity between these two
    * items. A simple measure of item similarity is co-occurrence, which is the number of times two items appeared in a
    * same transaction.
    *
    * @param dataset
    * @return
    */
  private[spark] def calculateUserItemAffinities(dataset: Dataset[_]): DataFrame = {
    val referenceTime: Date = new SimpleDateFormat(getStartTimeFormat)
      .parse(get(startTime).getOrElse(Calendar.getInstance().getTime.toString))

    //Time Decay calculates the half life since the reference time
    val timeDecay = udf((time: String) => {
      val activityDate = new SimpleDateFormat(getActivityTimeFormat).parse(time)
      val timeDifference = (referenceTime.getTime - activityDate.getTime) / (1000 * 60)
      math.pow(2, -1.0 * timeDifference / (getTimeDecayCoeff * 24 * 60))
    })
    val blendWeights = udf((theta: Double, rho: Double) => theta * rho)
    val fillOne = udf((_: String) => 1)

    val itemCount = dataset.select(col(getItemCol)).groupBy().max(getItemCol).collect()(0).getDouble(0).toInt
    val numItems = dataset.sparkSession.sparkContext.broadcast(itemCount)

    val columnsToArray = udf((itemId: Double, rating: Double) => Array(itemId, rating))

    val seqToArray = udf((itemUserAffinityPairs: Seq[Seq[Double]]) => {
      val map = itemUserAffinityPairs.map(r => r.head.toInt -> r(1)).toMap
      (0 to numItems.value).map(i => map.getOrElse(i, 0.0).toFloat).toArray
    })

    dataset
      .withColumn(C.AffinityCol, (dataset.columns.contains(getTimeCol), dataset.columns.contains(getRatingCol)) match {
        case (true, true)   => blendWeights(timeDecay(col(getTimeCol)), col(getRatingCol))
        case (true, false)  => timeDecay(col(getTimeCol))
        case (false, true)  => col(getRatingCol)
        case (false, false) => fillOne(col(getUserCol))
      }).select(getUserCol, getItemCol, C.AffinityCol)
      .groupBy(getUserCol, getItemCol).agg(sum(col(C.AffinityCol)) as C.AffinityCol)
      .withColumn("itemUserAffinityPair", columnsToArray(col(getItemCol), col(C.AffinityCol)))
      .groupBy(getUserCol).agg(collect_list(col("itemUserAffinityPair")))
      .withColumn("flatList", seqToArray(col("collect_list(itemUserAffinityPair)")))
      .select(col(getUserCol), col("flatList"))
  }

  /**
    * User-to-Item affinity matrix contains for each user-item pair an affinity score of the user towards the item.
    * Affinity score is computed as a weighted number of transactions in which the user and the item appear together,
    * where newer transactions are weighted more than the older transactions.
    *
    * Diagonal elements, occ(Item i), simply represent the number of occurrences of each item. The advantage of
    * co-occurrence is that it is very easy to update. However, it favors predictability, and the most popular items
    * will be recommended most of the time. To alleviate that, two additional similarity measures are used: lift and
    * Jaccard. They can be thought of as normalized co-occurrences.
    *
    * Lift measures how much the co-occurrence of two items is higher than it would be by chance, i.e., what is the
    * contribution of interaction of the two items. It is obtained as
    *
    * lift(Item i, Item j) = cooccur(Item i, Item j) / (occ(Item i) * occ(Item j)) .
    *
    * Lift favors serendipity / discoverability. For example, items 2 and 5 have the same co-occurrence with item 4,
    * but item 5 in general occurs less frequently than item 2 and will be favored by lift.
    *
    *
    * Jaccard measure is defined as the number of transaction in which two items appear together divided by the
    * number of transactions in which either of them appears:
    *
    * Jaccard(Item 1, Item 2) = cooccur(Item1, Item 2) / (occ(Item 1) + occ(Item 2) - cooccur(Item 1, Item 2)) .
    *
    * Jaccard measure is a tradeoff between co-occurrence and lift and is the default in SAR.
    *
    * @param dataset
    * @return
    */
  private[spark] def calculateItemItemSimilarity(dataset: Dataset[_]): DataFrame = {

    val itemCounts = dataset//.cache
      .groupBy(col(getItemCol)).agg(countDistinct(col(getUserCol)))
      .collect.map(r => r.get(0) -> r.getLong(1)).toMap

    val broadcastItemCounts = dataset.sparkSession.sparkContext.broadcast(itemCounts)

    val maxCounts = dataset.agg(max(col(getUserCol)), max(col(getItemCol))).take(1)(0)
    val userCount = maxCounts.getDouble(0).toInt + 1
    val itemCount = maxCounts.getDouble(1).toInt + 1

    val broadcastMatrix = {
      val sparse = SparseMatrix.fromCOO(userCount, itemCount,
        dataset
          .groupBy(getUserCol, getItemCol).agg(count(getItemCol))
          .select(col(getUserCol), col(getItemCol))
          .collect.map(userItemPair => (userItemPair.getDouble(0).toInt, userItemPair.getDouble(1).toInt, 1.0)))
      dataset.sparkSession.sparkContext.broadcast(
        new BSM[Double](sparse.values, sparse.numRows, sparse.numCols, sparse.colPtrs, sparse.rowIndices)
      )
    }

    val createItemFeaturesVector = udf((users: Seq[Double]) => {
      val vec = Array.fill[Double](userCount)(0.0)
      users.foreach(user => vec(user.toInt) = 1.0)
      val sm = Matrices.dense(1, vec.length, vec).asML.toSparse
      val smBSM: BSM[Double] = new BSM[Double](sm.values, sm.numRows, sm.numCols, sm.colPtrs, sm.rowIndices)
      val value: BSM[Double] = smBSM * broadcastMatrix.value
      new DenseVector(value.toDense.toArray)
    })

    val calculateFeature = udf((itemID: Double, features: linalg.Vector) => {
      val countI = features.apply(itemID.toInt)
      features.toArray.indices.map(i => {
        val countJ: Long = broadcastItemCounts.value.getOrElse(i, 0)
        val cooco = features.apply(i)
        if (!(cooco < getSupportThreshold)) {
          getSimilarityFunction match {
            case "jaccard" => (cooco / (countI + countJ - cooco)).toFloat
            case "lift"    => (cooco / (countI * countJ)).toFloat
            case _         => cooco.toFloat
          }
        }
        else 0
      })
    })

    dataset
      .select(col(getItemCol), col(getUserCol))
      .groupBy(getItemCol).agg(collect_list(getUserCol) as "collect_list")
      .withColumn(C.FeaturesCol, createItemFeaturesVector(col("collect_list")))
      .select(col(getItemCol), col(C.FeaturesCol))
      .withColumn(C.ItemAffinities, calculateFeature(col(getItemCol), col(C.FeaturesCol)))
      .select(col(getItemCol), col(C.ItemAffinities))
  }
}

object SAR extends DefaultParamsReadable[SAR]

trait SARParams extends Wrappable with RecommendationParams {

  /** @group setParam */
  def setSimilarityFunction(value: String): this.type = set(similarityFunction, value)

  val similarityFunction = new Param[String](this, "similarityFunction",
    "Defines the similarity function to be used by " +
      "the model. Lift favors serendipity, Co-occurrence favors predictability, " +
      "and Jaccard is a nice compromise between the two.")

  /** @group setParam */
  def setTimeCol(value: String): this.type = set(timeCol, value)

  val timeCol = new Param[String](this, "timeCol", "Time of activity")

  /** @group setParam */
  def setUserCol(value: String): this.type = set(userCol, value)

  /** @group setParam */
  def setItemCol(value: String): this.type = set(itemCol, value)

  /** @group setParam */
  def setRatingCol(value: String): this.type = set(ratingCol, value)

  def setSupportThreshold(value: Int): this.type = set(supportThreshold, value)

  val supportThreshold = new IntParam(this, "supportThreshold", "Minimum number of ratings per item")

  def setStartTime(value: String): this.type = set(startTime, value)

  val startTime = new Param[String](this, "startTime", "Set time custom now time if using historical data")

  def setActivityTimeFormat(value: String): this.type = set(activityTimeFormat, value)

  val activityTimeFormat = new Param[String](this, "activityTimeFormat", "Time format for events, " +
    "default: yyyy/MM/dd'T'h:mm:ss")

  def setTimeDecayCoeff(value: Int): this.type = set(timeDecayCoeff, value)

  val timeDecayCoeff = new IntParam(this, "timeDecayCoeff", "Use to scale time decay coeff to different half life dur")

  def setStartTimeFormat(value: String): this.type = set(startTimeFormat, value)

  val startTimeFormat = new Param[String](this, "startTimeFormat", "Format for start time")

  setDefault(timeDecayCoeff -> 30, activityTimeFormat -> "yyyy/MM/dd'T'h:mm:ss", supportThreshold -> 4,
    ratingCol -> C.RatingCol, userCol -> C.UserCol, itemCol -> C.ItemCol, similarityFunction ->
      "jaccard", timeCol -> "time", startTimeFormat -> "EEE MMM dd HH:mm:ss Z yyyy")
}
