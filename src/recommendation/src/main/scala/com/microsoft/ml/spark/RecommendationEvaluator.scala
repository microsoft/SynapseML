// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.recommendation.RecEvaluatorParams
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.evaluation.RankingMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}

import scala.reflect.ClassTag

class AdvancedRankingMetrics[T: ClassTag](predictionAndLabels: RDD[(Array[T], Array[T])],
                                          k: Int, nItems: Long)
  extends Serializable {
  val metrics = new RankingMetrics[T](predictionAndLabels)

  lazy val uniqueItemsRecommended: Array[T] = predictionAndLabels
    .map(row => row._1)
    .reduce((x, y) => x.toSet.union(y.toSet).toArray)

  lazy val map: Double = metrics.meanAveragePrecision
  lazy val ndcg: Double = metrics.ndcgAt(k)
  lazy val mapk: Double = metrics.precisionAt(k)
  lazy val recallAtK: Double = predictionAndLabels.map(r =>
    r._1.distinct.intersect(r._2.distinct).length.toDouble / r._1.length.toDouble).mean()
  lazy val diversityAtK: Double = {
    uniqueItemsRecommended.length.toDouble / nItems
  }
  lazy val maxDiversity: Double = {
    val itemCount = predictionAndLabels
      .map(row => row._2)
      .reduce((x, y) => x.toSet.union(y.toSet).toArray)
      .union(uniqueItemsRecommended).toSet
      .size
    itemCount.toDouble / nItems
  }
  lazy val meanReciprocalRank: Double = {
    predictionAndLabels.map { case (pred, lab) =>
      val labSet = lab.toSet

      if (labSet.nonEmpty) {
        var i = 0
        var reciprocalRank = 0.0
        while (i < pred.length && reciprocalRank == 0.0) {
          if (labSet.contains(pred(i))) {
            reciprocalRank = 1.0 / (i + 1)
          }
          i += 1
        }
        reciprocalRank
      } else {
        0.0
      }
    }.mean()
  }
  lazy val fractionConcordantPairs: Double = {
    predictionAndLabels.map { case (pred, lab) =>
      var nc = 0.0
      var nd = 0.0
      pred.zipWithIndex.foreach(a => {
        if (lab.length > a._2) {
          if (a._1 == lab(a._2)) nc += 1
          else nd += 1
        }
      })
      nc / (nc + nd)
    }.mean()
  }

  def matchMetric(metricName: String): Double = metricName match {
    case "map" => map
    case "ndcgAt" => ndcg
    case "precisionAtk" => mapk
    case "recallAtK" => recallAtK
    case "diversityAtK" => diversityAtK
    case "maxDiversity" => maxDiversity
  }

  def getAllMetrics: Map[String, Double] = {
    Map("map" -> map,
      "ndcgAt" -> ndcg,
      "precisionAtk" -> mapk,
      "recallAtK" -> recallAtK,
      "diversityAtK" -> diversityAtK,
      "maxDiversity" -> maxDiversity,
      "mrr" -> meanReciprocalRank,
      "fcp" -> fractionConcordantPairs)
  }
}

trait HasRecommenderCols extends Params {
  val userCol = new Param[String](this, "userCol", "Column of users")

  /** @group setParam */
  def setUserCol(value: String): this.type = set(userCol, value)

  def getUserCol:String =  $(userCol)

  val itemCol = new Param[String](this, "itemCol", "Column of items")

  /** @group setParam */
  def setItemCol(value: String): this.type = set(itemCol, value)

  def getItemCol:String =  $(itemCol)

  val ratingCol = new Param[String](this, "ratingCol", "Column of ratings")

  /** @group setParam */
  def setRatingCol(value: String): this.type = set(ratingCol, value)

  def getRatingCol:String =  $(ratingCol)

}

class RecommendationEvaluator(override val uid: String)
  extends Evaluator with RecEvaluatorParams
    with RecommendationSplitParams with HasRecommenderCols {

  def this() = this(Identifiable.randomUID("recEval"))

  val nItems: LongParam = new LongParam(this, "nItems", "number of items")

  def setNItems(value: Long): this.type = set(nItems, value)

  def getNItems: Long = $(nItems)

  val k: IntParam = new IntParam(this, "k",
    "number of items", ParamValidators.inRange(1, Integer.MAX_VALUE))
  setDefault(k -> 1)

  /** @group getParam */
  def getK: Int = $(k)

  /** @group setParam */
  def setK(value: Int): this.type = set(k, value)

  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("ndcgAt", "map", "mapk", "recallAtK", "diversityAtK",
      "maxDiversity", "mrr", "fcp"))
    new Param(this, "metricName", "metric name in evaluation " +
      "(ndcgAt|map|precisionAtk|recallAtK|diversityAtK|maxDiversity|mrr|fcp)", allowedParams)
  }
  setDefault(metricName -> "ndcgAt", nItems -> -1)


  /** @group getParam */
  def getMetricName: String = $(metricName)

  /** @group setParam */
  def setMetricName(value: String): this.type = set(metricName, value)

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def getMetrics(dataset: Dataset[_]): AdvancedRankingMetrics[Any] = {
    val predictionAndLabels = dataset
      .select(getPredictionCol, getLabelCol)
      .rdd.map { case Row(prediction: Seq[Any], label: Seq[Any]) => (prediction.toArray, label.toArray) }
      .cache()

    new AdvancedRankingMetrics[Any](predictionAndLabels, getK, getNItems)
    //TODO come back to this Any type
  }

  def getMetricsMap(dataset: Dataset[_]): Map[String, Double] = {
    getMetrics(dataset).getAllMetrics
  }

  override def evaluate(dataset: Dataset[_]): Double = {
    getMetrics(dataset).matchMetric(getMetricName)
  }

  override def copy(extra: ParamMap): RecommendationEvaluator = defaultCopy(extra)

}
