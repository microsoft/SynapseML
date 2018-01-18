// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.recommendation.MsftRecEvaluatorParams
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.evaluation.RankingMetrics
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable.ListBuffer

@InternalWrapper
final class MsftRecommendationEvaluator(override val uid: String)
  extends Evaluator with MsftRecEvaluatorParams {
  var nItems: Long = -1

  val metricsList = new ListBuffer[Map[String, Double]]()

  def printMetrics(): Unit = {
    metricsList.foreach(map =>{
      print(map.toString())
    })
  }

  def getMetricsList: ListBuffer[Map[String, Double]] = metricsList

  def setNumberItems(value: Long): this.type = {
    this.nItems = value
    this
  }

  def this() = this(Identifiable.randomUID("recEval"))

  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("ndcgAt", "map", "mapk", "recallAtK", "diversityAtK",
      "maxDiversity"))
    new Param(this, "metricName", "metric name in evaluation " +
      "(ndcgAt|map|mapk|recallAtK|diversityAtK|maxDiversity)", allowedParams)
  }

  val saveAll: BooleanParam = new BooleanParam(this, "saveAll", "save all metrics")

  /** @group getParam */
  def getSaveAll: Boolean = $(saveAll)

  /** @group setParam */
  def setSaveAll(value: Boolean): this.type = set(saveAll, value)

  setDefault(saveAll -> false)

  val k: IntParam = new IntParam(this, "k",
    "number of items", ParamValidators.inRange(1, Integer.MAX_VALUE))

  /** @group getParam */
  def getK: Int = $(k)

  /** @group setParam */
  def setK(value: Int): this.type = set(k, value)

  setDefault(k -> 1)

  /** @group getParam */
  def getMetricName: String = $(metricName)

  /** @group setParam */
  def setMetricName(value: String): this.type = set(metricName, value)

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  setDefault(metricName -> "ndcgAt")

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  override def evaluate(dataset: Dataset[_]): Double = {
    val schema = dataset.schema
    val predictionType = schema($(predictionCol)).dataType
    val labelType = schema($(labelCol)).dataType

    val predictionAndLabels = dataset
      .select(col($(predictionCol)).cast(predictionType), col($(labelCol)).cast(labelType)).rdd
      .map { case Row(prediction: Seq[Any], label: Seq[Any]) => (prediction.toArray, label.toArray) }

    val metrics = new RankingMetrics[Any](predictionAndLabels)

    class AdvancedRankingMetrics(rankingMetrics: RankingMetrics[Any]) {
      def recallAtK(): Double = predictionAndLabels.map(r =>
        r._1.toSet.intersect(r._2.toSet).size.toDouble / r._1.size.toDouble).mean()

      def diversityAtK(): Double = {
        val uniqueItemsRecommended = predictionAndLabels.map(row => row._1)
          .reduce((x, y) => x.toSet.union(y.toSet).toArray)
        uniqueItemsRecommended.length.toDouble / nItems
      }

      def maxDiversity(): Double = {
        val uniqueItemsRecommended = predictionAndLabels.map(row => row._1)
          .reduce((x, y) => x.toSet.union(y.toSet).toArray)
        val uniqueItemsSeen = predictionAndLabels.map(row => row._2)
          .reduce((x, y) => x.toSet.union(y.toSet).toArray)
        val items = uniqueItemsSeen.union(uniqueItemsRecommended).length
        items.toDouble / nItems
      }

      def matchMetric(metricName: String): Double = metricName match {
        case "map" => metrics.meanAveragePrecision
        case "ndcgAt" => metrics.ndcgAt($(k))
        case "mapk" => metrics.precisionAt($(k))
        case "recallAtK" => metrics.recallAtK()
        case "diversityAtK" => metrics.diversityAtK()
        case "maxDiversity" => metrics.maxDiversity()
      }
    }

    import scala.language.implicitConversions
    implicit def aRankingMetricsToRankingMetrics(rankingMetrics: RankingMetrics[Any]): AdvancedRankingMetrics =
      new AdvancedRankingMetrics(rankingMetrics)

    if (getSaveAll) {
      val map = metrics.meanAveragePrecision
      val ndcgAt = metrics.ndcgAt($(k))
      val mapk = metrics.precisionAt($(k))
      val recallAtK = metrics.recallAtK()
      val diversityAtK = metrics.diversityAtK()
      val maxDiversity = metrics.maxDiversity()
      val allMetrics = Map("map" -> map, "ndcgAt" -> ndcgAt, "mapk" -> mapk, "recallAtK" -> recallAtK,
        "diversityAtK" -> diversityAtK, "maxDiversity" -> maxDiversity)
      metricsList += allMetrics
    }

    metrics.matchMetric($(metricName))
  }

  override def copy(extra: ParamMap): MsftRecommendationEvaluator = defaultCopy(extra)

}
