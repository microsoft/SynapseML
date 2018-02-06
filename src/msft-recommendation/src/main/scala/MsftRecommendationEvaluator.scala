// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.recommendation.MsftRecEvaluatorParams
import org.apache.spark.ml.util.{ComplexParamsReadable, Identifiable}
import org.apache.spark.mllib.evaluation.RankingMetrics
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable.ListBuffer

@InternalWrapper
final class MsftRecommendationEvaluator(override val uid: String)
  extends Evaluator with MsftRecEvaluatorParams {
  var nItems: LongParam = new LongParam(this, "nItems", "number of items")

  val metricsList = new ListBuffer[Map[String, Double]]()

  def printMetrics(): Unit = {
    metricsList.foreach(map => {
      print(map.toString())
    })
  }

  def getMetricsList: ListBuffer[Map[String, Double]] = metricsList

  def setNItems(value: Long): this.type = set(nItems, value)

  setDefault(nItems -> -1)

  def this() = this(Identifiable.randomUID("recEval"))

  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("ndcgAt", "map", "mapk", "recallAtK", "diversityAtK",
      "maxDiversity"))
    new Param(this, "metricName", "metric name in evaluation " +
      "(ndcgAt|map|precisionAtk|recallAtK|diversityAtK|maxDiversity)", allowedParams)
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
    predictionAndLabels.cache()
    val metrics = new RankingMetrics[Any](predictionAndLabels)

    class AdvancedRankingMetrics(rankingMetrics: RankingMetrics[Any]) {
      lazy val uniqueItemsRecommended = predictionAndLabels
        .map(row => row._1)
        .reduce((x, y) => x.toSet.union(y.toSet).toArray)

      lazy val map: Double = metrics.meanAveragePrecision
      lazy val ndcg: Double = metrics.ndcgAt($(k))
      lazy val mapk: Double = metrics.precisionAt($(k))
      lazy val recallAtK: Double = predictionAndLabels.map(r =>
        r._1.distinct.intersect(r._2.distinct).size.toDouble / r._1.size.toDouble).mean()
      lazy val diversityAtK: Double = {
        uniqueItemsRecommended.length.toDouble / $(nItems)
      }
      lazy val maxDiversity: Double = {
        val itemCount = predictionAndLabels
          .map(row => row._2)
          .reduce((x, y) => x.toSet.union(y.toSet).toArray)
          .union(uniqueItemsRecommended).toSet
          .size
        itemCount.toDouble / $(nItems)
      }

      def matchMetric(metricName: String): Double = metricName match {
        case "map" => metrics.map
        case "ndcgAt" => metrics.ndcg
        case "precisionAtk" => metrics.mapk
        case "recallAtK" => metrics.recallAtK
        case "diversityAtK" => metrics.diversityAtK
        case "maxDiversity" => metrics.maxDiversity
      }

      def getAllMetrics(): Map[String, Double] = {
        Map("map" -> map, "ndcgAt" -> ndcg, "precisionAtk" -> mapk, "recallAtK" -> recallAtK,
          "diversityAtK" -> diversityAtK, "maxDiversity" -> maxDiversity)
      }
    }

    import scala.language.implicitConversions
    implicit def aRankingMetricsToRankingMetrics(rankingMetrics: RankingMetrics[Any]): AdvancedRankingMetrics =
      new AdvancedRankingMetrics(rankingMetrics)

    if (getSaveAll) metricsList += metrics.getAllMetrics()

    metrics.matchMetric($(metricName))
  }

  override def copy(extra: ParamMap): MsftRecommendationEvaluator = defaultCopy(extra)

}
