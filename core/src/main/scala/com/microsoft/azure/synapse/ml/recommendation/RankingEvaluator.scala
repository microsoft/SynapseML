// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.recommendation

import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.recommendation.{HasRecommenderCols, RecEvaluatorParams}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable}
import org.apache.spark.mllib.evaluation.RankingMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}

class AdvancedRankingMetrics(predictionAndLabels: RDD[(Array[Any], Array[Any])],
  k: Int, nItems: Long)
  extends Serializable {

  lazy val uniqueItemsRecommended: Array[Any] = predictionAndLabels
    .map(row => row._1)
    .reduce((x, y) => x.toSet.union(y.toSet).toArray)

  lazy val metrics                         = new RankingMetrics[Any](predictionAndLabels)
  lazy val map: Double                     = metrics.meanAveragePrecision
  lazy val ndcg: Double                    = metrics.ndcgAt(k)
  lazy val precisionAtk: Double            = metrics.precisionAt(k)
  lazy val recallAtK: Double               = predictionAndLabels.map(r =>
    r._1.distinct.intersect(r._2.distinct).length
      .toDouble / r
      ._1.length.toDouble).mean()
  lazy val diversityAtK: Double            = {
    uniqueItemsRecommended.length.toDouble / nItems
  }
  lazy val maxDiversity: Double            = {
    val itemCount = predictionAndLabels
      .map(row => row._2)
      .reduce((x, y) => x.toSet.union(y.toSet).toArray)
      .union(uniqueItemsRecommended).toSet
      .size
    itemCount.toDouble / nItems
  }
  lazy val meanReciprocalRank: Double      = {
    predictionAndLabels.map { case (pred, lab) =>
      val labSet = lab.toSet

      if (labSet.nonEmpty) {
        var i = 0
        var reciprocalRank = 0.0
        while (i < pred.length && reciprocalRank == 0.0) {  //scalastyle:ignore while
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
    case "map"          => map
    case "ndcgAt"       => ndcg
    case "precisionAtk" => precisionAtk
    case "recallAtK"    => recallAtK
    case "diversityAtK" => diversityAtK
    case "maxDiversity" => maxDiversity
    case "mrr"          => meanReciprocalRank
    case "fcp"          => fractionConcordantPairs
  }

  def getAllMetrics: Map[String, Double] = {
    Map("map" -> map,
      "ndcgAt" -> ndcg,
      "precisionAtk" -> precisionAtk,
      "recallAtK" -> recallAtK,
      "diversityAtK" -> diversityAtK,
      "maxDiversity" -> maxDiversity,
      "mrr" -> meanReciprocalRank,
      "fcp" -> fractionConcordantPairs)
  }
}

class RankingEvaluator(override val uid: String)
  extends Evaluator with RecEvaluatorParams with HasRecommenderCols with ComplexParamsWritable with SynapseMLLogging {
  logClass(FeatureNames.Recommendation)

  def this() = this(Identifiable.randomUID("recEval"))

  val nItems: LongParam = new LongParam(this, "nItems", "number of items")

  def setNItems(value: Long): this.type = set(nItems, value)

  def getNItems: Long = $(nItems)

  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("ndcgAt", "map", "mapk", "recallAtK", "diversityAtK",
      "maxDiversity", "mrr", "fcp"))
    new Param(this, "metricName", "metric name in evaluation " +
      "(ndcgAt|map|precisionAtk|recallAtK|diversityAtK|maxDiversity|mrr|fcp)",
      allowedParams)
  }

  /** @group getParam */
  def getMetricName: String = $(metricName)

  /** @group setParam */
  def setMetricName(value: String): this.type = set(metricName, value)

  setDefault(nItems -> -1, metricName -> "ndcgAt", k -> 10)

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def getMetrics(dataset: Dataset[_]): AdvancedRankingMetrics = {
    val predictionAndLabels = dataset
      .select(getPredictionCol, getLabelCol)
      .rdd.map { case Row(prediction: Seq[Any], label: Seq[Any]) => (prediction.toArray, label.toArray) }
      .cache()

    new AdvancedRankingMetrics(predictionAndLabels, getK, getNItems)
  }

  def getMetricsMap(dataset: Dataset[_]): Map[String, Double] = {
    getMetrics(dataset).getAllMetrics
  }

  override def evaluate(dataset: Dataset[_]): Double = {
    getMetrics(dataset).matchMetric(getMetricName)
  }

  override def copy(extra: ParamMap): RankingEvaluator = defaultCopy(extra)

}

object RankingEvaluator extends ComplexParamsReadable[RankingEvaluator]
