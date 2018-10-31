// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.recommendation.{HasRecommenderCols, RecEvaluatorParams}
import org.apache.spark.ml.util.{ComplexParamsReadable, ComplexParamsWritable, Identifiable}
import org.apache.spark.mllib.evaluation.RankingMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}

class RankingEvaluator(override val uid: String)
  extends Evaluator with RecEvaluatorParams with HasRecommenderCols with ComplexParamsWritable {

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
