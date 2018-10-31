// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.mllib.evaluation.RankingMetrics
import org.apache.spark.rdd.RDD

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
