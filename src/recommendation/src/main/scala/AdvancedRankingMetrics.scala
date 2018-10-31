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
  lazy val battleAverage: Double           = {
    //order of hits does not matter for BA
    predictionAndLabels.map { case (pred, lab) =>
      var nc = 0.0
      var nd = 0.0
      pred.zipWithIndex.foreach(a => {
        if (lab.length > a._2) {
          if(lab.contains(a._1)) nc += 1
          else nd += 1
        }
      })
      nc / (nc + nd)
    }.mean()
  }
  lazy val slugging: Double                = {
    //slugging is the power behind the hit
    predictionAndLabels.map { case (pred, lab) =>
      var nc = 0.0
      var nd = 0.0
      pred.zipWithIndex.foreach(a => {
        if (lab.length > a._2) {
          if(lab.contains(a._1)) nc += 1/lab.indexOf(a._1)
          nd += 1
        }
      })
      nc / (nd)
    }.mean()
  }
  lazy val ops: Double                     = battleAverage + slugging
  lazy val runs: Double                    = {
    //a run is a hit in the right order
    predictionAndLabels.map { case (pred, lab) =>
      var nc = 0.0
      var nd = 0.0
      pred.zipWithIndex.foreach(a => {
        if (lab.length > a._2) {
          if (a._1 == lab(a._2)) nc += 1
        }
      })
      nc
    }.sum()
  }
  lazy val hr: Double                      = {
    //a home run is when the top label item appears in the predictions
    predictionAndLabels.map { case (pred, lab) =>
      var nc = 0.0
      var nd = 0.0
      pred.zipWithIndex.foreach(a => {
        if (lab.length > a._2) {
          if(lab(0) == a._1) nc += 1
        }
      })
      nc
    }.sum()
  }
  lazy val so: Double                      = {
    //strike out is when there is no union between pred and lab
    predictionAndLabels.map { case (pred, lab) =>
      var nc = 0.0
      var nd = 0.0
      pred.zipWithIndex.foreach(a => {
        if (lab.length > a._2) {
          if(lab.contains(a._1)) nc += 1
        }
      })
      if (nc == 0) 1 else 0
    }.sum()
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
    case "battleAverage" => battleAverage
    case "slugging"      => slugging
    case "ops"           => ops
    case "runs"          => runs
    case "hr"            => hr
    case "so"            => so
  }

  def getAllMetrics: Map[String, Double] = {
    Map("map" -> map,
      "ndcgAt" -> ndcg,
      "precisionAtk" -> precisionAtk,
      "recallAtK" -> recallAtK,
      "diversityAtK" -> diversityAtK,
      "maxDiversity" -> maxDiversity,
      "mrr" -> meanReciprocalRank,
      "fcp" -> fractionConcordantPairs,
      "battleAverage" -> battleAverage,
      "slugging" -> slugging,
      "ops" -> ops,
      "runs" -> runs,
      "hr" -> hr,
      "so" -> so
    )
  }
}
