// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.contracts

// Case class matching
sealed abstract class Metric

// Just for clarity in the contract file
object ConvenienceTypes {
    type UniqueName = String
    type MetricTable = Map[UniqueName, Seq[Metric]]
}
import com.microsoft.azure.synapse.ml.core.contracts.ConvenienceTypes._

// One option
case class TypedMetric[T](name: UniqueName, value: T) extends Metric

// Other option (reflection friendly - do we need reflection?)
sealed abstract class TypenameMetric
case class DoubleMetric(name: UniqueName, value: Double) extends TypenameMetric
case class StringMetric(name: UniqueName, value: String) extends TypenameMetric
case class IntegralMetric(name: UniqueName, value: Long) extends TypenameMetric

case class TypenameMetricGroup(name: UniqueName, values: Map[UniqueName, Seq[TypenameMetric]])

/** Defines contract for Metric table, which is a metric name to list of values.
  * @param data
  */
case class MetricData(data: Map[String, Seq[Double]], metricType: String, modelName: String)

object MetricData {
  def create(data: Map[String, Double], metricType: String, modelName: String): MetricData = {
    new MetricData(data.map(kvp => (kvp._1, List(kvp._2))), metricType, modelName)
  }

  def createTable(data: Map[String, Seq[Double]], metricType: String, modelName: String): MetricData = {
    new MetricData(data, metricType, modelName)
  }
}
