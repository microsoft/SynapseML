// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import com.microsoft.ml.spark.schema.SparkBindings
import spray.json.DefaultJsonProtocol._

case class TimeSeriesPoint(timestamp: String, value: Double)

case class ADRequest(series: Seq[TimeSeriesPoint],
                     granularity: String,
                     maxAnomalyRatio: Option[Double],
                     sensitivity: Option[Int],
                     customInterval: Option[Int],
                     period: Option[Int])

object ADRequest extends SparkBindings[ADRequest]

case class ADLastResponse(isAnomaly: Boolean,
                          isPositiveAnomaly: Boolean,
                          isNegativeAnomaly: Boolean,
                          period: Int,
                          expectedValue: Double,
                          upperMargin: Double,
                          lowerMargin: Double,
                          suggestedWindow: Int)

object ADLastResponse extends SparkBindings[ADLastResponse]

case class ADEntireResponse(isAnomaly: Seq[Boolean],
                            isPositiveAnomaly: Seq[Boolean],
                            isNegativeAnomaly: Seq[Boolean],
                            period: Int,
                            expectedValues: Seq[Double],
                            upperMargins: Seq[Double],
                            lowerMargins: Seq[Double])

object ADEntireResponse extends SparkBindings[ADEntireResponse]

object AnomalyDetectorProtocol {
  implicit val tspEnc = jsonFormat2(TimeSeriesPoint.apply)
  implicit val adreqEnc = jsonFormat6(ADRequest.apply)
}
