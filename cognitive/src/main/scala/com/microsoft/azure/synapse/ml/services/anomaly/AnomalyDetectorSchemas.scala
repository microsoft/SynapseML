// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.anomaly

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

object TimeSeriesPoint extends SparkBindings[TimeSeriesPoint]

case class TimeSeriesPoint(timestamp: String, value: Double)

case class ADRequest(series: Seq[TimeSeriesPoint],
                     granularity: String,
                     maxAnomalyRatio: Option[Double],
                     sensitivity: Option[Int],
                     customInterval: Option[Int],
                     period: Option[Int],
                     imputeMode: Option[String],
                     imputeFixedValue: Option[Double])

object ADRequest extends SparkBindings[ADRequest]

case class ADLastResponse(isAnomaly: Boolean,
                          isPositiveAnomaly: Boolean,
                          isNegativeAnomaly: Boolean,
                          period: Int,
                          expectedValue: Double,
                          upperMargin: Double,
                          lowerMargin: Double,
                          suggestedWindow: Int,
                          severity: Double)

object ADLastResponse extends SparkBindings[ADLastResponse]

case class ADSingleResponse(isAnomaly: Boolean,
                            isPositiveAnomaly: Boolean,
                            isNegativeAnomaly: Boolean,
                            period: Int,
                            expectedValue: Double,
                            upperMargin: Double,
                            lowerMargin: Double,
                            severity: Double)

object ADSingleResponse extends SparkBindings[ADSingleResponse]

case class ADEntireResponse(isAnomaly: Seq[Boolean],
                            isPositiveAnomaly: Seq[Boolean],
                            isNegativeAnomaly: Seq[Boolean],
                            period: Int,
                            expectedValues: Seq[Double],
                            upperMargins: Seq[Double],
                            lowerMargins: Seq[Double],
                            severity: Seq[Double]) {

  def explode: Seq[ADSingleResponse] = {
    isAnomaly.indices.map { i =>
      ADSingleResponse(
        isAnomaly(i), isPositiveAnomaly(i), isNegativeAnomaly(i),
        period, expectedValues(i), upperMargins(i), lowerMargins(i), severity(i)
      )
    }
  }
}

object ADEntireResponse extends SparkBindings[ADEntireResponse]

object AnomalyDetectorProtocol {
  implicit val TspEnc: RootJsonFormat[TimeSeriesPoint] = jsonFormat2(TimeSeriesPoint.apply)
  implicit val AdreqEnc: RootJsonFormat[ADRequest] = jsonFormat8(ADRequest.apply)
}
