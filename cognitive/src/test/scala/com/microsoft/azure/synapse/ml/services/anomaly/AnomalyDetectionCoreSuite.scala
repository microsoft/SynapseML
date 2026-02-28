// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.anomaly

import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{
  ArrayType, DataType, DoubleType, IntegerType, StringType, StructField, StructType
}
import org.scalatest.funsuite.AnyFunSuite
import spray.json._
import spray.json.DefaultJsonProtocol._

private[anomaly] class TestableAnomalyDetector(uid: String) extends AnomalyDetectorBase(uid) {
  def setSeries(v: Seq[TimeSeriesPoint]): this.type = setScalarParam(series, v)

  def setSeriesCol(v: String): this.type = setVectorParam(series, v)

  def buildEntity(row: Row): StringEntity = prepareEntity(row).get.asInstanceOf[StringEntity]

  override def responseDataType: DataType = ADEntireResponse.schema

  override def urlPath: String = "/anomalydetector/v1.1/timeseries/entire/detect"
}

class AnomalyDetectionCoreSuite extends AnyFunSuite {

  import AnomalyDetectorProtocol._

  private val timeSeriesPointSchema = StructType(Seq(
    StructField("timestamp", StringType, nullable = false),
    StructField("value", DoubleType, nullable = false)
  ))

  private val requestSchema = StructType(Seq(
    StructField("seriesInput", ArrayType(timeSeriesPointSchema), nullable = false),
    StructField("granularityInput", StringType, nullable = false),
    StructField("maxRatioInput", DoubleType, nullable = true),
    StructField("sensitivityInput", IntegerType, nullable = true),
    StructField("customIntervalInput", IntegerType, nullable = true),
    StructField("periodInput", IntegerType, nullable = true),
    StructField("imputeModeInput", StringType, nullable = true),
    StructField("imputeFixedValueInput", DoubleType, nullable = true)
  ))

  private def parseEntity(entity: StringEntity): ADRequest =
    EntityUtils.toString(entity, "UTF-8").parseJson.convertTo[ADRequest]

  private def timeSeriesRow(timestamp: String, value: Double): Row =
    new GenericRowWithSchema(Array[Any](timestamp, value), timeSeriesPointSchema)

  test("ADEntireResponse explode returns aligned single responses") {
    val entire = ADEntireResponse(
      isAnomaly = Seq(false, true),
      isPositiveAnomaly = Seq(false, true),
      isNegativeAnomaly = Seq(false, false),
      period = 12,
      expectedValues = Seq(10.0, 99.0),
      upperMargins = Seq(11.0, 105.0),
      lowerMargins = Seq(9.0, 95.0),
      severity = Seq(0.1, 0.8)
    )

    assert(entire.explode == Seq(
      ADSingleResponse(false, false, false, 12, 10.0, 11.0, 9.0, 0.1),
      ADSingleResponse(true, true, false, 12, 99.0, 105.0, 95.0, 0.8)
    ))
  }

  test("AnomalyDetectorProtocol json format round-trips ADRequest") {
    val request = ADRequest(
      series = Seq(TimeSeriesPoint("2024-01-01T00:00:00Z", 1.5)),
      granularity = "daily",
      maxAnomalyRatio = Some(0.3),
      sensitivity = None,
      customInterval = Some(2),
      period = Some(7),
      imputeMode = Some("linear"),
      imputeFixedValue = None
    )

    val json = request.toJson.asJsObject
    assert(json.fields("series").convertTo[Seq[TimeSeriesPoint]] == request.series)
    assert(json.fields("granularity").convertTo[String] == "daily")
    assert(json.convertTo[ADRequest] == request)
  }

  test("prepareEntity builds deterministic payload from scalar params") {
    val detector = new TestableAnomalyDetector("scalar-ad")
      .setSeries(Seq(
        TimeSeriesPoint("2024-01-01T00:00:00Z", 10.0),
        TimeSeriesPoint("2024-01-02T00:00:00Z", 11.5)
      ))
      .setGranularity("hourly")
      .setMaxAnomalyRatio(0.2)
      .setSensitivity(88)
      .setCustomInterval(3)
      .setPeriod(24)
      .setImputeMode("auto")
      .setImputeFixedValue(5.0)

    val request = parseEntity(detector.buildEntity(Row.empty))
    assert(request.series.map(_.timestamp) == Seq("2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"))
    assert(request.granularity == "hourly")
    assert(request.maxAnomalyRatio.contains(0.2))
    assert(request.sensitivity.contains(88))
    assert(request.customInterval.contains(3))
    assert(request.period.contains(24))
    assert(request.imputeMode.contains("auto"))
    assert(request.imputeFixedValue.contains(5.0))
  }

  test("prepareEntity converts row-based series structs and optional params") {
    val detector = new TestableAnomalyDetector("row-ad")
      .setSeriesCol("seriesInput")
      .setGranularityCol("granularityInput")
      .setMaxAnomalyRatioCol("maxRatioInput")
      .setSensitivityCol("sensitivityInput")
      .setCustomIntervalCol("customIntervalInput")
      .setPeriodCol("periodInput")
      .setImputeModeCol("imputeModeInput")
      .setImputeFixedValueCol("imputeFixedValueInput")

    val requestRow = new GenericRowWithSchema(Array[Any](
      Seq(
        timeSeriesRow("2024-05-01T00:00:00Z", 20.0),
        timeSeriesRow("2024-05-02T00:00:00Z", 21.0)
      ),
      "daily",
      0.4,
      75,
      5,
      14,
      "fixed",
      2.5
    ), requestSchema)

    val request = parseEntity(detector.buildEntity(requestRow))
    assert(request.series == Seq(
      TimeSeriesPoint("2024-05-01T00:00:00Z", 20.0),
      TimeSeriesPoint("2024-05-02T00:00:00Z", 21.0)
    ))
    assert(request.granularity == "daily")
    assert(request.maxAnomalyRatio.contains(0.4))
    assert(request.sensitivity.contains(75))
    assert(request.customInterval.contains(5))
    assert(request.period.contains(14))
    assert(request.imputeMode.contains("fixed"))
    assert(request.imputeFixedValue.contains(2.5))
  }
}
