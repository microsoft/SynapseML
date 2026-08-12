// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.anomaly

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import spray.json._

import scala.collection.JavaConverters._

class AnomalyDetectorSchemasSuite extends TestBase {

  import AnomalyDetectorProtocol._
  import MADJsonProtocol._

  test("AnomalyDetectorProtocol serializes and deserializes ADRequest with optionals") {
    val request = ADRequest(
      series = Seq(
        TimeSeriesPoint("2024-01-01T00:00:00Z", 1.0),
        TimeSeriesPoint("2024-01-01T01:00:00Z", 2.0)
      ),
      granularity = "hourly",
      maxAnomalyRatio = Some(0.2),
      sensitivity = Some(95),
      customInterval = Some(4),
      period = Some(8),
      imputeMode = Some("fixed"),
      imputeFixedValue = Some(1.5)
    )

    assert(request.toJson.convertTo[ADRequest] == request)
    assert(TimeSeriesPoint("2024-01-01T00:00:00Z", 1.0).toJson.convertTo[TimeSeriesPoint] ==
      TimeSeriesPoint("2024-01-01T00:00:00Z", 1.0))
  }

  test("AnomalyDetectorProtocol deserializes ADRequest optionals to None when omitted") {
    val json =
      """{"series":[{"timestamp":"2024-01-01T00:00:00Z","value":1.0}],"granularity":"daily"}"""
    val request = json.parseJson.convertTo[ADRequest]

    assert(request.series == Seq(TimeSeriesPoint("2024-01-01T00:00:00Z", 1.0)))
    assert(request.granularity == "daily")
    assert(request.maxAnomalyRatio.isEmpty)
    assert(request.sensitivity.isEmpty)
    assert(request.customInterval.isEmpty)
    assert(request.period.isEmpty)
    assert(request.imputeMode.isEmpty)
    assert(request.imputeFixedValue.isEmpty)
  }

  test("ADEntireResponse explode returns one ADSingleResponse per index") {
    val response = ADEntireResponse(
      isAnomaly = Seq(true, false),
      isPositiveAnomaly = Seq(true, false),
      isNegativeAnomaly = Seq(false, true),
      period = 12,
      expectedValues = Seq(1.1, 2.2),
      upperMargins = Seq(1.3, 2.4),
      lowerMargins = Seq(0.9, 2.0),
      severity = Seq(0.8, 0.1)
    )

    val exploded = response.explode
    assert(exploded == Seq(
      ADSingleResponse(true, true, false, 12, 1.1, 1.3, 0.9, 0.8),
      ADSingleResponse(false, false, true, 12, 2.2, 2.4, 2.0, 0.1)
    ))
  }

  test("MADJsonProtocol round trips key schema case classes") {
    val error = DMAError(Some("BadRequest"), Some("invalid input"))
    val variableState = DMAVariableState(
      Some("cpu"),
      Some(0.2),
      Some(30),
      Some("2024-01-01T00:00:00Z"),
      Some("2024-01-01T01:00:00Z")
    )
    val modelState = ModelState(
      Some(Seq(1, 2)),
      Some(Seq(1.0, 0.8)),
      Some(Seq(1.1, 0.9)),
      Some(Seq(0.4, 0.5))
    )
    val diagnosticsInfo = DiagnosticsInfo(Some(modelState), Some(Seq(variableState)))
    val alignPolicy = AlignPolicy(Some("Outer"), Some("Linear"), Some(0))
    val maeRequest = MAERequest(
      "https://storage/path.csv",
      "OneTable",
      "2024-01-01T00:00:00Z",
      "2024-01-02T00:00:00Z",
      Some(300),
      Some(alignPolicy),
      Some("demo-model")
    )
    val correlationChanges = CorrelationChanges(Some(Seq("memory")))
    val interpretation = Interpretation(Some("cpu"), Some(0.7), Some(correlationChanges))
    val dmaValue = DMAValue(Some(Seq(interpretation)), Some(true), Some(0.4), Some(0.9))
    val dmaResult = DMAResult("2024-01-01T00:01:00Z", Some(dmaValue), Some(Seq(error)))
    val setupInfo = DMASetupInfo(
      "https://storage/path.csv",
      Some(5),
      "2024-01-01T00:00:00Z",
      "2024-01-02T00:00:00Z"
    )
    val summary = DMASummary("Ready", Some(Seq(error)), Some(Seq(variableState)), setupInfo)
    val maeModelInfo = MAEModelInfo(
      Some(300),
      Some(alignPolicy),
      "https://storage/path.csv",
      "OneTable",
      "2024-01-01T00:00:00Z",
      "2024-01-02T00:00:00Z",
      Some("demo-model"),
      "Ready",
      Some(Seq(error)),
      Some(diagnosticsInfo)
    )
    val variable = Variable(
      Seq("2024-01-01T00:00:00Z", "2024-01-01T00:01:00Z"),
      Seq(1.0, 2.0),
      "cpu"
    )
    val dlmaRequest = DLMARequest(Seq(variable), 3)
    val dmaRequest = DMARequest(
      "https://storage/path.csv",
      "2024-01-01T00:00:00Z",
      "2024-01-02T00:00:00Z",
      Some(5)
    )

    assert(error.toJson.convertTo[DMAError] == error)
    assert(variableState.toJson.convertTo[DMAVariableState] == variableState)
    assert(modelState.toJson.convertTo[ModelState] == modelState)
    assert(diagnosticsInfo.toJson.convertTo[DiagnosticsInfo] == diagnosticsInfo)
    assert(alignPolicy.toJson.convertTo[AlignPolicy] == alignPolicy)
    assert(maeRequest.toJson.convertTo[MAERequest] == maeRequest)
    assert(correlationChanges.toJson.convertTo[CorrelationChanges] == correlationChanges)
    assert(interpretation.toJson.convertTo[Interpretation] == interpretation)
    assert(dmaValue.toJson.convertTo[DMAValue] == dmaValue)
    assert(dmaResult.toJson.convertTo[DMAResult] == dmaResult)
    assert(setupInfo.toJson.convertTo[DMASetupInfo] == setupInfo)
    assert(summary.toJson.convertTo[DMASummary] == summary)
    assert(maeModelInfo.toJson.convertTo[MAEModelInfo] == maeModelInfo)
    assert(variable.toJson.convertTo[Variable] == variable)
    assert(dlmaRequest.toJson.convertTo[DLMARequest] == dlmaRequest)
    assert(dmaRequest.toJson.convertTo[DMARequest] == dmaRequest)
  }

  test("MADJsonProtocol deserializes optional fields to None when absent") {
    val dmaRequestJson =
      """{"dataSource":"source","startTime":"2024-01-01T00:00:00Z","endTime":"2024-01-02T00:00:00Z"}"""
    val maeRequestJson =
      """{"dataSource":"source","dataSchema":"OneTable",""" +
        """"startTime":"2024-01-01T00:00:00Z",""" +
        """"endTime":"2024-01-02T00:00:00Z"}"""
    val dmaResultJson = """{"timestamp":"2024-01-01T00:00:00Z"}"""
    val dmaValueJson = """{}"""

    assert(dmaRequestJson.parseJson.convertTo[DMARequest].topContributorCount.isEmpty)
    val maeRequest = maeRequestJson.parseJson.convertTo[MAERequest]
    assert(maeRequest.slidingWindow.isEmpty)
    assert(maeRequest.alignPolicy.isEmpty)
    assert(maeRequest.displayName.isEmpty)
    val result = dmaResultJson.parseJson.convertTo[DMAResult]
    assert(result.value.isEmpty)
    assert(result.errors.isEmpty)
    val value = dmaValueJson.parseJson.convertTo[DMAValue]
    assert(value.interpretation.isEmpty)
    assert(value.isAnomaly.isEmpty)
    assert(value.severity.isEmpty)
    assert(value.score.isEmpty)
  }

  test("DMAVariableState and DiagnosticsInfo getters cover present and empty branches") {
    val variableState = DMAVariableState(
      Some("cpu"),
      Some(0.3),
      Some(11),
      Some("2024-01-01T00:00:00Z"),
      Some("2024-01-01T00:10:00Z")
    )
    assert(variableState.getVariable == "cpu")
    assert(variableState.getFilledNARatio == 0.3)
    assert(variableState.getEffectiveCount == 11)
    assert(variableState.getFirstTimestamp == "2024-01-01T00:00:00Z")
    assert(variableState.getLastTimestamp == "2024-01-01T00:10:00Z")

    val modelState = ModelState(None, None, None, None)
    assert(modelState.getEpochIds.asScala.isEmpty)
    assert(modelState.getTrainLosses.asScala.isEmpty)
    assert(modelState.getValidationLosses.asScala.isEmpty)
    assert(modelState.getLatenciesInSeconds.asScala.isEmpty)

    val diagnosticsInfo = DiagnosticsInfo(Some(modelState), Some(Seq(variableState)))
    assert(diagnosticsInfo.getModelState == modelState)
    assert(diagnosticsInfo.getVariableStates.asScala.toSeq == Seq(variableState))

    val emptyVariableState = DMAVariableState(None, None, None, None, None)
    val emptyDiagnosticsInfo = DiagnosticsInfo(None, None)
    intercept[NoSuchElementException](emptyVariableState.getVariable)
    intercept[NoSuchElementException](emptyVariableState.getFilledNARatio)
    intercept[NoSuchElementException](emptyVariableState.getEffectiveCount)
    intercept[NoSuchElementException](emptyVariableState.getFirstTimestamp)
    intercept[NoSuchElementException](emptyVariableState.getLastTimestamp)
    intercept[NoSuchElementException](emptyDiagnosticsInfo.getModelState)
    intercept[NoSuchElementException](emptyDiagnosticsInfo.getVariableStates)
  }

}
