// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.anomaly

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

// DMA stands for DetectMultivariateAnomaly
object DMARequest extends SparkBindings[DMARequest]

case class DMARequest(dataSource: String,
                      startTime: String,
                      endTime: String,
                      topContributorCount: Option[Int])

object DMAResponse extends SparkBindings[DMAResponse]

case class DMAResponse(resultId: String,
                       summary: DMASummary,
                       results: Option[Seq[DMAResult]])

case class DMASummary(status: String,
                      errors: Option[Seq[DMAError]],
                      variableStates: Option[Seq[DMAVariableState]],
                      setupInfo: DMASetupInfo)

object DMAError extends SparkBindings[DMAError]

case class DMAError(code: String, message: String)

case class DMAVariableState(variable: Option[String],
                            filledNARatio: Option[Double],
                            effectiveCount: Option[Int],
                            firstTimestamp: Option[String],
                            lastTimestamp: Option[String])

case class DMASetupInfo(dataSource: String,
                        topContributorCount: Option[Int],
                        startTime: String,
                        endTime: String)

case class DMAResult(timestamp: String, value: Option[DMAValue], errors: Option[Seq[DMAError]])

case class DMAValue(interpretation: Option[Seq[Interpretation]],
                    isAnomaly: Option[Boolean],
                    severity: Option[Double],
                    score: Option[Double])

case class Interpretation(variable: Option[String],
                          contributionScore: Option[Double],
                          correlationChanges: Option[CorrelationChanges])

case class CorrelationChanges(changedVariables: Option[Seq[String]])

// MAE stands for MultivariateAnomalyEstimator
object MAERequest extends SparkBindings[MAERequest]

case class MAERequest(dataSource: String,
                      dataSchema: String,
                      startTime: String,
                      endTime: String,
                      slidingWindow: Option[Int],
                      alignPolicy: Option[AlignPolicy],
                      displayName: Option[String])

object MAEResponse extends SparkBindings[MAEResponse]

case class MAEResponse(modelId: String,
                       createdTime: String,
                       lastUpdatedTime: String,
                       modelInfo: MAEModelInfo)

case class MAEModelInfo(slidingWindow: Option[Int],
                        alignPolicy: Option[AlignPolicy],
                        dataSource: String,
                        dataSchema: String,
                        startTime: String,
                        endTime: String,
                        displayName: Option[String],
                        status: String,
                        errors: Option[Seq[DMAError]],
                        diagnosticsInfo: Option[DiagnosticsInfo])

case class AlignPolicy(alignMode: Option[String], fillNAMethod: Option[String], paddingValue: Option[Int])

case class DiagnosticsInfo(modelState: Option[ModelState], variableStates: Option[Seq[DMAVariableState]])

case class ModelState(epochIds: Option[Seq[Int]],
                      trainLosses: Option[Seq[Double]],
                      validationLosses: Option[Seq[Double]],
                      latenciesInSeconds: Option[Seq[Double]])

object DLMARequest extends SparkBindings[DLMARequest]

case class DLMARequest(variables: Seq[Variable], topContributorCount: Int)

object Variable extends SparkBindings[Variable]

case class Variable(timestamps: Seq[String], values: Seq[Double], variable: String)

object DLMAResponse extends SparkBindings[DLMAResponse]

case class DLMAResponse(variableStates: Option[Seq[DMAVariableState]], results: Option[Seq[DMAResult]])

object MADJsonProtocol extends DefaultJsonProtocol {
  implicit val DMAReqEnc: RootJsonFormat[DMARequest] = jsonFormat4(DMARequest.apply)
  implicit val EEnc: RootJsonFormat[DMAError] = jsonFormat2(DMAError.apply)
  implicit val VSEnc: RootJsonFormat[DMAVariableState] = jsonFormat5(DMAVariableState.apply)
  implicit val MSEnc: RootJsonFormat[ModelState] = jsonFormat4(ModelState.apply)
  implicit val DIEnc: RootJsonFormat[DiagnosticsInfo] = jsonFormat2(DiagnosticsInfo.apply)
  implicit val APEnc: RootJsonFormat[AlignPolicy] = jsonFormat3(AlignPolicy.apply)
  implicit val MAEReqEnc: RootJsonFormat[MAERequest] = jsonFormat7(MAERequest.apply)
  implicit val CorrelationChangesEnc: RootJsonFormat[CorrelationChanges] = jsonFormat1(CorrelationChanges.apply)
  implicit val InterpretationEnc: RootJsonFormat[Interpretation] = jsonFormat3(Interpretation.apply)
  implicit val DMAValueEnc: RootJsonFormat[DMAValue] = jsonFormat4(DMAValue.apply)
  implicit val DMAResEnc: RootJsonFormat[DMAResult] = jsonFormat3(DMAResult.apply)
  implicit val DMASetupInfoEnc: RootJsonFormat[DMASetupInfo] = jsonFormat4(DMASetupInfo.apply)
  implicit val DMASummaryEnc: RootJsonFormat[DMASummary] = jsonFormat4(DMASummary.apply)
  implicit val MAEModelInfoEnc: RootJsonFormat[MAEModelInfo] = jsonFormat10(MAEModelInfo.apply)
  implicit val VariableEnc: RootJsonFormat[Variable] = jsonFormat3(Variable.apply)
  implicit val DLMARequestEnc: RootJsonFormat[DLMARequest] = jsonFormat2(DLMARequest.apply)
}
