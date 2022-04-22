// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

// DMA stands for DetectMultivariateAnomaly
object DMARequest extends SparkBindings[DMARequest]

case class DMARequest(source: String,
                      startTime: String,
                      endTime: String)

object DMAResponse extends SparkBindings[DMAResponse]

case class DMAResponse(resultId: String,
                       summary: DMASummary,
                       results: Seq[DMAResult])

case class DMASummary(status: String,
                      errors: Option[Seq[DMAError]],
                      variableStates: Option[Seq[DMAVariableState]],
                      setupInfo: DMASetupInfo)

object DMAError extends SparkBindings[DMAError]

case class DMAError(code: String, message: String)

case class DMAVariableState(variable: Option[String],
                            filledNARatio: Option[Double],
                            effectiveCount: Option[Int],
                            startTime: Option[String],
                            endTime: Option[String],
                            errors: Option[Seq[DMAError]])

case class DMASetupInfo(source: String,
                        startTime: String,
                        endTime: String)

case class DMAResult(timestamp: String, value: Option[DMAValue], errors: Option[Seq[DMAError]])

case class DMAValue(contributors: Option[Seq[DMAContributor]],
                    isAnomaly: Boolean,
                    severity: Double,
                    score: Double)

case class DMAContributor(contributionScore: Option[Double], variable: Option[String])

// MAE stands for MultivariateAnomalyEstimator
object MAERequest extends SparkBindings[MAERequest]

case class MAERequest(source: String,
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
                        source: String,
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

object MADJsonProtocol extends DefaultJsonProtocol {
  implicit val DMAReqEnc: RootJsonFormat[DMARequest] = jsonFormat3(DMARequest.apply)
  implicit val EEnc: RootJsonFormat[DMAError] = jsonFormat2(DMAError.apply)
  implicit val VSEnc: RootJsonFormat[DMAVariableState] = jsonFormat6(DMAVariableState.apply)
  implicit val MSEnc: RootJsonFormat[ModelState] = jsonFormat4(ModelState.apply)
  implicit val DIEnc: RootJsonFormat[DiagnosticsInfo] = jsonFormat2(DiagnosticsInfo.apply)
  implicit val APEnc: RootJsonFormat[AlignPolicy] = jsonFormat3(AlignPolicy.apply)
  implicit val MAEReqEnc: RootJsonFormat[MAERequest] = jsonFormat6(MAERequest.apply)
  implicit val DMAContributorEnc: RootJsonFormat[DMAContributor] = jsonFormat2(DMAContributor.apply)
  implicit val DMAValueEnc: RootJsonFormat[DMAValue] = jsonFormat4(DMAValue.apply)
  implicit val DMAResEnc: RootJsonFormat[DMAResult] = jsonFormat3(DMAResult.apply)
  implicit val DMASetupInfoEnc: RootJsonFormat[DMASetupInfo] = jsonFormat3(DMASetupInfo.apply)
  implicit val DMASummaryEnc: RootJsonFormat[DMASummary] = jsonFormat4(DMASummary.apply)
  implicit val MAEModelInfoEnc: RootJsonFormat[MAEModelInfo] = jsonFormat9(MAEModelInfo.apply)
}
