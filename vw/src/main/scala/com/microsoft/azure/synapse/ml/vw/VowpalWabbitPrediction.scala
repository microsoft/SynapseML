// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructType
import org.vowpalwabbit.spark.VowpalWabbitNative
import org.vowpalwabbit.spark.prediction.ScalarPrediction
import vowpalWabbit.responses.{ActionProbs, ActionScores, DecisionScores, Multilabels, PDF, PDFValue}

import scala.reflect.runtime.universe.TypeTag

/**
  * Provide schemas and accessor functions for almost all VW prediction types.
  */
object VowpalWabbitPrediction {
  private def toStructType[T: TypeTag] = ExpressionEncoder[T]().resolveAndBind().schema

  private val SchemaMap = {
    import VowpalWabbitSchema.Predictions._

    Map(
      "prediction_type_t::scalars" -> toStructType[Scalars],
      "prediction_type_t::multiclass" -> toStructType[Multiclass],
      "prediction_type_t::prob" -> toStructType[Prob],
      "prediction_type_t::multilabels" -> toStructType[Multilabels],
      "prediction_type_t::scalar" -> toStructType[Scalar],
      "prediction_type_t::action_scores" -> toStructType[ActionScores],
      "prediction_type_t::action_probs" -> toStructType[ActionProbs],
      "prediction_type_t::decision_probs" -> toStructType[DecisionProbs],
      "prediction_type_t::action_pdf_value" -> toStructType[ActionPDFValue],
      "prediction_type_t::pdf" -> toStructType[PDF]
    )
  }

  def getSchema(vw: VowpalWabbitNative): StructType = SchemaMap(vw.getOutputPredictionType)

  type VowpalWabbitPredictionToSeqFunc = Object => Seq[Any]

  // TODO: the java wrapper would have to support them too
  // CASE(prediction_type_t::pdf)
  // CASE(prediction_type_t::multiclassprobs)
  // CASE(prediction_type_t::action_pdf_value)
  // CASE(prediction_type_t::active_multiclass)
  // CASE(prediction_type_t::nopred)

  val PredictionFunctions = Map(
    "prediction_type_t::scalar" -> predictScalar,
    "prediction_type_t::scalars" -> predictScalars,
    "prediction_type_t::action_scores"-> predictActionScores,
    "prediction_type_t::action_probs" -> predictActionProbs,
    "prediction_type_t::decision_probs" -> predictDecisionProbs,
    "prediction_type_t::multilabels" -> predictMultilabels,
    "prediction_type_t::multiclass" -> predictMulticlass,
    "prediction_type_t::prob" -> predictProb,
    "prediction_type_t::action_pdf_value" -> predictActionPdfValue,
    "prediction_type_t::pdf" -> predictPdf)

  def getPredictionFunc(vw: VowpalWabbitNative): VowpalWabbitPredictionToSeqFunc =
    PredictionFunctions(vw.getOutputPredictionType)

  private def predictScalar: VowpalWabbitPredictionToSeqFunc = obj => {
    val pred = obj.asInstanceOf[ScalarPrediction]
    Seq(pred.getValue, pred.getConfidence)
  }

  private def predictScalars: VowpalWabbitPredictionToSeqFunc = obj => Seq(obj.asInstanceOf[Array[Float]])

  private def predictActionScores: VowpalWabbitPredictionToSeqFunc = obj => {
    val pred = obj.asInstanceOf[ActionScores].getActionScores
    Seq(pred.map({ a_s => Row.fromTuple(a_s.getAction, a_s.getScore) }))
  }

  private def predictActionProbs: VowpalWabbitPredictionToSeqFunc = obj => Seq(obj.asInstanceOf[ActionProbs]
    .getActionProbs
    .map { a_s => Row.fromTuple((a_s.getAction, a_s.getProbability)) })

  private def predictDecisionProbs: VowpalWabbitPredictionToSeqFunc = obj => Seq(obj.asInstanceOf[DecisionScores]
    .getDecisionScores.map({ ds => {
    ds.getActionScores.map { a_s => Row.fromTuple((a_s.getAction, a_s.getScore)) }
  }}))

  private def predictMultilabels: VowpalWabbitPredictionToSeqFunc = obj => Seq(obj.asInstanceOf[Multilabels].getLabels)

  private def predictMulticlass: VowpalWabbitPredictionToSeqFunc = obj => Seq(obj.asInstanceOf[java.lang.Integer])

  private def predictProb: VowpalWabbitPredictionToSeqFunc = obj => Seq(obj.asInstanceOf[java.lang.Float])

  private def predictActionPdfValue: VowpalWabbitPredictionToSeqFunc = obj => {
    val pred = obj.asInstanceOf[PDFValue]
    Seq(pred.getAction, pred.getPDFValue)
  }

  private def predictPdf: VowpalWabbitPredictionToSeqFunc = obj => {
    Seq(obj.asInstanceOf[PDF]
      .getPDFSegments
      .map { s => Row.fromTuple((s.getLeft, s.getRight, s.getPDFValue)) })
  }
}
