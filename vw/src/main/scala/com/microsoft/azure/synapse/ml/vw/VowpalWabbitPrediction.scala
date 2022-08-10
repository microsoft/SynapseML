// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, StructField, StructType}
import org.vowpalwabbit.spark.VowpalWabbitNative
import org.vowpalwabbit.spark.prediction.ScalarPrediction
import vowpalWabbit.responses.{ActionProbs, ActionScores, DecisionScores, Multilabels, PDF, PDFValue}

/**
  * Provide schemas and accessor functions for almost all VW prediction types.
  */
object VowpalWabbitPrediction {
  val SchemaMap = Map(
    "prediction_type_t::scalars" -> Seq(StructField("predictions", ArrayType(FloatType), nullable = false)),
    "prediction_type_t::multiclass" -> Seq(StructField("prediction", IntegerType, nullable = false)),
    "prediction_type_t::prob" -> Seq(StructField("prediction", FloatType, nullable = false)),
    "prediction_type_t::multilabels" -> Seq(StructField("prediction", ArrayType(IntegerType), nullable = false)),
    "prediction_type_t::scalar" -> Seq(
      StructField("prediction", FloatType, nullable = false),
      StructField("confidence", FloatType, nullable = false)),
    "prediction_type_t::action_scores" -> Seq(
      StructField("predictions", ArrayType(StructType(Seq(
        StructField("action", IntegerType, nullable = false),
        StructField("score", FloatType, nullable = false)
      ))))
    ),
    "prediction_type_t::action_probs" -> Seq(
      StructField("predictions", ArrayType(StructType(Seq(
        StructField("action", IntegerType, nullable = false),
        StructField("probability", FloatType, nullable = false)
      ))))
    ),
    "prediction_type_t::decision_probs" -> Seq(
      StructField("predictions",
        ArrayType(
          ArrayType(
            StructType(Seq(
              StructField("action", IntegerType, nullable = false),
              StructField("score", FloatType, nullable = false)
            )))))),
    "prediction_type_t::action_pdf_value" -> Seq(
      StructField("action", FloatType, nullable = false),
      StructField("pdf", FloatType, nullable = false)
    ),
    "prediction_type_t::pdf" -> Seq(
      StructField("segments", ArrayType(
        StructType(Seq(
          StructField("left", FloatType, nullable = false),
          StructField("right", FloatType, nullable = false),
          StructField("pdfValue", FloatType, nullable = false)
        )))))
    )

  def getSchema(vw: VowpalWabbitNative): StructType = StructType(SchemaMap(vw.getOutputPredictionType))

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
