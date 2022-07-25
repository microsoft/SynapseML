// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, StructField, StructType}
import org.vowpalwabbit.spark.VowpalWabbitNative
import org.vowpalwabbit.spark.prediction.ScalarPrediction
import vowpalWabbit.responses.{ActionProbs, ActionScores, DecisionScores, Multilabels, PDF, PDFValue}

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

  def getPredictionFunc(vw: VowpalWabbitNative): VowpalWabbitPredictionToSeqFunc = {
//    println(s"Pred type: ${vw.getOutputPredictionType}")

    vw.getOutputPredictionType match {
      case "prediction_type_t::scalar" =>
        obj => {
          val pred = obj.asInstanceOf[ScalarPrediction]
          Seq(pred.getValue, pred.getConfidence)
        }
      case "prediction_type_t::scalars" =>
        obj => Seq(obj.asInstanceOf[Array[Float]])
      case "prediction_type_t::action_scores" =>
        obj => {
          val pred = obj.asInstanceOf[ActionScores].getActionScores
          Seq(pred.map({ a_s => Row.fromTuple(a_s.getAction, a_s.getScore) }))
        }
      case "prediction_type_t::action_probs" =>
        obj => Seq(obj.asInstanceOf[ActionProbs]
          .getActionProbs
          .map { a_s => Row.fromTuple((a_s.getAction, a_s.getProbability)) })
      case "prediction_type_t::decision_probs" =>
        obj => Seq(obj.asInstanceOf[DecisionScores]
          .getDecisionScores.map({ ds => {
            ds.getActionScores.map { a_s => Row.fromTuple((a_s.getAction, a_s.getScore)) }
          }}))
      case "prediction_type_t::multilabels" =>
        obj => Seq(obj.asInstanceOf[Multilabels].getLabels)
      case "prediction_type_t::multiclass" =>
        obj => Seq(obj.asInstanceOf[java.lang.Integer])
      case "prediction_type_t::prob" =>
        obj => Seq(obj.asInstanceOf[java.lang.Float])
      case "prediction_type_t::action_pdf_value" =>
        obj => {
          val pred = obj.asInstanceOf[PDFValue]
          Seq(pred.getAction, pred.getPDFValue)
        }
      case "prediction_type_t::pdf" =>
        obj => {
          Seq(obj.asInstanceOf[PDF]
            .getPDFSegments
            .map { s => Row.fromTuple((s.getLeft, s.getRight, s.getPDFValue)) })
        }
      case x => throw new NotImplementedError(s"Prediction type '$x' not supported")

      // TODO: the java wrapper would have to support them too
      // CASE(prediction_type_t::pdf)
      // CASE(prediction_type_t::multiclassprobs)
      // CASE(prediction_type_t::action_pdf_value)
      // CASE(prediction_type_t::active_multiclass)
      // CASE(prediction_type_t::nopred)
    }
  }
}

