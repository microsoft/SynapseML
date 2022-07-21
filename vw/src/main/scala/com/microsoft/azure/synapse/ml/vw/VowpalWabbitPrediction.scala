// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, StringType, StructField, StructType}
import org.vowpalwabbit.spark.VowpalWabbitNative
import org.vowpalwabbit.spark.prediction.ScalarPrediction
import vowpalWabbit.responses.{ActionProbs, ActionScore, ActionScores, DecisionScores, Multilabels, PDF, PDFValue}

object VowpalWabbitPrediction {
  def getSchema(vw: VowpalWabbitNative): StructType = {
    val fields = vw.getOutputPredictionType match {
      case "prediction_type_t::scalars" => Seq(StructField("predictions", ArrayType(FloatType), false))
      case "prediction_type_t::multiclass" => Seq(StructField("prediction", IntegerType, false))
      case "prediction_type_t::prob" => Seq(StructField("prediction", FloatType, false))
      case "prediction_type_t::multilabels" => Seq(StructField("prediction", ArrayType(IntegerType), false))
      case "prediction_type_t::scalar" => Seq(
        StructField("prediction", FloatType, false),
        StructField("confidence", FloatType, false))
      case "prediction_type_t::action_scores" => Seq(
        StructField("predictions", ArrayType(StructType(Seq(
          StructField("action", IntegerType, false),
          StructField("score", FloatType, false)
        ))))
      )
      case "prediction_type_t::action_probs" => Seq(
        StructField("predictions", ArrayType(StructType(Seq(
          StructField("action", IntegerType, false),
          StructField("probability", FloatType, false)
        ))))
      )
      case "prediction_type_t::decision_probs" => Seq(
        StructField("predictions",
          ArrayType(
            ArrayType(
              StructType(Seq(
                StructField("action", IntegerType, false),
                StructField("score", FloatType, false)
              ))))))
      case "prediction_type_t::action_pdf_value" => Seq(
        StructField("action", FloatType, false),
        StructField("pdf", FloatType, false)
      )
      case "prediction_type_t::pdf" => Seq(
        StructField("segments", ArrayType(
          StructType(Seq(
            StructField("left", FloatType, false),
            StructField("right", FloatType, false),
            StructField("pdfValue", FloatType, false)
          )))))

      case x => throw new NotImplementedError(s"Prediction type '${x}' not supported")
      // TODO: the java wrapper would have to support them too
      // CASE(prediction_type_t::multiclassprobs)
      // CASE(prediction_type_t::active_multiclass)
      // CASE(prediction_type_t::nopred)
    }

    // always return the the input column
    StructType(fields)
  }

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
          pred.map({ a_s => Row.fromTuple(a_s.getAction, a_s.getScore) })
        }
      case "prediction_type_t::action_probs" =>
        obj => obj.asInstanceOf[ActionProbs]
          .getActionProbs
          .map { a_s => Row.fromTuple((a_s.getAction, a_s.getProbability)) }
      case "prediction_type_t::decision_probs" =>
        obj => obj.asInstanceOf[DecisionScores]
          .getDecisionScores.map({ ds => {
            ds.getActionScores.map { a_s => Row.fromTuple((a_s.getAction, a_s.getScore)) }
          }})
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
        obj => Seq(obj.asInstanceOf[PDF]
          .getPDFSegments
          .map { s => Row.fromTuple((s.getLeft, s.getRight, s.getPDFValue)) })
      case x => throw new NotImplementedError(s"Prediction type '${x}' not supported")

      // TODO: the java wrapper would have to support them too
      // CASE(prediction_type_t::pdf)
      // CASE(prediction_type_t::multiclassprobs)
      // CASE(prediction_type_t::action_pdf_value)
      // CASE(prediction_type_t::active_multiclass)
      // CASE(prediction_type_t::nopred)
    }
  }
}

