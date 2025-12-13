// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}

import scala.collection.mutable.ListBuffer

/**
  * Collects predictions.
  */
abstract class PredictionBuffer extends Serializable {

  protected val modelField = StructField(PredictionBuffer.ModelCol, BinaryType, nullable=true)

  def append(inputRow: Row, prediction: Object): Unit

  def result(model: Array[Byte]): Seq[Row]

  def schema: StructType
}

object PredictionBuffer {
  val ModelCol = "_vowpalwabbit_model"
}

class PredictionBufferDiscard extends PredictionBuffer {
  def append(inputRow: Row, prediction: Object): Unit  = { }

  def result(model: Array[Byte]): Seq[Row] = Seq(Row.fromTuple(Tuple1(model)))

  def schema: StructType = StructType(Seq(modelField))
}

class PredictionBufferKeep(predictionSchema: StructType,
                           predictionFunc: VowpalWabbitPrediction.VowpalWabbitPredictionToSeqFunc,
                           inputSchema: StructType,
                           predictionIdCol: String)
  extends PredictionBuffer {

  val predictionIdIdx = inputSchema.fieldIndex(predictionIdCol)

  val predictions = ListBuffer[Row]()

  // scalastyle:off null
  def append(inputRow: Row, prediction: Object): Unit = {
    predictions.append(Row.fromSeq(
      Seq(null, // model
          inputRow.get(predictionIdIdx)) ++
      predictionFunc(prediction)))
  }

  /**
    * @return First row has the model (and nothing else), remaining rows have predictions and no model
    */
  def result(model: Array[Byte]): Seq[Row] = {
    // Row w/ model and everything else empty
    (Row.fromSeq(model +: (0 to predictionSchema.length).map({ _ => null})) +:
      predictions).toSeq
  }
  // scalastyle:on null

  def schema: StructType = {
    val predictionIdField = inputSchema(predictionIdIdx)

    StructType(
      StructField(PredictionBuffer.ModelCol, BinaryType, nullable = true) +:
        // StructField("stats", Encoders.product[TrainingStats].schema, nullable=true),
        StructField(predictionIdField.name, predictionIdField.dataType, nullable = true) +:
        predictionSchema.fields)
  }
}
