// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.HasInputCol
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Estimator, Model}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions => F}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer

/**
  * This Estimator supports driving VW using VW-style examples (e.g. 0 |a b c)
  */
class VowpalWabbitGeneric(override val uid: String)
  extends Estimator[VowpalWabbitGenericModel]
    with VowpalWabbitBaseLearner
    with HasInputCol
    with SynapseMLLogging {
  logClass()

  override protected lazy val pyInternalWrapper = true

  def this() = this(Identifiable.randomUID("VowpalWabbitGeneric"))

  setDefault(inputCol -> "value")

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  protected def getInputColumns: Seq[String] = Seq(getInputCol)

  override protected def trainFromRows(schema: StructType,
                                       inputRows: Iterator[Row],
                                       ctx: TrainContext): Unit = {

    val featureIdx = schema.fieldIndex(getInputCol)

    // learn from all rows
    for (row <- inputRows) {
      // ingestion and learning is collapsed
      ctx.learnTime.measure {
        // learn + convert prediction
        val pred = ctx.vw.learnFromString(row.getString(featureIdx))

        // collect all predictions
        ctx.predictionBuffer.append(row, pred)
      }

      // trigger inter-pass all reduce
      if (ctx.synchronizationSchedule.shouldTriggerAllReduce(row))
        ctx.vw.endPass()
    }
  }

  def fit(dataset: org.apache.spark.sql.Dataset[_]): VowpalWabbitGenericModel = {
    // TODO: copied from predictor. should we merge train in here?
    copyValues(train(dataset)
      .setParent(this))
  }

  def transformSchema(schema: StructType): StructType = {
    // validate the input column is here
    // TODO: is this the right way of doing it? (as in expecting an exception?)
    schema.fieldIndex(getInputCol)

    schema
  }

  protected def train(dataset: Dataset[_]): VowpalWabbitGenericModel = {
    logTrain({
      val model = new VowpalWabbitGenericModel(uid)
        .setInputCol(getInputCol)

      trainInternal(dataset.toDF, model)
    })
  }
}

object VowpalWabbitGeneric extends ComplexParamsReadable[VowpalWabbitGeneric]

class VowpalWabbitGenericModel(override val uid: String)
  extends Model[VowpalWabbitGenericModel]
    with VowpalWabbitBaseModel
    with HasInputCol
    with ComplexParamsWritable with Wrappable with SynapseMLLogging {
  logClass()

  override protected lazy val pyInternalWrapper = true

  def this() = this(Identifiable.randomUID("VowpalWabbitGenericModel"))

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  private def schemaForPredictionType: StructType =
    StructType(StructField(getInputCol, StringType, nullable = false) +: VowpalWabbitPrediction.getSchema(vw).fields)

  override def transform(dataset: Dataset[_]): DataFrame = {
    // this is doing predict, but lightgbm also logs logTransform in the model...
    logTransform {
      val df = dataset.toDF()
      val inputColIdx = df.schema.fieldIndex(getInputCol)

      val predictToSeq = VowpalWabbitPrediction.getPredictionFunc(vw)
      val rowEncoder = RowEncoder(schemaForPredictionType)

      df.mapPartitions(inputRows => {
        inputRows.map { row => {
          val input = row.getString(inputColIdx)
          val prediction = vw.predictFromString(input)

          Row.fromSeq(row.toSeq ++ predictToSeq(prediction))
        }}
      })(rowEncoder)
        .toDF()
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    // validate the inputCol is present
    // TODO: is this the right way of doing it?
    schema.fieldIndex(getInputCol)

    StructType(schemaForPredictionType)
  }
}

object VowpalWabbitGenericModel extends ComplexParamsReadable[VowpalWabbitGenericModel]
