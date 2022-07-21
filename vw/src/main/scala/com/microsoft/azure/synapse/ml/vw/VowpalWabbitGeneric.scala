// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.env.StreamUtilities
import com.microsoft.azure.synapse.ml.logging.BasicLogging

import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Estimator, Model}
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Row, functions => F}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.vowpalwabbit.spark.VowpalWabbitNative

trait HasInputCol extends Params {
  val inputCol = new Param[String](this, "inputCol", "The name of the input column")
  def setInputCol(value: String): this.type = set(inputCol, value)
  def getInputCol: String = $(inputCol)
}

/**
  * This Estimator supports driving VW using VW-style examples (e.g. 0 |a b c)
  */
class VowpalWabbitGeneric(override val uid: String) extends Estimator[VowpalWabbitGenericModel]
  with VowpalWabbitBaseLearner
  with HasInputCol
  with BasicLogging {
  logClass()

  override protected lazy val pyInternalWrapper = true

  def this() = this(Identifiable.randomUID("VowpalWabbitGeneric"))

  setDefault(inputCol -> "input")

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  protected def getInputColumns(): Seq[String] = Seq(getInputCol)

  override protected def trainFromRows(schema: StructType,
                                       inputRows: Iterator[Row],
                                       ctx: TrainContext): Unit = {

    val featureIdx = schema.fieldIndex(getInputCol)

    // learn from all rows
    for (row <- inputRows) {
      // ingestion and learning is collapsed
      ctx.learnTime.measure {
        ctx.vw.learnFromString(row.getString(featureIdx))
      }

      // trigger inter-pass all reduce
      0 until ctx.synchronizationSchedule.getAllReduceTriggerCount(row) foreach { _ => ctx.vw.endPass() }
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

class VowpalWabbitGenericProgressive(override val uid: String)
  extends VowpalWabbitBaseProgressive
    with HasInputCol
    with BasicLogging {
  logClass()
  override protected lazy val pyInternalWrapper = true

  def this() = this(Identifiable.randomUID("VowpalWabbitGenericProgressive"))

  setDefault(inputCol -> "input")

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override protected def getInputColumns(): Seq[String] = Seq(getInputCol)

  // wrap the block w/ a VW instance that will be closed when the block is done.
  private def executeWithVowpalWabbit[T](block: VowpalWabbitNative => T): T = {
    val localInitialModel = if (isDefined(initialModel)) Some(getInitialModel) else None

    val args = buildCommandLineArguments(getCommandLineArgs.result, "")

    val vw =
      if (localInitialModel.isEmpty) new VowpalWabbitNative(args)
      else new VowpalWabbitNative(args, localInitialModel.get)

    StreamUtilities.using(vw) { block(_) }.get
  }

  // it's a bit annoying that we have to start/stop VW to understand the schema
  lazy val (additionalOutputSchema, predictionFunc) = {
    executeWithVowpalWabbit { vw => {
      val schema = VowpalWabbitPrediction.getSchema(vw)
      val func = VowpalWabbitPrediction.getPredictionFunc(vw)

      (schema, func)
    } }
  }

  override def getAdditionalOutputSchema(): StructType = additionalOutputSchema

  var featureIdx: Int = 0

  override def trainFromRow(vw: VowpalWabbitNative, row: Row): Seq[Any] = {
    // fetch data
    val features = row.getString(featureIdx)

    // learn
    val pred = vw.learnFromString(features)

    // convert prediction to seq
    predictionFunc(pred)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    // ugh, would have to pass the context all the way through to trainFromRow
    featureIdx = dataset.schema.fieldIndex(getInputCol)

    super.transform(dataset)
  }
}

object VowpalWabbitGeneric extends ComplexParamsReadable[VowpalWabbitGeneric]

class VowpalWabbitGenericModel(override val uid: String)
  extends Model[VowpalWabbitGenericModel]
    with VowpalWabbitBaseModel
    with HasInputCol
    with ComplexParamsWritable with Wrappable with BasicLogging {
  logClass()

  override protected lazy val pyInternalWrapper = true

  def this() = this(Identifiable.randomUID("VowpalWabbitGenericModel"))

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  private def schemaForPredictionType(): StructType =
    StructType(StructField(getInputCol, StringType, false) +: VowpalWabbitPrediction.getSchema(vw).fields)

  override def transform(dataset: Dataset[_]): DataFrame = {
    // this is doing predict, but lightgbm also logs logTransform in the model...
    logTransform {
      val df = dataset.toDF()
      val inputColIdx = df.schema.fieldIndex(getInputCol)

      val predictToSeq = VowpalWabbitPrediction.getPredictionFunc(vw)

      val rowEncoder = RowEncoder(schemaForPredictionType())

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
