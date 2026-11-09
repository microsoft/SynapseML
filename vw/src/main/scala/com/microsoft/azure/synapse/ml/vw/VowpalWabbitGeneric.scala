// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.HasInputCol
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Estimator, Model}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions => F}
import org.apache.spark.sql.types.{StringType, StructType}

/**
  * This Estimator supports driving VW using VW-style examples (e.g. 0 |a b c)
  */
class VowpalWabbitGeneric(override val uid: String)
  extends Estimator[VowpalWabbitGenericModel]
    with VowpalWabbitBaseLearner
    with HasInputCol
    with SynapseMLLogging {
  logClass(FeatureNames.VowpalWabbit)

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
    logFit({
      val model = new VowpalWabbitGenericModel(uid)
        .setInputCol(getInputCol)

      trainInternal(dataset.toDF, model)
    }, dataset.columns.length)
  }
}

object VowpalWabbitGeneric extends ComplexParamsReadable[VowpalWabbitGeneric]

/**
  * Predicts from string examples while preserving every top-level input field in its original order,
  * including its type, nullability and metadata. Existing flat prediction fields are appended.
  * Input names that conflict with prediction names must be renamed before transforming.
  * Name comparisons follow Spark's case-sensitivity setting; the default is case-insensitive.
  */
class VowpalWabbitGenericModel(override val uid: String)
  extends Model[VowpalWabbitGenericModel]
    with VowpalWabbitBaseModel
    with HasInputCol
    with ComplexParamsWritable with Wrappable with SynapseMLLogging {
  logClass(FeatureNames.VowpalWabbit)

  override protected lazy val pyInternalWrapper = true

  def this() = this(Identifiable.randomUID("VowpalWabbitGenericModel"))

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  // Preserve top-level input fields with their types, metadata and nullability.
  // Prediction fields stay flat; callers must rename any conflicting input columns.
  private def schemaForPredictionType(schema: StructType, caseSensitive: Boolean): StructType = {
    require(schema(getInputCol).dataType == StringType,
      s"Input column '$getInputCol' must have StringType.")
    val predictionFields = VowpalWabbitPrediction.getSchema(vw).fields
    val conflicts = schema.fieldNames.filter { inputName =>
      predictionFields.exists { field =>
        if (caseSensitive) inputName == field.name else inputName.equalsIgnoreCase(field.name)
      }
    }
    require(conflicts.isEmpty,
      s"Input columns conflict with prediction fields: ${conflicts.mkString(", ")}. Rename them before transforming.")
    StructType(schema.fields ++ predictionFields)
  }

  /**
    * Keeps all input fields with their type, nullability and metadata at every nesting level.
    * Appends flat predictions.
    * The input example column must be a string. Conflicting names are rejected using
    * the dataset session's spark.sql.caseSensitive setting, including inputCol itself.
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    // this is doing predict, but lightgbm also logs logTransform in the model...
    logTransform ({
      val df = dataset.toDF()
      val inputColIdx = df.schema.fieldIndex(getInputCol)

      val predictToSeq = VowpalWabbitPrediction.getPredictionFunc(vw)
      val caseSensitive = df.sparkSession.conf.get("spark.sql.caseSensitive", "false").trim.toBoolean
      val outputSchema = schemaForPredictionType(df.schema, caseSensitive)
      val rowEncoder = ExpressionEncoder(outputSchema)

      val encoded = df.mapPartitions(inputRows => {
        inputRows.map { row => {
          val input = row.getString(inputColIdx)
          val prediction = vw.predictFromString(input)

          Row.fromSeq(row.toSeq ++ predictToSeq(prediction))
        }}
      })(rowEncoder)
        .toDF()

      val internalColumns = outputSchema.fields.indices.map(index => s"__vw_generic_out_$index")
      encoded.toDF(internalColumns: _*).select(outputSchema.fields.zip(internalColumns).map {
        case (field, internalName) => F.col(internalName).cast(field.dataType).as(field.name, field.metadata)
      }: _*)
    }, dataset.columns.length)
  }

  /**
    * Preserves all input fields and appends flat prediction fields, rejecting non-string examples
    * and conflicting names. Uses the active Spark session's case-sensitivity setting, or false
    * when no session is active; callers should use the same session as the dataset to transform.
    */
  override def transformSchema(schema: StructType): StructType = {
    val caseSensitive = SparkSession.getActiveSession
      .exists(_.conf.get("spark.sql.caseSensitive", "false").trim.toBoolean)
    schemaForPredictionType(schema, caseSensitive)
  }
}

object VowpalWabbitGenericModel extends ComplexParamsReadable[VowpalWabbitGenericModel]
