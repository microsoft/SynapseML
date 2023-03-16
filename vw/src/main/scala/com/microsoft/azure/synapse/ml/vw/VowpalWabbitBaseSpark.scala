// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.contracts.HasWeightCol
import com.microsoft.azure.synapse.ml.core.env.StreamUtilities
import org.apache.spark.SparkContext
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasLabelCol}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types => T}
import org.vowpalwabbit.spark.VowpalWabbitExample

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * * Base implementation for estimators that use Spark-based features (e.g. SparkML vectors)
  */
trait VowpalWabbitBaseSpark extends VowpalWabbitBaseLearner
  with HasLabelCol
  with HasWeightCol
  with HasFeaturesCol
  with HasAdditionalFeatures {
  setDefault(additionalFeatures -> Array())

  protected def getAdditionalColumns: Seq[String] = Seq.empty

  private val costSensitiveLabelSchema = T.StructType(Seq(
    T.StructField("costs", T.ArrayType(T.FloatType), false),
    T.StructField("classes", T.ArrayType(T.IntegerType), false)
  ))

  private val contextualBanditLabelSchema = T.StructType(Seq(
    T.StructField("action", T.IntegerType, false),
    T.StructField("cost", T.FloatType, false),
    T.StructField("probability", T.FloatType, false)
  ))

  private val contextualBanditContinuousLabelSchema = T.StructType(Seq(
    T.StructField("actions", T.ArrayType(T.FloatType), false),
    T.StructField("costs", T.ArrayType(T.FloatType), false),
    T.StructField("pdfValues", T.ArrayType(T.FloatType), false)
  ))

  /**
    * default weight to 1 if not provided
    * @param schema the schema to fetch weight from
    * @return an accessor method used to fetch the weight
    */
  protected def getWeightGetter(schema: T.StructType) = if (get(weightCol).isDefined)
    getAsFloat(schema, schema.fieldIndex(getWeightCol))
  else
    (_: Row) => 1f

  type VowpalWabbitLabelSetFunc = (Row, VowpalWabbitExample) => Unit

  private def floatLabelSet(schema: T.StructType): VowpalWabbitLabelSetFunc = {
    val labelColIdx = schema.fieldIndex(getLabelCol)
    val weightGetter = getWeightGetter(schema)

    (row: Row, ex: VowpalWabbitExample) => ex.setLabel(weightGetter(row), row.getFloat(labelColIdx))
  }

  private def doubleLabelSet(schema: T.StructType): VowpalWabbitLabelSetFunc = {
    val labelColIdx = schema.fieldIndex(getLabelCol)
    val weightGetter = getWeightGetter(schema)

    (row: Row, ex: VowpalWabbitExample) => ex.setLabel(weightGetter(row), row.getDouble(labelColIdx).toFloat)
  }

  private def integerLabelSet(schema: T.StructType): VowpalWabbitLabelSetFunc = {
    val labelColIdx = schema.fieldIndex(getLabelCol)
    val weightGetter = getWeightGetter(schema)

    (row: Row, ex: VowpalWabbitExample) => ex.setMulticlassLabel(weightGetter(row), row.getInt(labelColIdx))
  }

  private def arrayIntegerLabelSet(schema: T.StructType): VowpalWabbitLabelSetFunc = {
    val labelColIdx = schema.fieldIndex(getLabelCol)

    logWarning("Ignoring weights. MultiLabels does not support weights.")

    (row: Row, ex: VowpalWabbitExample) => ex.setMultiLabels(row.getAs[Seq[Int]](labelColIdx).toArray)
  }

  private def costSensitiveLabelSet(t: T.StructType): VowpalWabbitLabelSetFunc = {
    val costsIndex = t.fieldIndex("cost")
    val classesIndex = t.fieldIndex("classes")

    (row: Row, ex: VowpalWabbitExample) =>
      ex.setCostSensitiveLabels(
        row.getAs[Seq[Float]](costsIndex).toArray,
        row.getAs[Seq[Int]](classesIndex).toArray)
  }

  private def contextualBanditLabel(t: T.StructType): VowpalWabbitLabelSetFunc = {
    val actionIndex = t.fieldIndex("action")
    val costIndex = t.fieldIndex("cost")
    val probabilityIndex = t.fieldIndex("probability")

    (row: Row, ex: VowpalWabbitExample) =>
      ex.setContextualBanditLabel(
        row.getInt(actionIndex),
        row.getFloat(costIndex),
        row.getFloat(probabilityIndex))
  }

  private def contextualBanditContinuousLabel(t: T.StructType): VowpalWabbitLabelSetFunc = {
    val actionsIndex = t.fieldIndex("actions")
    val costIndex = t.fieldIndex("costs")
    val pdfValuesIndex = t.fieldIndex("pdfValues")

    (row: Row, ex: VowpalWabbitExample) =>
      ex.setContextualBanditContinuousLabel(
        row.getAs[Seq[Float]](actionsIndex).toArray,
        row.getAs[Seq[Float]](costIndex).toArray,
        row.getAs[Seq[Float]](pdfValuesIndex).toArray)
  }

  // scalastyle:off cyclomatic.complexity
  // this only works well for Estimators. for Predictors the label is always going to be a Double
  protected def createLabelSetter(schema: T.StructType): VowpalWabbitLabelSetFunc = {
    // match label spark types to VW labels
    schema(schema.fieldIndex(getLabelCol)).dataType match {
      case _: T.FloatType => floatLabelSet(schema)
      case _: T.DoubleType => doubleLabelSet(schema)
      case _: T.IntegerType => integerLabelSet(schema)
      case t: T.ArrayType if t.elementType == T.IntegerType => arrayIntegerLabelSet(schema)
      case t: T.StructType if t.equals(costSensitiveLabelSchema) => costSensitiveLabelSet(t)
      case t: T.StructType if t.equals(contextualBanditLabelSchema) => contextualBanditLabel(t)
      case t: T.StructType if t.equals(contextualBanditContinuousLabelSchema) => contextualBanditContinuousLabel(t)
      case other => throw new UnsupportedOperationException(s"Label type is not supported $other")
    }
  }
  // scalastyle:on cyclomatic.complexity

  protected override def getInputColumns: Seq[String] =
    Seq(getFeaturesCol, getLabelCol) ++
      getAdditionalFeatures ++
      getAdditionalColumns ++
      Seq(get(weightCol)).flatten

  protected override def trainFromRows(schema: T.StructType,
                                       inputRows: Iterator[Row],
                                       ctx: TrainContext): Unit = {
    val applyLabel = createLabelSetter(schema)
    val featureColIndices = VowpalWabbitUtil.generateNamespaceInfos(
      schema,
      getHashSeed,
      Seq(getFeaturesCol) ++ getAdditionalFeatures)

    val outputPredictions = isDefined(predictionIdCol)

    StreamUtilities.using(ctx.vw.createExample()) { example =>
      for (row <- inputRows) {
        ctx.nativeIngestTime.measure {
          // transfer label
          applyLabel(row, example)

          // transfer features
          VowpalWabbitUtil.addFeaturesToExample(featureColIndices, row, example)
        }

        // learn and cleanup
        ctx.learnTime.measure {
          try {
            // collect 1-step ahead predictions
            if (outputPredictions)
              ctx.predictionBuffer.append(row, example.predict())

            example.learn()
          }
          finally {
            example.clear()
          }
        }

        // inter-pass all reduce trigger
        if (ctx.synchronizationSchedule.shouldTriggerAllReduce(row))
          ctx.vw.endPass()
      }
    }

  }
}
