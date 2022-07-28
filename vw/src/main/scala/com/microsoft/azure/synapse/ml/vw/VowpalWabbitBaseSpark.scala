// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.contracts.{HasWeightCol}
import com.microsoft.azure.synapse.ml.core.env.StreamUtilities
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasLabelCol}
import org.apache.spark.sql.{Row, types => T}
import org.vowpalwabbit.spark.VowpalWabbitExample

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

  // this only works well for Estimators. for Predictors the label is always going to be a Double
  protected def createLabelSetter(schema: T.StructType): (Row, VowpalWabbitExample) => Unit = {
    val labelColIdx = schema.fieldIndex(getLabelCol)
    val weightGetter = getWeightGetter(schema)

    // match label spark types to VW labels

    schema(labelColIdx).dataType match {
      case _: T.FloatType =>
        (row: Row, ex: VowpalWabbitExample) =>
          ex.setLabel(weightGetter(row), row.getFloat(labelColIdx))

      case _: T.DoubleType =>
        (row: Row, ex: VowpalWabbitExample) =>
          ex.setLabel(weightGetter(row), row.getDouble(labelColIdx).toFloat)

      case _: T.IntegerType =>
        (row: Row, ex: VowpalWabbitExample) =>
          ex.setMulticlassLabel(weightGetter(row), row.getInt(labelColIdx))

      case t: T.ArrayType if t.elementType == T.IntegerType =>
        logWarning("Ignoring weights. MultiLabels does not support weights.")

        (row: Row, ex: VowpalWabbitExample) =>
          ex.setMultiLabels(row.getAs[Seq[Int]](labelColIdx).toArray)

      case t: T.StructType if t.equals(costSensitiveLabelSchema) =>
        val costsIndex = t.fieldIndex("cost")
        val classesIndex = t.fieldIndex("classes")

        (row: Row, ex: VowpalWabbitExample) =>
          ex.setCostSensitiveLabels(
            row.getAs[Seq[Float]](costsIndex).toArray,
            row.getAs[Seq[Int]](classesIndex).toArray)

      case t: T.StructType if t.equals(contextualBanditLabelSchema) =>
        val actionIndex = t.fieldIndex("action")
        val costIndex = t.fieldIndex("cost")
        val probabilityIndex = t.fieldIndex("probability")

        (row: Row, ex: VowpalWabbitExample) =>
          ex.setContextualBanditLabel(
            row.getInt(actionIndex),
            row.getFloat(costIndex),
            row.getFloat(probabilityIndex))

      case t: T.StructType if t.equals(contextualBanditContinuousLabelSchema) =>
        val actionsIndex = t.fieldIndex("actions")
        val costIndex = t.fieldIndex("costs")
        val pdfValuesIndex = t.fieldIndex("pdfValues")

        (row: Row, ex: VowpalWabbitExample) =>
          ex.setContextualBanditContinuousLabel(
            row.getAs[Seq[Float]](actionsIndex).toArray,
            row.getAs[Seq[Float]](costIndex).toArray,
            row.getAs[Seq[Float]](pdfValuesIndex).toArray)

      case other => throw new UnsupportedOperationException(s"Label type is not supported $other")
    }
  }

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
