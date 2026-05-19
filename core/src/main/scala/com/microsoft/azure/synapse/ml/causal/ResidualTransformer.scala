// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.HasOutputCol
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.ml.linalg.SQLDataTypes
import org.apache.spark.ml.param.{IntParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, NumericType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/** Compute the differences between observed and predicted values of data.
  * for classification, we compute residual as "observed - probability($(classIndex))"
  * for regression, we compute residual as "observed - prediction"
  */
class ResidualTransformer(override val uid: String) extends Transformer
  with HasOutputCol with DefaultParamsWritable with Wrappable with SynapseMLLogging {

  logClass(FeatureNames.Causal)

  def this() = this(Identifiable.randomUID("ComputeResidualsTransformer"))

  val observedCol = new Param[String](this, "observedCol", "observed data (label column)")

  def setObservedCol(value: String): this.type = set(param = observedCol, value = value)

  final def getObservedCol: String = getOrDefault(observedCol)

  val predictedCol = new Param[String](this, "predictedCol", "predicted data (prediction or probability columns")

  def setPredictedCol(value: String): this.type = set(param = predictedCol, value = value)

  final def getPredictedCol: String = getOrDefault(predictedCol)

  val classIndex =
    new IntParam(
      this,
      "classIndex",
      "The index of the class to compute residual for classification outputs. Default value is 1.",
      ParamValidators.gtEq(0))

  def setClassIndex(value: Int): this.type = set(param = classIndex, value = value)

  final def getClassIndex: Int = getOrDefault(classIndex)

  override def copy(extra: ParamMap): ResidualTransformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    StructType(
      schema.fields :+ StructField(
        name = $(outputCol),
        dataType = DoubleType,
        nullable = false
      )
    )

  setDefault(observedCol -> "label", predictedCol -> "prediction", outputCol -> "residual", classIndex -> 1)

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      transformSchema(schema = dataset.schema, logging = true)
      // Make sure the observedCol is a DoubleType or IntegerType
      val observedColType = dataset.schema(getObservedCol).dataType
      require(observedColType == DoubleType || observedColType == LongType
        || observedColType == IntegerType || observedColType == BooleanType,
        s"${this.getClass.getSimpleName}: " +
          s"observedCol must be of type DoubleType, LongType, IntegerType or BooleanType but got $observedColType")

      val convertedDataset = if (observedColType == BooleanType) {
        dataset.withColumn(getObservedCol, col(getObservedCol).cast(IntegerType))
      } else dataset

      val predictedColDataType = convertedDataset.schema(getPredictedCol).dataType

      predictedColDataType match {
        case SQLDataTypes.VectorType =>
          // For probability vector, compute the residual as "observed - probability($index)"
          convertedDataset.withColumn(getOutputCol,
            col(getObservedCol) - vector_to_array(col(getPredictedCol))(getClassIndex)
          )
        case _: NumericType =>
          // For prediction numeric, compute residual as "observed - prediction"
          convertedDataset.withColumn(getOutputCol,
            col(getObservedCol) - col(getPredictedCol)
          )
        case _ =>
          throw new IllegalArgumentException(
            s"Prediction column $getPredictedCol must be of type Vector or NumericType, but is $predictedColDataType" +
              s", please use 'setPredictedCol' to set the correct prediction column")
      }
    }, dataset.columns.length)
  }
}

object ResidualTransformer extends DefaultParamsReadable[ResidualTransformer]
