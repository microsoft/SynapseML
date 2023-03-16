// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import com.microsoft.azure.synapse.ml.core.utils.SlicerFunctions
import com.microsoft.azure.synapse.ml.param.{DataFrameParam, TransformerParam}
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param._
import org.apache.spark.sql.functions.{array, col}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}

trait CanValidateSchema {
  protected def validateSchema(inputSchema: StructType): Unit = {}
}

trait HasMetricsCol extends Params with CanValidateSchema {
  final val metricsCol = new Param[String](
    this,
    "metricsCol",
    "Column name for fitting metrics"
  )

  final def getMetricsCol: String = $(metricsCol)

  final def setMetricsCol(v: String): this.type = this.set(metricsCol, v)

  protected override def validateSchema(inputSchema: StructType): Unit = {
    super.validateSchema(inputSchema)
    require(
      !inputSchema.fieldNames.contains(getMetricsCol),
      s"Input schema (${inputSchema.simpleString}) already contains metrics column $getMetricsCol"
    )
  }
}

trait HasModel extends Params with CanValidateSchema {
  final val model = new TransformerParam(this, "model", "The model to be interpreted.")

  final def getModel: Transformer = $(model)

  final def setModel(v: Transformer): this.type = set(model, v)
}

trait HasNumSamples extends Params with CanValidateSchema {
  final val numSamples: IntParam = new IntParam(
    this,
    "numSamples",
    "Number of samples to generate.",
    ParamValidators.gt(0)
  )

  final def getNumSamples: Int = $(numSamples)

  final def getNumSamplesOpt: Option[Int] = this.get(numSamples)

  final def setNumSamples(value: Int): this.type = this.set(numSamples, value)
}

trait HasTokensCol extends Params with CanValidateSchema {
  final val tokensCol = new Param[String](
    this,
    "tokensCol",
    "The column holding the tokens"
  )

  final def getTokensCol: String = $(tokensCol)

  final def setTokensCol(v: String): this.type = this.set(tokensCol, v)
}

trait HasSuperpixelCol extends Params with CanValidateSchema {
  final val superpixelCol = new Param[String](
    this,
    "superpixelCol",
    "The column holding the superpixel decompositions"
  )

  final def getSuperpixelCol: String = $(superpixelCol)

  final def setSuperpixelCol(v: String): this.type = set(superpixelCol, v)
}

trait HasSamplingFraction extends Params with CanValidateSchema {
  final val samplingFraction = new DoubleParam(
    this,
    "samplingFraction",
    "The fraction of superpixels (for image) or tokens (for text) to keep on",
    ParamValidators.inRange(0, 1)
  )

  final def getSamplingFraction: Double = $(samplingFraction)

  final def setSamplingFraction(d: Double): this.type = set(samplingFraction, d)
}

trait HasBackgroundData extends Params with CanValidateSchema {
  final val backgroundData: DataFrameParam = new DataFrameParam(
    this,
    "backgroundData",
    "A dataframe containing background data"
  )

  final def getBackgroundData: DataFrame = $(backgroundData)

  final def setBackgroundData(value: DataFrame): this.type = set(backgroundData, value)
}

trait HasExplainTarget extends Params with CanValidateSchema {
  final val targetCol: Param[String] = new Param[String](
    this,
    "targetCol",
    "The column name of the prediction target to explain (i.e. the response variable). " +
      "This is usually set to \"prediction\" for regression models and " +
      "\"probability\" for probabilistic classification models. Default value: probability")
  setDefault(targetCol, "probability")

  final def getTargetCol: String = $(targetCol)

  final def setTargetCol(value: String): this.type = this.set(targetCol, value)

  final val targetClasses: IntArrayParam = new IntArrayParam(
    this,
    "targetClasses",
    "The indices of the classes for multinomial classification models. Default: 0." +
      "For regression models this parameter is ignored."
  )

  final def getTargetClasses: Array[Int] = $(targetClasses)

  final def setTargetClasses(values: Array[Int]): this.type = this.set(targetClasses, values)

  final val targetClassesCol: Param[String] = new Param[String](
    this,
    "targetClassesCol",
    "The name of the column that specifies the indices of the classes for multinomial classification models."
  )

  final def getTargetClassesCol: String = $(targetClassesCol)

  final def setTargetClassesCol(value: String): this.type = this.set(targetClassesCol, value)

  protected override def validateSchema(inputSchema: StructType): Unit = {
    super.validateSchema(inputSchema)
    if (this.get(targetClassesCol).isDefined) {
      val dataType = inputSchema(this.getTargetClassesCol).dataType
      require(DataType.equalsStructurally(dataType, ArrayType(IntegerType), ignoreNullability = true),
        s"Column $getTargetClassesCol must be an array type of integers, but got $dataType instead"
      )
    }
  }

  setDefault(targetClasses -> Array.empty[Int])

  /**
    * This function supports a variety of target column types:
    * - NumericType: in the case of a regression model
    * - VectorType: in the case of a typical Spark ML classification model with probability output
    * - ArrayType(NumericType): in the case where the output was converted to an array of numeric types.
    * - MapType(IntegerType, NumericType): this is to support ZipMap type of output for sklearn models via ONNX runtime.
    */

  private[explainers] def extractTarget(schema: StructType, targetClassesCol: String): Column = {
    val toVector = UDFUtils.oldUdf(
      (values: Seq[Double]) => Vectors.dense(values.toArray),
      VectorType
    )

    val target = schema(getTargetCol).dataType match {
      case _: NumericType =>
        toVector(array(col(getTargetCol)))
      case VectorType =>
        SlicerFunctions.vectorSlicer(col(getTargetCol), col(targetClassesCol))
      case ArrayType(elementType: NumericType, _) =>
        SlicerFunctions.arraySlicer(elementType)(col(getTargetCol), col(targetClassesCol))
      case MapType(_: IntegerType | LongType | ShortType | ByteType, valueType: NumericType, _) =>
        SlicerFunctions.mapSlicer(valueType)(col(getTargetCol), col(targetClassesCol))
      case other =>
        throw new IllegalArgumentException(
          s"Only numeric types, vector type, array of numeric types and map types with numeric value type " +
            s"are supported as target column. The current type is $other."
        )
    }

    target
  }
}
