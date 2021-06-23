// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.explainers

import com.microsoft.ml.spark.core.utils.SlicerFunctions
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.linalg.SQLDataTypes.{MatrixType, VectorType}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.shared.HasOutputCol
import org.apache.spark.ml.{ComplexParamsWritable, Transformer}
import org.apache.spark.sql.functions.{array, col}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, NumericType, StructType}

trait LocalExplainer
  extends Transformer with HasExplainTarget with HasOutputCol with HasModel with ComplexParamsWritable {

  final def setOutputCol(value: String): this.type = this.set(outputCol, value)

  protected override def validateSchema(inputSchema: StructType): Unit = {
    super.validateSchema(inputSchema)
    if (inputSchema.fieldNames.contains(getOutputCol)) {
      throw new IllegalArgumentException(s"Input schema already has column $getOutputCol")
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    this.validateSchema(schema)
    schema.add(getOutputCol, MatrixType)
  }

  protected def preprocess(df: DataFrame): DataFrame = df

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
      case MapType(_: IntegerType, valueType: NumericType, _) =>
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

object LocalExplainer {
  object LIME {
    def tabular: TabularLIME = {
      new TabularLIME()
    }

    def vector: VectorLIME = {
      new VectorLIME()
    }

    def image: ImageLIME = {
      new ImageLIME()
    }

    def text: TextLIME = {
      new TextLIME()
    }
  }

  object KernelSHAP {
    def tabular: TabularSHAP = {
      new TabularSHAP()
    }

    def vector: VectorSHAP = {
      new VectorSHAP()
    }

    def image: ImageSHAP = {
      new ImageSHAP()
    }

    def text: TextSHAP = {
      new TextSHAP()
    }
  }
}
