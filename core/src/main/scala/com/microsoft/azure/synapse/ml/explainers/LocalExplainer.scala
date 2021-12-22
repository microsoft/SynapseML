// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import com.microsoft.azure.synapse.ml.core.utils.SlicerFunctions
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
