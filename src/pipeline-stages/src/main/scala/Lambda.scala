// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.core.contracts.Wrappable
import com.microsoft.ml.spark.core.serialize.{ComplexParamsReadable, ComplexParamsWritable}
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{ParamMap, UDFParam}
import org.apache.spark.ml.util.{ComplexParamsReadable, Identifiable}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Lambda extends ComplexParamsReadable[Lambda] {
  def apply(f: Dataset[_] => DataFrame): Lambda = {
    new Lambda().setTransform(f)
  }
}

class Lambda(val uid: String) extends Transformer with Wrappable with ComplexParamsWritable {
  def this() = this(Identifiable.randomUID("Lambda"))

  val transformFunc = new UDFParam(this, "transformFunc", "holder for dataframe function")

  def setTransform(f: Dataset[_] => DataFrame): this.type = {
    set(transformFunc, udf(f, StringType))
  }

  def getTransform: Dataset[_] => DataFrame = {
    $(transformFunc).f.asInstanceOf[Dataset[_] => DataFrame]
  }

  val transformSchemaFunc = new UDFParam(this, "transformSchemaFunc", "the output schema after the transformation")

  def setTransformSchema(f: StructType => StructType): this.type = {
    set(transformSchemaFunc, udf(f, StringType))
  }

  def getTransformSchema: StructType => StructType = {
    $(transformSchemaFunc).f.asInstanceOf[StructType => StructType]
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    getTransform(dataset)
  }

  def transformSchema(schema: StructType): StructType = {
    if (get(transformSchemaFunc).isEmpty) {
      val sc = SparkContext.getOrCreate()
      val df = SparkSession.builder().getOrCreate().createDataFrame(sc.emptyRDD[Row], schema)
      transform(df).schema
    } else {
      getTransformSchema(schema)
    }
  }

  def copy(extra: ParamMap): Lambda = defaultCopy(extra)

}
