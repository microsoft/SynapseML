// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.schema

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.universe.TypeTag

abstract class SparkBindings[T: TypeTag] extends Serializable {

  lazy val schema: StructType = enc.schema
  private lazy val enc: ExpressionEncoder[T] = ExpressionEncoder[T]().resolveAndBind()
  private lazy val rowEnc: ExpressionEncoder[Row] = RowEncoder(enc.schema).resolveAndBind()

  // WARNING: each time you use this function on a dataframe, you should make a new converter.
  // Spark does some magic that makes this leak memory if re-used on a
  // different symbolic node of the parallel computation. That being said,
  // you should make a single converter before using it in a udf so
  // that the slow resolving and binding is not in the hotpath
  def makeFromRowConverter: Row => T = {
    val enc1 = enc.resolveAndBind().createDeserializer()
    val rowEnc1 = rowEnc.resolveAndBind().createSerializer();
    { r: Row => enc1(rowEnc1(r)) }
  }

  def makeFromInternalRowConverter: InternalRow => T = {
    val enc1 = enc.resolveAndBind().createDeserializer();
    { r: InternalRow => enc1(r) }
  }

  def makeToRowConverter: T => Row = {
    val enc1 = enc.resolveAndBind().createSerializer()
    val rowEnc1 = rowEnc.resolveAndBind().createDeserializer();
    { v: T => rowEnc1(enc1(v)) }
  }

  def makeToInternalRowConverter: T => InternalRow = {
    val enc1 = enc.resolveAndBind().createSerializer();
    { v: T => enc1(v) }
  }

}
