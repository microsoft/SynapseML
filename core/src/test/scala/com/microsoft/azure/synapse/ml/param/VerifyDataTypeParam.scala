// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.sql.types._

class VerifyDataTypeParam extends TestBase {

  private class TestParamsHolder extends Params {
    override val uid: String = "test-holder"
    val dataTypeParam = new DataTypeParam(this, "dataType", "A data type param")
    override def copy(extra: ParamMap): Params = this
  }

  test("DataTypeParam can be created with basic constructor") {
    val holder = new TestParamsHolder
    assert(holder.dataTypeParam.name === "dataType")
    assert(holder.dataTypeParam.doc === "A data type param")
  }

  test("DataTypeParam accepts StringType") {
    val holder = new TestParamsHolder
    holder.set(holder.dataTypeParam, StringType)
    assert(holder.get(holder.dataTypeParam).contains(StringType))
  }

  test("DataTypeParam accepts IntegerType") {
    val holder = new TestParamsHolder
    holder.set(holder.dataTypeParam, IntegerType)
    assert(holder.get(holder.dataTypeParam).contains(IntegerType))
  }

  test("DataTypeParam accepts DoubleType") {
    val holder = new TestParamsHolder
    holder.set(holder.dataTypeParam, DoubleType)
    assert(holder.get(holder.dataTypeParam).contains(DoubleType))
  }

  test("DataTypeParam accepts BooleanType") {
    val holder = new TestParamsHolder
    holder.set(holder.dataTypeParam, BooleanType)
    assert(holder.get(holder.dataTypeParam).contains(BooleanType))
  }

  test("DataTypeParam accepts ArrayType") {
    val holder = new TestParamsHolder
    val arrayType = ArrayType(StringType)
    holder.set(holder.dataTypeParam, arrayType)
    assert(holder.get(holder.dataTypeParam).contains(arrayType))
  }

  test("DataTypeParam accepts MapType") {
    val holder = new TestParamsHolder
    val mapType = MapType(StringType, IntegerType)
    holder.set(holder.dataTypeParam, mapType)
    assert(holder.get(holder.dataTypeParam).contains(mapType))
  }

  test("DataTypeParam accepts StructType") {
    val holder = new TestParamsHolder
    val structType = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType)
    ))
    holder.set(holder.dataTypeParam, structType)
    assert(holder.get(holder.dataTypeParam).contains(structType))
  }

  test("DataTypeParam accepts nested StructType") {
    val holder = new TestParamsHolder
    val nestedType = StructType(Seq(
      StructField("outer", StructType(Seq(
        StructField("inner", StringType)
      )))
    ))
    holder.set(holder.dataTypeParam, nestedType)
    assert(holder.get(holder.dataTypeParam).contains(nestedType))
  }

  test("DataTypeParam accepts TimestampType") {
    val holder = new TestParamsHolder
    holder.set(holder.dataTypeParam, TimestampType)
    assert(holder.get(holder.dataTypeParam).contains(TimestampType))
  }

  test("DataTypeParam accepts DateType") {
    val holder = new TestParamsHolder
    holder.set(holder.dataTypeParam, DateType)
    assert(holder.get(holder.dataTypeParam).contains(DateType))
  }

  test("DataTypeParam accepts BinaryType") {
    val holder = new TestParamsHolder
    holder.set(holder.dataTypeParam, BinaryType)
    assert(holder.get(holder.dataTypeParam).contains(BinaryType))
  }

  test("DataTypeParam with custom validator") {
    val holder = new Params {
      override val uid: String = "test"
      val numericOnlyParam = new DataTypeParam(
        this, "numericOnly", "Only numeric types",
        (dt: DataType) => dt.isInstanceOf[NumericType]
      )
      override def copy(extra: ParamMap): Params = this
    }
    holder.set(holder.numericOnlyParam, IntegerType)
    holder.set(holder.numericOnlyParam, DoubleType)
    holder.set(holder.numericOnlyParam, FloatType)
  }

  test("DataTypeParam can be cleared") {
    val holder = new TestParamsHolder
    holder.set(holder.dataTypeParam, StringType)
    assert(holder.isSet(holder.dataTypeParam))
    holder.clear(holder.dataTypeParam)
    assert(!holder.isSet(holder.dataTypeParam))
  }
}
