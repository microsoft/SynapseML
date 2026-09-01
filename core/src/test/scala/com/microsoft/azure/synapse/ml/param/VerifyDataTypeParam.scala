// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.using
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.Serializer
import org.apache.spark.ml.linalg.SQLDataTypes
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.sql.types._

import java.io.File

private class TestStringUDT extends UserDefinedType[String] {
  override def sqlType: DataType = StringType
  override def serialize(obj: String): Any = obj
  override def deserialize(datum: Any): String = datum.asInstanceOf[String]
  override def userClass: Class[String] = classOf[String]
}

class VerifyDataTypeParam extends TestBase {
  private val legacyDeserializationConfig =
    Serializer.LegacyObjectDeserializationConfig

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

  test("DataTypeParam custom validator accepts and rejects per its predicate") {
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
    assert(holder.get(holder.numericOnlyParam).contains(FloatType))
    assertThrows[IllegalArgumentException] {
      holder.set(holder.numericOnlyParam, StringType)
    }
  }

  test("DataTypeParam can be cleared") {
    val holder = new TestParamsHolder
    holder.set(holder.dataTypeParam, StringType)
    assert(holder.isSet(holder.dataTypeParam))
    holder.clear(holder.dataTypeParam)
    assert(!holder.isSet(holder.dataTypeParam))
  }

  test("DataTypeParam preserves the legacy format for standard data types") {
    val holder = new TestParamsHolder
    val path = new Path(new File(tmpDir.toFile, "standard-data-type").toString)
    val expected = StructType(Seq(
      StructField("values", ArrayType(DecimalType(12, 4))),
      StructField("features", SQLDataTypes.VectorType)
    ))

    holder.dataTypeParam.save(expected, spark, path, overwrite = true)

    using(path.getFileSystem(spark.sparkContext.hadoopConfiguration).open(path)) { input =>
      assert(input.read() === 0xac)
      assert(input.read() === 0xed)
    }.get
    assert(holder.dataTypeParam.load(spark, path) === expected)
  }

  test("DataTypeParam requires explicit trust for custom data types") {
    val holder = new TestParamsHolder
    val path = new Path(new File(tmpDir.toFile, "custom-data-type").toString)
    val config = legacyDeserializationConfig
    val previous = spark.conf.getOption(config)
    holder.dataTypeParam.save(new TestStringUDT, spark, path, overwrite = true)
    spark.conf.unset(config)

    try {
      val error = intercept[SecurityException] {
        holder.dataTypeParam.load(spark, path)
      }
      assert(error.getMessage.contains(config))

      spark.conf.set(config, "true")
      assert(holder.dataTypeParam.load(spark, path).isInstanceOf[TestStringUDT])
    } finally {
      previous.fold(spark.conf.unset(config))(spark.conf.set(config, _))
    }
  }
}
