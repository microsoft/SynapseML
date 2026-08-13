// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.schema

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class VerifyBinaryFileSchema extends TestBase {

  test("ColumnSchema has correct structure") {
    val schema = BinaryFileSchema.ColumnSchema
    assert(schema.fields.length === 2)

    val pathField = schema.fields(0)
    assert(pathField.name === "path")
    assert(pathField.dataType === StringType)
    assert(pathField.nullable === true)

    val bytesField = schema.fields(1)
    assert(bytesField.name === "bytes")
    assert(bytesField.dataType === BinaryType)
    assert(bytesField.nullable === true)
  }

  test("Schema wraps ColumnSchema in value field") {
    val schema = BinaryFileSchema.Schema
    assert(schema.fields.length === 1)

    val valueField = schema.fields(0)
    assert(valueField.name === "value")
    assert(valueField.dataType === BinaryFileSchema.ColumnSchema)
    assert(valueField.nullable === true)
  }

  test("getPath extracts path from Row") {
    val testPath = "/path/to/file.bin"
    val testBytes = Array[Byte](1, 2, 3)
    val row = Row(testPath, testBytes)

    assert(BinaryFileSchema.getPath(row) === testPath)
  }

  test("getBytes extracts bytes from Row") {
    val testPath = "/path/to/file.bin"
    val testBytes = Array[Byte](1, 2, 3, 4, 5)
    val row = Row(testPath, testBytes)

    val result = BinaryFileSchema.getBytes(row)
    assert(result.sameElements(testBytes))
  }

  test("getPath handles empty path") {
    val row = Row("", Array[Byte]())
    assert(BinaryFileSchema.getPath(row) === "")
  }

  test("getBytes handles empty byte array") {
    val row = Row("test", Array[Byte]())
    assert(BinaryFileSchema.getBytes(row).length === 0)
  }

  test("isBinaryFile with DataType returns true for matching schema") {
    assert(BinaryFileSchema.isBinaryFile(BinaryFileSchema.ColumnSchema) === true)
  }

  test("isBinaryFile with DataType returns false for non-matching schema") {
    assert(BinaryFileSchema.isBinaryFile(StringType) === false)
    assert(BinaryFileSchema.isBinaryFile(BinaryType) === false)
    assert(BinaryFileSchema.isBinaryFile(IntegerType) === false)

    // Different StructType
    val differentSchema = StructType(Seq(
      StructField("path", StringType, true)
    ))
    assert(BinaryFileSchema.isBinaryFile(differentSchema) === false)
  }

  test("isBinaryFile with StructField returns true for matching schema") {
    val field = StructField("data", BinaryFileSchema.ColumnSchema, true)
    assert(BinaryFileSchema.isBinaryFile(field) === true)
  }

  test("isBinaryFile with StructField returns false for non-matching schema") {
    val stringField = StructField("data", StringType, true)
    assert(BinaryFileSchema.isBinaryFile(stringField) === false)

    val binaryField = StructField("data", BinaryType, true)
    assert(BinaryFileSchema.isBinaryFile(binaryField) === false)
  }
}
