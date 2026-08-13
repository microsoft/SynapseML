// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.schema

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.image.ImageSchema
import org.apache.spark.sql.types._

class VerifyImageSchemaUtils extends TestBase {

  test("ColumnSchemaNullable has correct structure") {
    val schema = ImageSchemaUtils.ColumnSchemaNullable
    assert(schema.fields.length === 6)

    assert(schema.fields(0).name === "origin")
    assert(schema.fields(0).dataType === StringType)

    assert(schema.fields(1).name === "height")
    assert(schema.fields(1).dataType === IntegerType)

    assert(schema.fields(2).name === "width")
    assert(schema.fields(2).dataType === IntegerType)

    assert(schema.fields(3).name === "nChannels")
    assert(schema.fields(3).dataType === IntegerType)

    assert(schema.fields(4).name === "mode")
    assert(schema.fields(4).dataType === IntegerType)

    assert(schema.fields(5).name === "data")
    assert(schema.fields(5).dataType === BinaryType)
  }

  test("ColumnSchemaNullable fields are all nullable") {
    val schema = ImageSchemaUtils.ColumnSchemaNullable
    schema.fields.foreach { field =>
      assert(field.nullable === true, s"Field ${field.name} should be nullable")
    }
  }

  test("ImageSchemaNullable wraps ColumnSchemaNullable") {
    val schema = ImageSchemaUtils.ImageSchemaNullable
    assert(schema.fields.length === 1)
    assert(schema.fields(0).name === "image")
    assert(schema.fields(0).dataType === ImageSchemaUtils.ColumnSchemaNullable)
    assert(schema.fields(0).nullable === true)
  }

  test("isImage returns true for ImageSchema.columnSchema") {
    assert(ImageSchemaUtils.isImage(ImageSchema.columnSchema) === true)
  }

  test("isImage returns false for non-image types") {
    assert(ImageSchemaUtils.isImage(StringType) === false)
    assert(ImageSchemaUtils.isImage(BinaryType) === false)
    assert(ImageSchemaUtils.isImage(IntegerType) === false)
  }

  test("isImage returns false for different struct type") {
    val differentSchema = StructType(Seq(
      StructField("path", StringType, true)
    ))
    assert(ImageSchemaUtils.isImage(differentSchema) === false)
  }

  test("isImage with StructField returns true for image column") {
    val imageField = StructField("img", ImageSchema.columnSchema, true)
    assert(ImageSchemaUtils.isImage(imageField) === true)
  }

  test("isImage with StructField returns false for non-image column") {
    val stringField = StructField("text", StringType, true)
    assert(ImageSchemaUtils.isImage(stringField) === false)
  }

  test("ColumnSchemaNullable matches ImageSchema.columnSchema structurally") {
    // The nullable version should match structurally when ignoring nullability
    val isMatch = DataType.equalsStructurally(
      ImageSchemaUtils.ColumnSchemaNullable,
      ImageSchema.columnSchema,
      ignoreNullability = true
    )
    assert(isMatch === true)
  }
}
