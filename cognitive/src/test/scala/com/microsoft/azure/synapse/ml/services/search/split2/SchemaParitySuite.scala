// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.search.split2

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.services.search.AzureSearchWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, StringType, StructField, StructType}
import org.scalatest.PrivateMethodTester

/** Secret-free checks of the writer's production schema preflight, not the Azure service. */
class SchemaParitySuite extends TestBase with PrivateMethodTester {

  private val parity = PrivateMethod[Unit](Symbol("checkSchemaParity"))
  private val actionColumn = "action"

  private def indexJson(payload: String): String =
    s"""{"name":"parity-index","fields":[{"name":"id","type":"Edm.String","key":true},$payload]}"""

  private def complex(fields: String, collection: Boolean = false): String = {
    val kind = if (collection) "Collection(Edm.ComplexType)" else "Edm.ComplexType"
    s"""{"name":"payload","type":"$kind","fields":[$fields]}"""
  }

  private val a = """{"name":"a","type":"Edm.String"}"""
  private val b = """{"name":"b","type":"Edm.String"}"""

  private def shape(payload: DataType): StructType = StructType(Seq(
    StructField(actionColumn, StringType), StructField("id", StringType), StructField("payload", payload)))

  private def check(schema: StructType, json: String): Unit = {
    // Exercise an actual Spark DataFrame schema and the real private parser/guard together.
    val frame = spark.createDataFrame(java.util.Collections.emptyList[Row](), schema)
    AzureSearchWriter invokePrivate parity(frame.schema, json, actionColumn)
  }

  private def fields(names: String*): StructType =
    StructType(names.map(name => StructField(name, StringType)))

  test("production private schema preflight resolves and accepts an equal nested shape") {
    check(shape(fields("a")), indexJson(complex(a)))
  }

  test("schema preflight rejects an extra trailing nested DataFrame field") {
    intercept[AssertionError] { check(shape(fields("a", "b")), indexJson(complex(a))) }
  }

  test("schema preflight rejects an extra trailing field in an array of structs") {
    intercept[AssertionError] {
      check(shape(ArrayType(fields("a", "b"))), indexJson(complex(a, collection = true)))
    }
  }

  test("schema preflight rejects a deeper nested extra DataFrame field") {
    val child = """{"name":"child","type":"Edm.ComplexType","fields":[""" + a + "]}"
    intercept[AssertionError] {
      check(shape(StructType(Seq(StructField("child", fields("a", "b"))))), indexJson(complex(child)))
    }
  }

  test("schema preflight preserves nested and array nullability differences") {
    val nonNullable = StructType(Seq(StructField("a", StringType, nullable = false)))
    check(shape(nonNullable), indexJson(complex(a)))
    check(shape(ArrayType(nonNullable, containsNull = false)), indexJson(complex(a, collection = true)))
  }

  test("schema preflight preserves an omitted trailing index field") {
    check(shape(fields("a")), indexJson(complex(a + "," + b)))
    check(shape(ArrayType(fields("a"))), indexJson(complex(a + "," + b, collection = true)))
  }

  test("schema preflight still rejects a nested type mismatch") {
    intercept[AssertionError] {
      check(shape(StructType(Seq(StructField("a", IntegerType)))), indexJson(complex(a)))
    }
  }

  test("schema preflight still rejects renamed or missing middle nested fields") {
    intercept[AssertionError] { check(shape(fields("renamed")), indexJson(complex(a))) }
    intercept[AssertionError] { check(shape(fields("b")), indexJson(complex(a + "," + b))) }
  }

  test("schema preflight still rejects an unknown top-level DataFrame field") {
    val schema = StructType(shape(fields("a")).fields :+ StructField("unknown", StringType))
    intercept[IllegalArgumentException] { check(schema, indexJson(complex(a))) }
  }

  test("schema preflight preserves an empty nested prefix") {
    check(shape(StructType(Seq.empty)), indexJson(complex(a)))
  }
}
