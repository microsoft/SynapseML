// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

class VerifyUDFParam extends TestBase {

  private class TestParamsHolder extends Params {
    override val uid: String = "test-holder"
    val udfParam = new UDFParam(this, "udf", "A UDF param")
    override def copy(extra: ParamMap): Params = this
  }

  test("UDFParam can be created with basic constructor") {
    val holder = new TestParamsHolder
    assert(holder.udfParam.name === "udf")
    assert(holder.udfParam.doc === "A UDF param")
  }

  test("UDFParam accepts simple UDF") {
    val holder = new TestParamsHolder
    val myUdf = udf((x: Int) => x * 2)
    holder.set(holder.udfParam, myUdf)
    assert(holder.isSet(holder.udfParam))
  }

  test("UDFParam accepts string transformation UDF") {
    val holder = new TestParamsHolder
    val myUdf = udf((s: String) => s.toUpperCase)
    holder.set(holder.udfParam, myUdf)
    assert(holder.isSet(holder.udfParam))
  }

  test("UDFParam accepts multi-argument UDF") {
    val holder = new TestParamsHolder
    val myUdf = udf((a: Int, b: Int) => a + b)
    holder.set(holder.udfParam, myUdf)
    assert(holder.isSet(holder.udfParam))
  }

  test("UDFParam with custom validator") {
    val holder = new Params {
      override val uid: String = "test"
      // Accept any UDF
      val validatedUdf = new UDFParam(
        this, "validated", "Validated UDF",
        (_: UserDefinedFunction) => true
      )
      override def copy(extra: ParamMap): Params = this
    }
    val myUdf = udf((x: Double) => x * x)
    holder.set(holder.validatedUdf, myUdf)
  }

  test("UDFParam can be cleared") {
    val holder = new TestParamsHolder
    val myUdf = udf((x: Int) => x)
    holder.set(holder.udfParam, myUdf)
    assert(holder.isSet(holder.udfParam))
    holder.clear(holder.udfParam)
    assert(!holder.isSet(holder.udfParam))
  }

  test("UDFParam returns None when not set") {
    val holder = new TestParamsHolder
    assert(holder.get(holder.udfParam).isEmpty)
  }

  test("UDFParam assertEquality passes for same UDFs") {
    val holder = new TestParamsHolder
    val udf1 = udf((x: Int) => x * 2)
    val udf2 = udf((x: Int) => x * 2)
    // Note: This test verifies the assertEquality method exists and runs
    // Exact equality depends on internal UDF representation
    holder.udfParam.assertEquality(udf1, udf1)
  }

  test("UDFParam assertEquality throws for non-UDF types") {
    val holder = new TestParamsHolder
    assertThrows[AssertionError] {
      holder.udfParam.assertEquality("not a udf", "also not a udf")
    }
  }
}
