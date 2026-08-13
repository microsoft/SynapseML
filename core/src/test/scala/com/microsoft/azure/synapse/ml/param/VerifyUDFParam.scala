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

  test("UDFParam with custom validator rejects values the validator refuses") {
    val holder = new Params {
      override val uid: String = "test"
      // Only accept UDFs, and only when the validator agrees; here nothing is acceptable
      val validatedUdf = new UDFParam(
        this, "validated", "Validated UDF",
        (_: UserDefinedFunction) => false
      )
      override def copy(extra: ParamMap): Params = this
    }
    val myUdf = udf((x: Double) => x * x)
    assertThrows[IllegalArgumentException] {
      holder.set(holder.validatedUdf, myUdf)
    }
  }

  test("UDFParam with custom validator accepts values the validator allows") {
    val holder = new Params {
      override val uid: String = "test"
      val validatedUdf = new UDFParam(
        this, "validated", "Validated UDF",
        (_: UserDefinedFunction) => true
      )
      override def copy(extra: ParamMap): Params = this
    }
    val myUdf = udf((x: Double) => x * x)
    holder.set(holder.validatedUdf, myUdf)
    assert(holder.isSet(holder.validatedUdf))
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

  test("UDFParam assertEquality accepts the same UDF and rejects mismatched ones") {
    val holder = new TestParamsHolder
    val intUdf = udf((x: Int) => x * 2)
    val stringUdf = udf((x: Int) => x.toString)
    holder.udfParam.assertEquality(intUdf, intUdf)
    // UDFs with different return types must not compare equal
    assertThrows[AssertionError] {
      holder.udfParam.assertEquality(intUdf, stringUdf)
    }
  }

  test("UDFParam assertEquality throws for non-UDF types") {
    val holder = new TestParamsHolder
    assertThrows[AssertionError] {
      holder.udfParam.assertEquality("not a udf", "also not a udf")
    }
  }
}
