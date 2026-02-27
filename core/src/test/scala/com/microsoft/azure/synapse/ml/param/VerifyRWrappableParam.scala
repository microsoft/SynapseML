// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.param.{ParamMap, Params}
import spray.json._
import spray.json.DefaultJsonProtocol._

class VerifyRWrappableParam extends TestBase {

  test("RPrinter converts JsNull to R NULL") {
    val result = RPrinter(JsNull)
    assert(result === "NULL")
  }

  test("RPrinter converts JsTrue to R TRUE") {
    val result = RPrinter(JsTrue)
    assert(result === "TRUE")
  }

  test("RPrinter converts JsFalse to R FALSE") {
    val result = RPrinter(JsFalse)
    assert(result === "FALSE")
  }

  test("RPrinter converts integer JsNumber with L suffix") {
    val result = RPrinter(JsNumber(42))
    assert(result === "42L")
  }

  test("RPrinter converts double JsNumber without L suffix") {
    val result = RPrinter(JsNumber(3.14))
    assert(result === "3.14")
  }

  test("RPrinter converts JsString correctly") {
    val result = RPrinter(JsString("hello"))
    assert(result === "\"hello\"")
  }

  test("RPrinter converts empty JsArray to c()") {
    val arr = JsArray()
    val result = RPrinter(arr)
    assert(result === "c()")
  }

  test("RPrinter converts JsArray of numbers to list()") {
    val arr = JsArray(JsNumber(1), JsNumber(2), JsNumber(3))
    val result = RPrinter(arr)
    assert(result === "list(1L,2L,3L)")
  }

  test("RPrinter converts empty JsObject to c()") {
    val obj = JsObject()
    val result = RPrinter(obj)
    assert(result === "c()")
  }

  test("RPrinter converts JsObject to list2env") {
    val obj = JsObject("key" -> JsString("value"))
    val result = RPrinter(obj)
    assert(result.contains("list2env"))
    assert(result.contains("key"))
    assert(result.contains("value"))
  }

  test("RPrinter converts nested JsObject correctly") {
    val nested = JsObject(
      "bool" -> JsTrue,
      "null" -> JsNull,
      "number" -> JsNumber(42)
    )
    val result = RPrinter(nested)
    assert(result.contains("TRUE"))
    assert(result.contains("NULL"))
    assert(result.contains("42L"))
  }

  test("RPrinter converts JsArray of JsObjects correctly") {
    val arr = JsArray(
      JsObject("a" -> JsNumber(1)),
      JsObject("b" -> JsNumber(2))
    )
    val result = RPrinter(arr)
    assert(result.contains("list2env"))
  }

  test("rDefaultRender with JsonFormat for String") {
    val result = RWrappableParam.rDefaultRender("test")
    assert(result === "\"test\"")
  }

  test("rDefaultRender with JsonFormat for Int") {
    val result = RWrappableParam.rDefaultRender(42)
    assert(result === "42L")
  }

  test("rDefaultRender with JsonFormat for Boolean true") {
    val result = RWrappableParam.rDefaultRender(true)
    assert(result === "TRUE")
  }

  test("rDefaultRender with JsonFormat for Boolean false") {
    val result = RWrappableParam.rDefaultRender(false)
    assert(result === "FALSE")
  }

  test("rDefaultRender with custom jsonFunc") {
    val result = RWrappableParam.rDefaultRender(
      List(1, 2, 3),
      (v: List[Int]) => v.toJson.compactPrint
    )
    assert(result === "list(1L,2L,3L)")
  }

  // Test RWrappableParam trait implementation
  private class TestRParam(parent: Params, override val name: String, doc: String)
    extends org.apache.spark.ml.param.Param[String](parent, name, doc)
      with RWrappableParam[String]

  private class TestParams extends Params {
    override val uid: String = "test-uid"
    val stringParam = new TestRParam(this, "testString", "A test string param")
    override def copy(extra: ParamMap): Params = this
  }

  test("RWrappableParam.rValue renders value correctly") {
    val params = new TestParams
    val result = params.stringParam.rValue("hello")
    assert(result === "\"hello\"")
  }

  test("RWrappableParam.rName returns param name") {
    val params = new TestParams
    val result = params.stringParam.rName("anyValue")
    assert(result === "testString")
  }

  test("RWrappableParam.rConstructorLine generates correct format") {
    val params = new TestParams
    val result = params.stringParam.rConstructorLine("world")
    assert(result === "testString=\"world\"")
  }

  test("RWrappableParam.rSetterLine generates correct format") {
    val params = new TestParams
    val result = params.stringParam.rSetterLine("value")
    assert(result === "setTestString(\"value\")")
  }
}
