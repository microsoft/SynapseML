// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.param.{ParamMap, Params}
import spray.json._
import spray.json.DefaultJsonProtocol._

class VerifyPythonWrappableParam extends TestBase {

  test("PythonPrinter converts JsNull to Python None") {
    val result = PythonPrinter(JsNull)
    assert(result === "None")
  }

  test("PythonPrinter converts JsTrue to Python True") {
    val result = PythonPrinter(JsTrue)
    assert(result === "True")
  }

  test("PythonPrinter converts JsFalse to Python False") {
    val result = PythonPrinter(JsFalse)
    assert(result === "False")
  }

  test("PythonPrinter converts JsNumber correctly") {
    assert(PythonPrinter(JsNumber(42)) === "42")
    assert(PythonPrinter(JsNumber(3.14)) === "3.14")
    assert(PythonPrinter(JsNumber(-100)) === "-100")
  }

  test("PythonPrinter converts JsString correctly") {
    val result = PythonPrinter(JsString("hello"))
    assert(result === "\"hello\"")
  }

  test("PythonPrinter converts JsArray correctly") {
    val arr = JsArray(JsNumber(1), JsNumber(2), JsNumber(3))
    val result = PythonPrinter(arr)
    assert(result === "[1,2,3]")
  }

  test("PythonPrinter converts JsObject correctly") {
    val obj = JsObject("key" -> JsString("value"))
    val result = PythonPrinter(obj)
    assert(result.contains("key"))
    assert(result.contains("value"))
  }

  test("PythonPrinter converts nested structures") {
    val nested = JsObject(
      "bool" -> JsTrue,
      "null" -> JsNull,
      "number" -> JsNumber(42)
    )
    val result = PythonPrinter(nested)
    assert(result.contains("True"))
    assert(result.contains("None"))
    assert(result.contains("42"))
  }

  test("pyDefaultRender with JsonFormat") {
    val result = PythonWrappableParam.pyDefaultRender("test")
    assert(result === "\"test\"")
  }

  test("pyDefaultRender with Int") {
    val result = PythonWrappableParam.pyDefaultRender(42)
    assert(result === "42")
  }

  test("pyDefaultRender with Boolean true") {
    val result = PythonWrappableParam.pyDefaultRender(true)
    assert(result === "True")
  }

  test("pyDefaultRender with Boolean false") {
    val result = PythonWrappableParam.pyDefaultRender(false)
    assert(result === "False")
  }

  test("pyDefaultRender with custom jsonFunc") {
    val result = PythonWrappableParam.pyDefaultRender(
      List(1, 2, 3),
      (v: List[Int]) => v.toJson.compactPrint
    )
    assert(result === "[1,2,3]")
  }

  // Test PythonWrappableParam trait implementation
  private class TestPythonParam(parent: Params, override val name: String, doc: String)
    extends org.apache.spark.ml.param.Param[String](parent, name, doc)
      with PythonWrappableParam[String]

  private class TestParams extends Params {
    override val uid: String = "test-uid"
    val stringParam = new TestPythonParam(this, "testString", "A test string param")
    override def copy(extra: ParamMap): Params = this
  }

  test("PythonWrappableParam.pyValue renders value correctly") {
    val params = new TestParams
    val result = params.stringParam.pyValue("hello")
    assert(result === "\"hello\"")
  }

  test("PythonWrappableParam.pyName returns param name") {
    val params = new TestParams
    val result = params.stringParam.pyName("anyValue")
    assert(result === "testString")
  }

  test("PythonWrappableParam.pyConstructorLine generates correct format") {
    val params = new TestParams
    val result = params.stringParam.pyConstructorLine("world")
    assert(result === "testString=\"world\"")
  }

  test("PythonWrappableParam.pySetterLine generates correct format") {
    val params = new TestParams
    val result = params.stringParam.pySetterLine("value")
    assert(result === "setTestString(\"value\")")
  }
}
