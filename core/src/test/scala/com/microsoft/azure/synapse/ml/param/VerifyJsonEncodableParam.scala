// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.collection.JavaConverters._

class VerifyJsonEncodableParam extends TestBase {

  private object TestParams extends Params {
    override val uid: String = Identifiable.randomUID("TestParams") // scalastyle:ignore field.name
    override def copy(extra: ParamMap): Params = this
  }

  test("JsonEncodableParam jsonEncode/jsonDecode roundtrip for Int") {
    val param = new JsonEncodableParam[Int](TestParams, "intParam", "an int param")
    val original = 42
    val encoded = param.jsonEncode(original)
    val decoded = param.jsonDecode(encoded)
    assert(decoded === original)
  }

  test("JsonEncodableParam jsonEncode/jsonDecode roundtrip for String") {
    val param = new JsonEncodableParam[String](TestParams, "stringParam", "a string param")
    val original = "hello world"
    val encoded = param.jsonEncode(original)
    val decoded = param.jsonDecode(encoded)
    assert(decoded === original)
  }

  test("JsonEncodableParam jsonEncode/jsonDecode roundtrip for Seq[Int]") {
    implicit val seqFormat: JsonFormat[Seq[Int]] =
      immSeqFormat[Int].asInstanceOf[JsonFormat[Seq[Int]]]
    val param = new JsonEncodableParam[Seq[Int]](TestParams, "seqParam", "a seq param")
    val original = Seq(1, 2, 3)
    val encoded = param.jsonEncode(original)
    val decoded = param.jsonDecode(encoded)
    assert(decoded === original)
  }

  test("ServiceParamJsonProtocol eitherFormat encodes Left correctly") {
    import com.microsoft.azure.synapse.ml.param.ServiceParamJsonProtocol._
    val value: Either[Int, String] = Left(10)
    val json = value.toJson
    assert(json === JsObject("left" -> JsNumber(10)))
  }

  test("ServiceParamJsonProtocol eitherFormat encodes Right correctly") {
    import com.microsoft.azure.synapse.ml.param.ServiceParamJsonProtocol._
    val value: Either[Int, String] = Right("abc")
    val json = value.toJson
    assert(json === JsObject("right" -> JsString("abc")))
  }

  test("ServiceParamJsonProtocol eitherFormat roundtrip Either[Int, String]") {
    import com.microsoft.azure.synapse.ml.param.ServiceParamJsonProtocol._
    val left: Either[Int, String] = Left(5)
    val right: Either[Int, String] = Right("test")
    assert(left.toJson.convertTo[Either[Int, String]] === left)
    assert(right.toJson.convertTo[Either[Int, String]] === right)
  }

  test("ServiceParamJsonProtocol eitherFormat throws on invalid JSON") {
    import com.microsoft.azure.synapse.ml.param.ServiceParamJsonProtocol._
    val invalid = JsObject("other" -> JsNumber(1))
    assertThrows[IllegalArgumentException] {
      invalid.convertTo[Either[Int, String]]
    }
  }

  test("ServiceParam.toSeq converts Java ArrayList to Scala Seq") {
    val javaList = new java.util.ArrayList[Int]()
    javaList.add(1)
    javaList.add(2)
    javaList.add(3)
    val result = ServiceParam.toSeq(javaList)
    assert(result === Seq(1, 2, 3))
  }

  test("ServiceParam.toMap converts nested Java Map to Scala Map with ListMap ordering") {
    val inner = new java.util.LinkedHashMap[String, Object]()
    inner.put("b", java.lang.Integer.valueOf(2))
    inner.put("a", java.lang.Integer.valueOf(1))
    val outer = new java.util.LinkedHashMap[String, Object]()
    outer.put("nested", inner)
    outer.put("key", "value")
    val result = ServiceParam.toMap(outer)
    assert(result("key") === "value")
    val nested = result("nested").asInstanceOf[Map[String, Any]]
    assert(nested("b") === 2)
    assert(nested("a") === 1)
    assert(nested.keys.toSeq === Seq("b", "a"))
  }

  test("ServiceParam.toMap handles nested Java List") {
    val javaList = new java.util.ArrayList[Object]()
    javaList.add(java.lang.Integer.valueOf(1))
    javaList.add(java.lang.Integer.valueOf(2))
    val outer = new java.util.LinkedHashMap[String, Object]()
    outer.put("list", javaList)
    val result = ServiceParam.toMap(outer)
    assert(result("list") === Seq(1, 2))
  }
}
