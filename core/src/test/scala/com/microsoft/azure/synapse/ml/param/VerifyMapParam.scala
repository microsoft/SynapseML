// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.Identifiable

class VerifyMapParam extends TestBase {

  private object TestParams extends Params {
    override val uid: String = Identifiable.randomUID("TestParams") // scalastyle:ignore field.name
    override def copy(extra: ParamMap): Params = this
  }

  test("StringStringMapParam jsonEncode/jsonDecode roundtrip") {
    val param = new StringStringMapParam(TestParams, "ssMap", "a string-string map param")
    val original = Map("key1" -> "value1", "key2" -> "value2")
    val encoded = param.jsonEncode(original)
    val decoded = param.jsonDecode(encoded)
    assert(decoded === original)
  }

  test("StringIntMapParam jsonEncode/jsonDecode roundtrip") {
    val param = new StringIntMapParam(TestParams, "siMap", "a string-int map param")
    val original = Map("a" -> 1, "b" -> 2, "c" -> 3)
    val encoded = param.jsonEncode(original)
    val decoded = param.jsonDecode(encoded)
    assert(decoded === original)
  }

  test("MapParam with empty map roundtrip") {
    val param = new StringStringMapParam(TestParams, "emptyMap", "an empty map param")
    val original = Map.empty[String, String]
    val encoded = param.jsonEncode(original)
    val decoded = param.jsonDecode(encoded)
    assert(decoded === original)
  }

  test("Java HashMap conversion via w() method") {
    val param = new StringStringMapParam(TestParams, "javaMap", "a java map param")
    val javaMap = new java.util.HashMap[String, String]()
    javaMap.put("hello", "world")
    javaMap.put("foo", "bar")
    val paramPair = param.w(javaMap)
    assert(paramPair.param === param)
    assert(paramPair.value === Map("hello" -> "world", "foo" -> "bar"))
  }

  test("jsonEncode produces valid JSON string") {
    val param = new StringIntMapParam(TestParams, "jsonMap", "a json map param")
    val original = Map("x" -> 10, "y" -> 20)
    val encoded = param.jsonEncode(original)
    val parsed = spray.json.JsonParser(encoded)
    assert(parsed.isInstanceOf[spray.json.JsObject])
    val fields = parsed.asJsObject.fields
    assert(fields("x") === spray.json.JsNumber(10))
    assert(fields("y") === spray.json.JsNumber(20))
  }

  test("jsonDecode parses JSON back to correct Map") {
    val param = new StringStringMapParam(TestParams, "decodeMap", "a decode map param")
    val json = """{"alpha":"one","beta":"two"}"""
    val result = param.jsonDecode(json)
    assert(result === Map("alpha" -> "one", "beta" -> "two"))
  }
}
