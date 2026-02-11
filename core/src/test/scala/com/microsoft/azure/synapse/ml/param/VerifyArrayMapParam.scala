// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import spray.json._
import ArrayMapJsonProtocol._

class VerifyArrayMapParam extends TestBase {

  private object TestParams extends Params {
    override val uid: String = Identifiable.randomUID("TestParams")
    override def copy(extra: ParamMap): Params = this
  }

  test("MapJsonFormat write/read roundtrip with Int values") {
    val original = Map("a" -> 1, "b" -> 2, "c" -> 3)
    val json = original.toJson
    val decoded = json.convertTo[Map[String, Any]]
    assert(decoded === original)
  }

  test("MapJsonFormat write/read roundtrip with String values") {
    val original: Map[String, Any] = Map("key1" -> "value1", "key2" -> "value2")
    val json = original.toJson
    val decoded = json.convertTo[Map[String, Any]]
    assert(decoded === original)
  }

  test("MapJsonFormat write/read roundtrip with Boolean values") {
    val original: Map[String, Any] = Map("flag1" -> true, "flag2" -> false)
    val json = original.toJson
    val decoded = json.convertTo[Map[String, Any]]
    assert(decoded === original)
  }

  test("MapJsonFormat write/read roundtrip with nested Map") {
    val original: Map[String, Any] = Map("outer" -> Map("inner" -> 42))
    val json = original.toJson
    val decoded = json.convertTo[Map[String, Any]]
    assert(decoded === original)
  }

  test("MapJsonFormat write/read roundtrip with mixed types") {
    val original: Map[String, Any] = Map("name" -> "test", "count" -> 5, "enabled" -> true)
    val json = original.toJson
    val decoded = json.convertTo[Map[String, Any]]
    assert(decoded === original)
  }

  test("ArrayMapParam jsonEncode/jsonDecode roundtrip with array of maps") {
    val param = new ArrayMapParam(TestParams, "testParam", "doc")
    val original = Array(
      Map[String, Any]("a" -> 1, "b" -> "hello"),
      Map[String, Any]("c" -> true, "d" -> 2)
    )
    val encoded = param.jsonEncode(original)
    val decoded = param.jsonDecode(encoded)
    assert(decoded.length === original.length)
    assert(decoded.zip(original).forall { case (d, o) => d == o })
  }

  test("ArrayMapParam jsonEncode/jsonDecode with empty array") {
    val param = new ArrayMapParam(TestParams, "testParam", "doc")
    val original = Array.empty[Map[String, Any]]
    val encoded = param.jsonEncode(original)
    val decoded = param.jsonDecode(encoded)
    assert(decoded.isEmpty)
  }
}
