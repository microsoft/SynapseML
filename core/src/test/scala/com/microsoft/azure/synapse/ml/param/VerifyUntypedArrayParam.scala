// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.param.AnyJsonFormat._
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import spray.json._

import scala.collection.immutable.ListMap

class VerifyUntypedArrayParam extends TestBase {

  private object TestParams extends Params {
    override val uid: String = Identifiable.randomUID("TestParams") // scalastyle:ignore field.name
    override def copy(extra: ParamMap): Params = this
  }

  test("AnyJsonFormat roundtrip for Int") {
    val original: Any = 42
    val json = original.toJson
    val result = json.convertTo[Any]
    assert(result === 42)
    assert(result.isInstanceOf[Int])
  }

  test("AnyJsonFormat roundtrip for Double") {
    val original: Any = 3.14
    val json = original.toJson
    val result = json.convertTo[Any]
    assert(result === 3.14)
    assert(result.isInstanceOf[Double])
  }

  test("AnyJsonFormat roundtrip for String") {
    val original: Any = "hello"
    val json = original.toJson
    val result = json.convertTo[Any]
    assert(result === "hello")
    assert(result.isInstanceOf[String])
  }

  test("AnyJsonFormat roundtrip for Boolean") {
    val original: Any = true
    val json = original.toJson
    val result = json.convertTo[Any]
    assert(result === true)
    assert(result.isInstanceOf[Boolean])
  }

  test("AnyJsonFormat roundtrip for Seq") {
    val original: Any = Seq(1, 2, 3)
    val json = original.toJson
    val result = json.convertTo[Any]
    assert(result === List(1, 2, 3))
  }

  test("AnyJsonFormat roundtrip for Map[String, Any]") {
    val original: Any = Map("key" -> "value", "num" -> 10)
    val json = original.toJson
    val result = json.convertTo[Any]
    val resultMap = result.asInstanceOf[Map[String, Any]]
    assert(resultMap("key") === "value")
    assert(resultMap("num") === 10)
  }

  test("UntypedArrayParam jsonEncode/jsonDecode roundtrip with mixed types") {
    val param = new UntypedArrayParam(TestParams, "testParam", "a test param")
    val original = Array[Any](1, 2.5, "text", true)
    val encoded = param.jsonEncode(original)
    val decoded = param.jsonDecode(encoded)
    assert(decoded.length === original.length)
    assert(decoded(0) === 1)
    assert(decoded(1) === 2.5)
    assert(decoded(2) === "text")
    assert(decoded(3) === true)
  }

  test("AnyJsonFormat throws on unsupported type") {
    class CustomClass
    val unsupported: Any = new CustomClass
    assertThrows[IllegalArgumentException] {
      unsupported.toJson
    }
  }

  test("AnyJsonFormat preserves existing compact renderings") {
    assert((42: Any).toJson.compactPrint === "42")
    assert((3.14: Any).toJson.compactPrint === "3.14")
    assert(("hello": Any).toJson.compactPrint === "\"hello\"")
    assert((true: Any).toJson.compactPrint === "true")
    assert((Seq(1, 2, 3): Any).toJson.compactPrint === "[1,2,3]")
    assert((ListMap("b" -> 1, "a" -> 2): Any).toJson.compactPrint === """{"b":1,"a":2}""")
  }

  test("AnyJsonFormat writes JsValue, null, Long and Float") {
    val jsValue: Any = JsArray(JsObject("type" -> JsString("function")))
    assert(jsValue.toJson.compactPrint === """[{"type":"function"}]""")
    assert((null: Any).toJson === JsNull) //scalastyle:ignore null
    assert(Map[String, Any]("strict" -> null).toJson.compactPrint === //scalastyle:ignore null
      """{"strict":null}""")
    assert((9007199254740993L: Any).toJson.compactPrint === "9007199254740993")
    assert((1.5f: Any).toJson.compactPrint === "1.5")
    assert((0.1f: Any).toJson.compactPrint === "0.1")
  }

  test("AnyJsonFormat rejects non-finite Float values clearly") {
    Seq(Float.NaN, Float.PositiveInfinity, Float.NegativeInfinity).foreach { value =>
      val error = intercept[IllegalArgumentException] {
        (value: Any).toJson
      }
      assert(error.getMessage === s"Cannot serialize non-finite Float value $value")
    }
  }

  test("AnyJsonFormat reads JsNull") {
    assert(JsNull.convertTo[Any] == null) //scalastyle:ignore null
  }

  test("UntypedArrayParam roundtrips widened AnyJsonFormat values") {
    val param = new UntypedArrayParam(TestParams, "widened", "widened values")
    val original = Array[Any](9007199254740993L, 0.1f, null) //scalastyle:ignore null
    val decoded = param.jsonDecode(param.jsonEncode(original))
    assert(decoded(0) === 9007199254740993L)
    assert(decoded(1) === 0.1)
    assert(decoded(2) == null) //scalastyle:ignore null
  }
}
