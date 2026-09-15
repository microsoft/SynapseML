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

  test("AnyJsonFormat preserves insertion order beyond the four-field Map boundary") {
    val names = Seq("e_one", "d_two", "c_three", "b_four", "a_five", "z_six")
    (2 to names.size).foreach { size =>
      val selected = names.take(size)
      val value: Any = ListMap(selected.map(_ -> 1): _*)
      val expected = selected.map(name => s""""$name":1""").mkString("{", ",", "}")
      assert(value.toJson.compactPrint == expected)
      val nested: Any = Map("properties" -> value)
      assert(nested.toJson.compactPrint == s"""{"properties":$expected}""")
    }
  }

  test("AnyJsonFormat reads and writes JSON null in nested collections") {
    val jsonNull = Option.empty[AnyRef].orNull
    assert(anyFormat.write(jsonNull) == JsNull)
    assert(Option(anyFormat.read(JsNull)).isEmpty)
    val original: Any = Map("default" -> jsonNull, "enum" -> Seq(jsonNull, "answer"))
    val json = original.toJson
    assert(json == """{"default":null,"enum":[null,"answer"]}""".parseJson)
    assert(json.convertTo[Any] == original)
  }

  test("AnyJsonFormat supports wide integers and all converted numeric primitives") {
    val values = Seq[Any](
      Long.MinValue, Long.MaxValue, Byte.MinValue, Byte.MaxValue, Short.MinValue, Short.MaxValue,
      1.25f
    )
    values.foreach { value =>
      withClue(s"value $value: ") {
        val json = anyFormat.write(value)
        assert(json == JsNumber(BigDecimal(value.toString)))
        assert(anyFormat.write(anyFormat.read(json)) == json)
      }
    }
  }

  test("AnyJsonFormat rejects non-string object keys and unsupported nested values") {
    val invalid: Any = Map(1 -> "value")
    intercept[IllegalArgumentException] {
      invalid.toJson
    }
    val nested: Any = Map("value" -> new Object())
    val error = intercept[IllegalArgumentException] {
      nested.toJson
    }
    assert(error.getMessage.contains("Cannot serialize"))
  }

  test("UntypedArrayParam roundtrips nulls and wide numeric schema values") {
    val param = new UntypedArrayParam(TestParams, "schemaValues", "JSON Schema values")
    val original = Array[Any](Option.empty[AnyRef].orNull, Long.MaxValue,
      Map("default" -> Option.empty[AnyRef].orNull, "maximum" -> 3000000000L))
    assert(param.jsonDecode(param.jsonEncode(original)).toSeq == original.toSeq)
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
    val values = Seq[Any](new CustomClass,
      BigInt("9223372036854775808"), BigDecimal("0.12345678901234567890123456789"))
    values.foreach { unsupported =>
      assertThrows[IllegalArgumentException] {
        unsupported.toJson
      }
    }
  }
}
