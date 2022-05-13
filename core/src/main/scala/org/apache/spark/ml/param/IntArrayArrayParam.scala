// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import scala.collection.JavaConverters._
import org.json4s._
import org.json4s.jackson.JsonMethods._

/*object IntArrayArrayJsonProtocol extends DefaultJsonProtocol {

  implicit object MapJsonFormat extends JsonFormat[Map[String, Any]] {
    def write(m: Map[String, Any]): JsValue = {
      JsObject(m.mapValues {
        case v: Int => JsNumber(v)
        case v: Double => JsNumber(v)
        case v: String => JsString(v)
        case true => JsTrue
        case false => JsFalse
        case v: Map[_, _] => write(v.asInstanceOf[Map[String, Any]])
        case default => serializationError(s"Unable to serialize $default")
      })
    }

    def read(value: JsValue): Map[String, Any] = value.asInstanceOf[JsObject].fields.map(kvp => {
      val convValue = kvp._2 match {
        case JsNumber(n) => if (n.isValidInt) n.intValue().asInstanceOf[Any] else n.toDouble.asInstanceOf[Any]
        case JsString(s) => s
        case JsTrue => true
        case JsFalse => false
        case v: JsValue => read(v)
        case default => deserializationError(s"Unable to deserialize $default")
      }
      (kvp._1, convValue)
    })
  }

} */

private[param] object IntParamObject {
  /** Encodes a param value into JValue. */
  def jValueEncode(value: Int): JValue = {
      JInt(value)
  }

  /** Decodes a param value from JValue. */
  def jValueDecode(jValue: JValue): Int = {
    jValue match {
      case JInt(x) =>
        x.intValue
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $jValue to Int.")
    }
  }
}

/* Param for a Nested Array of Ints */
class IntArrayArrayParam(
    parent: Params,
    name: String,
    doc: String,
    isValid: Array[Array[Int]] => Boolean)
  extends Param[Array[Array[Int]]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[java.util.List[java.lang.Integer]]): ParamPair[Array[Array[Int]]] =
    w(value.asScala.map(_.asScala.map(_.asInstanceOf[Int]).toArray).toArray)

  override def jsonEncode(value: Array[Array[Int]]): String = {
    import org.json4s.JsonDSL._
    compact(render(value.toSeq.map(_.toSeq.map(IntParamObject.jValueEncode))))
  }

  override def jsonDecode(json: String): Array[Array[Int]] = {
    parse(json) match {
      case JArray(values) =>
        values.map {
          case JArray(values) =>
            values.map(IntParamObject.jValueDecode).toArray
          case _ =>
            throw new IllegalArgumentException(s"Cannot decode $json to Array[Array[Int]].")
        }.toArray
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Array[Array[Int]].")
    }
  }
}