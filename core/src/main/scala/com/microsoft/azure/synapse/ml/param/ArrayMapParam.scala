// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import org.apache.spark.ml.param.{Param, ParamPair, Params}
import spray.json._

import scala.collection.JavaConverters._

object ArrayMapJsonProtocol extends DefaultJsonProtocol {

  implicit object MapJsonFormat extends JsonFormat[Map[String, Any]] {
    //scalastyle:off cyclomatic.complexity
    def write(m: Map[String, Any]): JsValue = {
      JsObject(m.mapValues {
        case v: Int => JsNumber(v)
        case v: Short => JsNumber(v)
        case v: Long => JsNumber(v)
        case v: BigInt => JsNumber(v)
        case v: Double => JsNumber(v)
        case v: String => JsString(v)
        case true => JsTrue
        case false => JsFalse
        case v: Map[_, _] => write(v.asInstanceOf[Map[String, Any]])
        case default => serializationError(s"Unable to serialize $default")
      })
    }
    //scalastyle:on cyclomatic.complexity

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
}

/** Param for Array of stage parameter maps. */
class ArrayMapParam(parent: String, name: String, doc: String, isValid: Array[Map[String, Any]] => Boolean)
  extends Param[Array[Map[String, Any]]](parent, name, doc, isValid) with WrappableParam[Array[Map[String, Any]]] {

  import ArrayMapJsonProtocol._

  def this(parent: Params, name: String, doc: String, isValid: Array[Map[String, Any]] => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, (_: Array[Map[String, Any]]) => true)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: Array[Map[String, Any]]): ParamPair[Array[Map[String, Any]]] = super.w(value)

  def w(value: java.util.ArrayList[java.util.HashMap[String, Any]]): ParamPair[Array[Map[String, Any]]] =
    super.w(value.asScala.toArray.map(_.asScala.toMap))

  override def jsonEncode(value: Array[Map[String, Any]]): String = {
    val json = value.toSeq.toJson
    json.compactPrint
  }

  override def jsonDecode(json: String): Array[Map[String, Any]] = {
    val jsonValue = json.parseJson
    jsonValue.convertTo[Seq[Map[String, Any]]].toArray
  }

  override def rValue(v: Array[Map[String, Any]]): String = {
    implicit val defaultFormat = seqFormat[Map[String, Any]]
    RWrappableParam.rDefaultRender(v)
  }

  // TODO: remove when Sparklyr serialization/deserialization supports lists of environments
  override def rConstructorLine(v: Array[Map[String, Any]]): String = {
    if (v.isEmpty) {
      s"${rName(v)}=c()"
    } else {
      s"${rName(v)}='${jsonEncode(v)}'"
    }
  }
}
