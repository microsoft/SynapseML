// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import org.apache.spark.ml.param.{Param, Params}
import spray.json.{JsonFormat, _}

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

object ServiceParamJsonProtocol extends DefaultJsonProtocol {
  // scalastyle:off cyclomatic.complexity
  override implicit def eitherFormat[A: JsonFormat, B: JsonFormat]: JsonFormat[Either[A, B]] =
    new JsonFormat[Either[A, B]] {
      def write(either: Either[A, B]): JsValue = {
        either.fold[JsValue](
          a => JsObject("left" -> a.toJson),
          b => JsObject("right" -> b.toJson)
        )
      }

      def read(value: JsValue): Either[A, B] = {
        val obj = value.asJsObject
        obj.fields.get("left").map(j => Left(j.convertTo[A]))
          .orElse(obj.fields.get("right").map(j => Right(j.convertTo[B])))
          .getOrElse(throw new IllegalArgumentException("Could not parse either type"))
      }
    }
  // scalastyle:on cyclomatic.complexity
}

class JsonEncodableParam[T](parent: Params, name: String, doc: String, isValid: T => Boolean)
                           (@transient implicit val format: JsonFormat[T])
  extends Param[T](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String)(implicit format: JsonFormat[T]) =
    this(parent, name, doc, (_: T) => true)

  override def jsonEncode(value: T): String = {
    value.toJson.compactPrint
  }

  override def jsonDecode(json: String): T = {
    json.parseJson.convertTo[T]
  }

}


import com.microsoft.azure.synapse.ml.param.ServiceParamJsonProtocol._

object ServiceParam {
  def toSeq[T](arr: java.util.ArrayList[T]): Seq[T] = arr.asScala.toSeq

  // Convert a Java Map/Collection structure into a deeply-converted Scala structure
  // so nested maps/lists are serializable by spray-json (used by ServiceParam JSON encoding).
  private def toScalaAny(value: Any): Any = value match {
    case m: java.util.Map[_, _] =>
      m.asScala.map { case (k, v) => k.toString -> toScalaAny(v) }.toMap
    case l: java.util.List[_] =>
      l.asScala.map(toScalaAny).toSeq
    case other => toScalaPrimitive(other)
  }

  private def toScalaPrimitive(value: Any): Any = value match {
    case b: java.lang.Boolean => b.booleanValue()
    case i: java.lang.Integer => i.intValue()
    case l: java.lang.Long => l.longValue()
    case d: java.lang.Double => d.doubleValue()
    case f: java.lang.Float => f.floatValue()
    case s: java.lang.Short => s.shortValue()
    case by: java.lang.Byte => by.byteValue()
    case other => other
  }

  def toMap(m: java.util.Map[String, Object]): Map[String, Any] =
    m.asScala.map { case (k, v) => k -> toScalaAny(v) }.toMap
}

class ServiceParam[T: TypeTag](parent: Params,
                               name: String,
                               doc: String,
                               isValid: Either[T, String] => Boolean = (_: Either[T, String]) => true,
                               val isRequired: Boolean = false,
                               val isURLParam: Boolean = false,
                               val toValueString: T => String = { x: T => x.toString }
                              )
                              (@transient implicit val dataFormat: JsonFormat[T])
  extends JsonEncodableParam[Either[T, String]](parent, name, doc, isValid)
    with WrappableParam[Either[T, String]] {

  type ValueType = T

  val payloadName: String = name

  override def pyValue(v: Either[T, String]): String = {
    v match {
      case Left(t) => PythonWrappableParam.pyDefaultRender(t)
      case Right(n) => s""""$n""""
    }
  }

  override def pyName(v: Either[T, String]): String = {
    v match {
      case Left(_) => name
      case Right(_) => name + "Col"
    }
  }

  override def rValue(v: Either[T, String]): String = {
    v match {
      case Left(t) => RWrappableParam.rDefaultRender(t)
      case Right(n) => s""""$n""""
    }
  }

  override def rName(v: Either[T, String]): String = {
    v match {
      case Left(_) => name
      case Right(_) => name + "Col"
    }
  }


}

// Use this class if you want to extend JsonEncodableParam for Cognitive services param
class CognitiveServiceStructParam[T: TypeTag](parent: Params,
                                              name: String,
                                              doc: String,
                                              isValid: T => Boolean = (_: T) => true)
                                             (@transient implicit val dataFormat: JsonFormat[T])
  extends JsonEncodableParam[T](parent, name, doc, isValid)
    with WrappableParam[T] {

  override def pyValue(v: T): String = PythonWrappableParam.pyDefaultRender(v)

  override def rValue(v: T): String = RWrappableParam.rDefaultRender(v)

}
