// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import spray.json._
import spray.json.JsonFormat
import scala.collection.JavaConverters._

object ServiceParamJsonProtocol extends DefaultJsonProtocol {
  override implicit def eitherFormat[A: JsonFormat, B: JsonFormat]: JsonFormat[Either[A, B]] =
    new JsonFormat[Either[A, B]] {
      def write(either: Either[A, B]): JsValue = either match {
        case Left(a) => JsObject.apply(("left", a.toJson))
        case Right(b) => JsObject.apply(("right", b.toJson))
      }

      def read(value: JsValue): Either[A, B] = value.asJsObject().fields.head match {
        case ("left", jv) => Left(jv.convertTo[A])
        case ("right", jv) => Right(jv.convertTo[B])
        case _ => throw new IllegalArgumentException("Could not parse either type")
      }
    }

}

class JsonEncodableParam[T](parent: Params, name: String, doc: String, isValid: T => Boolean)
                           (@transient implicit val format: JsonFormat[T])
  extends Param[T](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String)(implicit format: JsonFormat[T]) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  override def jsonEncode(value: T): String = {
    value.toJson.compactPrint
  }

  override def jsonDecode(json: String): T = {
    json.parseJson.convertTo[T]
  }

}


import ServiceParamJsonProtocol._

object ServiceParam {
  def toSeq[T](arr: java.util.ArrayList[T]): Seq[T] = arr.asScala.toSeq
}

class ServiceParam[T](parent: Params,
                      name: String,
                      doc: String,
                      isValid: Either[T, String] => Boolean = ParamValidators.alwaysTrue,
                      val isRequired: Boolean = false,
                      val isURLParam: Boolean = false,
                      val toValueString: T => String = { x: T => x.toString }
                     )
                     (@transient implicit val dataFormat: JsonFormat[T])
  extends JsonEncodableParam[Either[T, String]](parent, name, doc, isValid)
    with PythonWrappableParam[Either[T, String]] {
  type ValueType = T

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

}
