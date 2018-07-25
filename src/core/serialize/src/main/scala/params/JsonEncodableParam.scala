// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import java.lang.IllegalArgumentException

import spray.json._
import spray.json.JsonFormat

object SafeDefaultJsonProtocol extends DefaultJsonProtocol {
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

import SafeDefaultJsonProtocol._

class VectorizableParam[T](parent: Params, name: String, doc: String, isValid: Either[T, String] => Boolean)
                          (@transient implicit val dataFormat: JsonFormat[T])
  extends JsonEncodableParam[Either[T, String]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String)(implicit format: JsonFormat[T]) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

}
