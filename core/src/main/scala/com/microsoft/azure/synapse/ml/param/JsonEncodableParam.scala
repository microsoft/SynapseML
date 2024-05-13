// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import org.apache.spark.ml.param.{Param, Params}
import spray.json.{JsonFormat, _}

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

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
