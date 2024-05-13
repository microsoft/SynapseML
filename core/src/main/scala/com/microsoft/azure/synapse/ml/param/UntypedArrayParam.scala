// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.param.{Param, ParamPair, Params}
import spray.json.{DefaultJsonProtocol, JsValue, JsonFormat, _}

import scala.collection.JavaConverters._

object AnyJsonFormat extends DefaultJsonProtocol {

  //scalastyle:off cyclomatic.complexity
  implicit def anyFormat: JsonFormat[Any] = {
    def throwFailure(any: Any) = throw new IllegalArgumentException(s"Cannot serialize ${any} of type ${any.getClass}")

    new JsonFormat[Any] {
      def write(any: Any): JsValue = any match {
        case v: Int => v.toJson
        case v: Double => v.toJson
        case v: String => v.toJson
        case v: Boolean => v.toJson
        case v: Integer => v.toLong.toJson
        case v: Seq[_] => seqFormat[Any].write(v)
        case v: Map[_, _] => {
          try {
            mapFormat[String, Any].write(v.asInstanceOf[Map[String, Any]])
          } catch {
            case _: Throwable => throwFailure(any)
          }
        }
        case _ => throwFailure(any)
      }

      def read(value: JsValue): Any = value match {
        case v: JsNumber =>
          val num = v.value
          num match {
            case _ if num.isValidInt => num.toInt
            case _ if num.isValidLong => num.toLong
            case _ if num.isExactDouble || num.isBinaryDouble || num.isDecimalDouble => num.toDouble
            case _ => num
          }
        case v: JsString => v.value
        case v: JsBoolean => v.value
        case v: JsArray => listFormat[Any].read(value)
        case v: JsObject => mapFormat[String, Any].read(value)
        case _ => throw new IllegalArgumentException(s"Cannot deserialize ${value}")
      }
    }
  }
  //scalastyle:on cyclomatic.complexity
}

/** :: DeveloperApi ::
  * Specialized generic version of `Param[Array[_]]` for Java.
  */
@DeveloperApi
class UntypedArrayParam(parent: Params, name: String, doc: String, isValid: Array[Any] => Boolean)
  extends Param[Array[Any]](parent, name, doc, isValid) with WrappableParam[Array[Any]] {
    import AnyJsonFormat._

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, (_: Array[Any]) => true)

  def w(value: java.util.ArrayList[_]): ParamPair[Array[Any]] =
    w(value.asScala.toArray.asInstanceOf[Array[Any]])

  def w(value: java.util.List[_]): ParamPair[Array[Any]] =
    w(value.asScala.toArray.asInstanceOf[Array[Any]])

  override def jsonEncode(value: Array[Any]): String = {
    value.toJson.compactPrint
  }

  override def jsonDecode(json: String): Array[Any] = {
    json.parseJson.convertTo[Array[Any]]
  }

}
