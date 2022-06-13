// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import org.apache.spark.annotation.DeveloperApi
import spray.json.{DefaultJsonProtocol, JsValue, JsonFormat, _}

import scala.collection.JavaConverters._

object AnyJsonFormat extends DefaultJsonProtocol {
  implicit def anyFormat: JsonFormat[Any] =
    new JsonFormat[Any] {
      def write(any: Any): JsValue = any match {
        case v: Int => v.toJson
        case v: Double => v.toJson
        case v: String => v.toJson
        case v: Boolean => v.toJson
        case v: Integer => v.toLong.toJson
        case _ => throw new IllegalArgumentException(s"Cannot serialize ${any} of type ${any.getClass}")
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
        case _ => throw new IllegalArgumentException(s"Cannot deserialize ${value}")
      }
    }

}

/** :: DeveloperApi ::
  * Specialized generic version of `Param[Array[_]]` for Java.
  */
@DeveloperApi
class UntypedArrayParam(parent: Params, name: String, doc: String, isValid: Array[Any] => Boolean)
  extends Param[Array[Any]](parent, name, doc, isValid) with WrappableParam[Array[Any]] {
    import AnyJsonFormat._

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

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

  def dotnetType: String = "object[]"

  override def dotnetSetter(dotnetClassName: String, capName: String, dotnetClassWrapperName: String): String = {
    s"""|public $dotnetClassName Set$capName($dotnetType value)
        |    => $dotnetClassWrapperName(Reference.Invoke(\"set$capName\", (object)value.ToJavaArrayList()));
        |""".stripMargin
  }

  override def dotnetGetter(capName: String): String = {
    s"""|public $dotnetReturnType Get$capName()
        |{
        |    var jvmObjects = (JvmObjectReference[])Reference.Invoke(\"get$capName\");
        |    var result = new object[jvmObjects.Length];
        |    for (int i = 0; i < result.Length; i++)
        |    {
        |        result[i] = SparkEnvironment.JvmBridge.CallStaticJavaMethod(
        |            "org.apache.spark.api.dotnet.DotnetUtils", "mapScalaToJava", (object)jvmObjects[i]);
        |    }
        |    return result;
        |}
        |""".stripMargin
  }

  def dotnetTestValue(v: Array[Any]): String =
    s"""new $dotnetType
       |    ${DotnetWrappableParam.dotnetDefaultRender(v, this)}""".stripMargin
}
