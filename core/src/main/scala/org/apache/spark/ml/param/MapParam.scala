// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.JavaConverters._

/** Param for Map of String to Seq of String. */
class MapParam[K, V](parent: Params, name: String, doc: String, isValid: Map[K, V] => Boolean)
                    (@transient implicit val fk: JsonFormat[K], @transient implicit val fv: JsonFormat[V])
  extends Param[Map[K, V]](parent, name, doc, isValid) with CollectionFormats
    with WrappableParam[Map[K, V]] {

  def this(parent: Params, name: String, doc: String)(implicit fk: JsonFormat[K], fv: JsonFormat[V]) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with the given value (for Java). */
  def w(value: java.util.HashMap[K, V]): ParamPair[Map[K, V]] = {
    this.->(value.asScala.toMap)
  }

  override def jsonEncode(value: Map[K, V]): String = {
    value.toJson.compactPrint
  }

  override def jsonDecode(json: String): Map[K, V] = {
    json.parseJson.convertTo[Map[K, V]]
  }

  def dotnetType: String = "Dictionary<object, object>"

  override def dotnetSetter(dotnetClassName: String, capName: String, dotnetClassWrapperName: String): String = {
    s"""|public $dotnetClassName Set$capName($dotnetType value) =>
        |    $dotnetClassWrapperName(Reference.Invoke(\"set$capName\", (object)value.ToJavaHashMap()));
        |""".stripMargin
  }

  protected def valuesType = "object"

  override def dotnetGetter(capName: String): String = {
    s"""|public $dotnetReturnType Get$capName()
        |{
        |    var jvmObject = (JvmObjectReference)Reference.Invoke(\"get$capName\");
        |    var hashMap = (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
        |        "org.apache.spark.api.dotnet.DotnetUtils", "convertToJavaMap", jvmObject);
        |    var keySet = (JvmObjectReference[])(
        |        (JvmObjectReference)hashMap.Invoke("keySet")).Invoke("toArray");
        |    var result = new $dotnetReturnType();
        |    foreach (var k in keySet)
        |    {
        |        result.Add((string)k.Invoke("toString"), ($valuesType)hashMap.Invoke("get", k));
        |    }
        |    return result;
        |}
        |""".stripMargin
  }

  def dotnetTestValue(v: Map[K, V]): String =
    s""".Set${this.name.capitalize}(new $dotnetType
       |    ${DotnetWrappableParam.dotnetDefaultRender(v, this)})""".stripMargin

}

class StringStringMapParam(parent: Params, name: String, doc: String, isValid: Map[String, String] => Boolean)
  extends MapParam[String, String](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  override def dotnetType: String = "Dictionary<string, string>"

  override protected def valuesType = "string"

}

class StringIntMapParam(parent: Params, name: String, doc: String, isValid: Map[String, Int] => Boolean)
  extends MapParam[String, Int](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  override def dotnetType: String = "Dictionary<string, int>"

  override protected def valuesType = "int"

}
