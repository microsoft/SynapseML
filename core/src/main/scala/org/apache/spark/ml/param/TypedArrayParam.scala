// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import spray.json.DefaultJsonProtocol._
import spray.json.JsonFormat

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._


class TypedArrayParam[T: TypeTag](parent: Params,
                         name: String,
                         doc: String,
                         isValid: Seq[T] => Boolean = ParamValidators.alwaysTrue)
                        (@transient implicit val dataFormat: JsonFormat[T])
  extends JsonEncodableParam[Seq[T]](parent, name, doc, isValid) with DotnetWrappableParam[Seq[T]] {
  type ValueType = T

  def w(v: java.util.ArrayList[T]): ParamPair[Seq[T]] = w(v.asScala)

  // TODO: Implement render for this
  override def dotnetTestValue(v: Seq[T]): String = {
    throw new NotImplementedError(s"No translation found for this TypedArrayParame: $v")
  }

  override def dotnetTestSetterLine(v: Seq[T]): String = {
    typeOf[T].toString match {
      case t if t == "Seq[com.microsoft.azure.synapse.ml.explainers.ICECategoricalFeature]" =>
        s"""Set${dotnetName(v).capitalize}(new ICECategoricalFeature[]{${dotnetTestValue(v)}})"""
      case t if t == "Seq[com.microsoft.azure.synapse.ml.explainers.ICENumericFeature]" =>
        s"""Set${dotnetName(v).capitalize}(new ICENumericFeature[]{${dotnetTestValue(v)}})"""
      case _ => throw new NotImplementedError(s"No translation found for this TypedArrayParame: $v")
    }
  }

}

class TypedIntArrayParam(parent: Params,
                         name: String,
                         doc: String,
                         isValid: Seq[Int] => Boolean = ParamValidators.alwaysTrue)
  extends JsonEncodableParam[Seq[Int]](parent, name, doc, isValid) with WrappableParam[Seq[Int]] {
  type ValueType = Int

  def w(v: java.util.ArrayList[Int]): ParamPair[Seq[Int]] = w(v.asScala)

  def dotnetType: String = "int[]"

  override def dotnetGetter(capName: String): String = {
    s"""|public $dotnetReturnType Get$capName()
        |{
        |    JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke(\"get$capName\");
        |    return ($dotnetReturnType)jvmObject.Invoke(\"array\");
        |}
        |""".stripMargin
  }

  def dotnetTestValue(v: Seq[Int]): String =
    s"""new $dotnetType
       |    ${DotnetWrappableParam.dotnetDefaultRender(v, this)}""".stripMargin

}

class TypedDoubleArrayParam(parent: Params,
                            name: String,
                            doc: String,
                            isValid: Seq[Double] => Boolean = ParamValidators.alwaysTrue)
  extends JsonEncodableParam[Seq[Double]](parent, name, doc, isValid) with WrappableParam[Seq[Double]] {
  type ValueType = Double

  def w(v: java.util.ArrayList[Double]): ParamPair[Seq[Double]] = w(v.asScala)

  def dotnetType: String = "double[]"

  override def dotnetGetter(capName: String): String = {
    s"""|public $dotnetReturnType Get$capName()
        |{
        |    JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke(\"get$capName\");
        |    return ($dotnetReturnType)jvmObject.Invoke(\"array\");
        |}
        |""".stripMargin
  }

  def dotnetTestValue(v: Seq[Double]): String =
    s"""new $dotnetType
       |    ${DotnetWrappableParam.dotnetDefaultRender(v, this)}""".stripMargin
}
