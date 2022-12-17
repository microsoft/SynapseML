// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import org.apache.spark.ml.param.{ParamPair, Params}
import spray.json.DefaultJsonProtocol._
import spray.json.JsonFormat

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._


abstract class TypedArrayParam[T: TypeTag](parent: Params,
                         name: String,
                         doc: String,
                         isValid: Seq[T] => Boolean = (_: Seq[T]) => true)
                        (@transient implicit val dataFormat: JsonFormat[T])
  extends JsonEncodableParam[Seq[T]](parent, name, doc, isValid)
    with WrappableParam[Seq[T]] {
  type ValueType = T

  def w(v: java.util.ArrayList[T]): ParamPair[Seq[T]] = w(v.asScala.toSeq)

  // TODO: Implement render for this
  override private[ml] def dotnetTestValue(v: Seq[T]): String = {
    throw new NotImplementedError(s"No translation found for this TypedArrayParam: $v")
  }

  override private[ml] def dotnetTestSetterLine(v: Seq[T]): String = {
    typeOf[T].toString match {
      case t if t == "com.microsoft.azure.synapse.ml.explainers.ICECategoricalFeature" =>
        s"""Set${dotnetName(v).capitalize}(new ICECategoricalFeature[]{${dotnetTestValue(v)}})"""
      case t if t == "com.microsoft.azure.synapse.ml.explainers.ICENumericFeature" =>
        s"""Set${dotnetName(v).capitalize}(new ICENumericFeature[]{${dotnetTestValue(v)}})"""
      case _ => throw new NotImplementedError(s"No translation found for this TypedArrayParam: $v")
    }
  }

  override def rValue(v: Seq[T]): String = {
    implicit val defaultFormat = seqFormat[T]
    RWrappableParam.rDefaultRender(v)(format)
  }

  override def rConstructorLine(v: Seq[T]): String = {
    if (v.isEmpty) {
      s"${rName(v)}=c()"
    } else {
      val className = typeOf[T].toString
      s"""${rName(v)}=list(${v.map(arg => {
        className match {
          case "com.microsoft.azure.synapse.ml.explainers.ICECategoricalFeature" =>
            s"""invoke_static(sc, "${className}", "fromMap", ${RWrappableParam.rDefaultRender(arg)})"""
          case "com.microsoft.azure.synapse.ml.explainers.ICENumericFeature" =>
            s"""invoke_static(sc, "${className}", "fromMap", ${RWrappableParam.rDefaultRender(arg)})"""
          case _ =>
            s"""invoke_new(sc, "${className}", ${rValue(v)})"""
        }
      }).mkString(",")})"""
    }
  }
}

class TypedIntArrayParam(parent: Params,
                         name: String,
                         doc: String,
                         isValid: Seq[Int] => Boolean = (_: Seq[Int]) => true)
  extends JsonEncodableParam[Seq[Int]](parent, name, doc, isValid) with WrappableParam[Seq[Int]] {
  type ValueType = Int

  def w(v: java.util.ArrayList[Int]): ParamPair[Seq[Int]] = w(v.asScala.toSeq)

  private[ml] def dotnetType: String = "int[]"

  override private[ml] def dotnetGetter(capName: String): String = {
    s"""|public $dotnetReturnType Get$capName()
        |{
        |    JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke(\"get$capName\");
        |    return ($dotnetReturnType)jvmObject.Invoke(\"array\");
        |}
        |""".stripMargin
  }

  private[ml] def dotnetTestValue(v: Seq[Int]): String =
    s"""new $dotnetType
       |    ${DotnetWrappableParam.dotnetDefaultRender(v, this)}""".stripMargin

}

class TypedDoubleArrayParam(parent: Params,
                            name: String,
                            doc: String,
                            isValid: Seq[Double] => Boolean = (_: Seq[Double]) => true)
  extends JsonEncodableParam[Seq[Double]](parent, name, doc, isValid) with WrappableParam[Seq[Double]] {
  type ValueType = Double

  def w(v: java.util.ArrayList[Double]): ParamPair[Seq[Double]] = w(v.asScala.toSeq)

  private[ml] def dotnetType: String = "double[]"

  override private[ml] def dotnetGetter(capName: String): String = {
    s"""|public $dotnetReturnType Get$capName()
        |{
        |    JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke(\"get$capName\");
        |    return ($dotnetReturnType)jvmObject.Invoke(\"array\");
        |}
        |""".stripMargin
  }

  private[ml] def dotnetTestValue(v: Seq[Double]): String =
    s"""new $dotnetType
       |    ${DotnetWrappableParam.dotnetDefaultRender(v, this)}""".stripMargin
}
