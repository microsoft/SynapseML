// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import org.apache.spark.ml.param.Param
import spray.json._

import java.lang.{StringBuilder => JStringBuilder}

trait PythonPrinter extends CompactPrinter {
  override protected def printLeaf(x: JsValue, sb: JStringBuilder): Unit = {
    x match {
      case JsNull      => sb.append("None")
      case JsTrue      => sb.append("True")
      case JsFalse     => sb.append("False")
      case JsNumber(x) => sb.append(x)
      case JsString(x) => printString(x, sb)
      case _           => throw new IllegalStateException
    }
  }
}

object PythonPrinter extends PythonPrinter

object PythonWrappableParam {

  def pyDefaultRender[T](value: T, jsonFunc: T => String): String = {
    PythonPrinter(jsonFunc(value).parseJson)
  }

  def pyDefaultRender[T](value: T)(implicit dataFormat: JsonFormat[T]): String = {
    pyDefaultRender(value, { v: T => v.toJson.compactPrint })
  }

  def pyDefaultRender[T](value: T, param: Param[T]): String = {
    pyDefaultRender(value, { v: T => param.jsonEncode(v) })
  }

}

trait PythonWrappableParam[T] extends Param[T] {

  val name: String

  type InnerType = T

  def pyValue(v: T): String = PythonWrappableParam.pyDefaultRender(v, this)

  def pyName(v: T): String = {
    name
  }

  def pyConstructorLine(v: T): String = {
    s"""${pyName(v)}=${pyValue(v)}"""
  }

  def pySetterLine(v: T): String = {
    s"""set${pyName(v).capitalize}(${pyValue(v)})"""
  }

}

trait ExternalPythonWrappableParam[T] extends PythonWrappableParam[T] {

  def pyLoadLine(modelNum: Int): String

}
