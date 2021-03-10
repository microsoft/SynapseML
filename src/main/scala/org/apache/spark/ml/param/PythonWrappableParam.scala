// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

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

  def defaultPythonize[T](value: T, jsonFunc: T => String): String = {
    PythonPrinter(jsonFunc(value).parseJson)
  }

  def defaultPythonize[T](value: T)(implicit dataFormat: JsonFormat[T]): String = {
    defaultPythonize(value, { v: T => v.toJson.compactPrint })
  }

  def defaultPythonize[T](value: T, param: Param[T]): String = {
    defaultPythonize(value, { v: T => param.jsonEncode(v) })
  }

}

trait PythonWrappableParam[T] extends Param[T] {

  val name: String

  type InnerType = T

  def valueToPython(v: T): String

  def pythonizedParamName(v: T): String = {
    name
  }

  def costructorBasedValue(v: T): String = {
    s"""${pythonizedParamName(v)}=${valueToPython(v)}"""
  }

  def setterBasedValue(v: T): String = {
    s"""set${pythonizedParamName(v).capitalize}(${valueToPython(v)})"""
  }

}

trait ExternalPythonWrappableParam[T] extends PythonWrappableParam[T] {

  def loadParameter(modelNum: Int): String

}
