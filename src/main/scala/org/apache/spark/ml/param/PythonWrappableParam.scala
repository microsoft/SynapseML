// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import spray.json._

object PythonWrappableParam {

  def defaultPythonize[T](value: T, jsonFunc: T => String): String = {
    value match {
      case v: String =>
        s""""$v""""
      case _: Double | _: Int | _: Long =>
        s"""${value.toString}"""
      case v: Boolean =>
        s"""${v.toString.capitalize}"""
      case v =>
        s"""json.loads('${jsonFunc(v)}')"""
    }
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
