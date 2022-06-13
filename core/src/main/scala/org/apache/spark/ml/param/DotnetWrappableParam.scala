// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import spray.json._

import java.lang.{StringBuilder => JStringBuilder}

trait DotnetPrinter extends CompactPrinter {

  override protected def printArray(elements: Seq[JsValue], sb: JStringBuilder): Unit = {
    sb.append("{")
    printSeq(elements, sb.append(',')) {
      case JsArray(e) => {
        sb.append("new []")
        printArray(e, sb)
      }
      case JsObject(e) => {
        sb.append("new Dictionary<string, object>")
        printObject(e, sb)
      }
      case e => print(e, sb)
    }
    sb.append("}")
  }

  override protected def printObject(members: Map[String, JsValue], sb: JStringBuilder): Unit = {
    sb.append('{')
    printSeq(members, sb.append(',')) { m =>
      sb.append('{')
      printString(m._1, sb)
      sb.append(',')
      print(m._2, sb)
      sb.append('}')
    }
    sb.append('}')
  }

}

object DotnetPrinter extends DotnetPrinter

object DotnetWrappableParam {

  def dotnetDefaultRender[T](value: T, jsonFunc: T => String): String = {
    DotnetPrinter(jsonFunc(value).parseJson)
  }

  def dotnetDefaultRender[T](value: T)(implicit dataFormat: JsonFormat[T]): String = {
    dotnetDefaultRender(value, { v: T => v.toJson.compactPrint })
  }

  def dotnetDefaultRender[T](value: T, param: Param[T]): String = {
    dotnetDefaultRender(value, { v: T => param.jsonEncode(v) })
  }

}

trait DotnetWrappableParam[T] extends Param[T] {

  val name: String

  // Used for generating set values for dotnet tests
  def dotnetTestValue(v: T): String

  def dotnetName(v: T): String = {
    name
  }

  // Used for generating dotnet tests setters
  def dotnetTestSetterLine(v: T): String =
    s"""Set${dotnetName(v).capitalize}(${dotnetTestValue(v)})"""

}

trait ExternalDotnetWrappableParam[T] extends DotnetWrappableParam[T] {

  // Use this in tests if the param is loaded instead of constructed directly
  def dotnetLoadLine(modelNum: Int): String

}
