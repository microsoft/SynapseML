// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import spray.json._
import java.lang.{StringBuilder => JStringBuilder}

trait RPrinter extends CompactPrinter {
  override protected def printArray(elements: Seq[JsValue], sb: JStringBuilder): Unit = {
    sb.append("c(")
    printSeq(elements, sb.append(','))(print(_, sb))
    sb.append(")")
  }

  override protected def printLeaf(x: JsValue, sb: JStringBuilder): Unit = {
    x match {
      case JsNull      => sb.append("NULL")
      case JsTrue      => sb.append("TRUE")
      case JsFalse     => sb.append("FALSE")
      case JsNumber(x) => sb.append(x)
      case JsString(x) => printString(x, sb)
      case _           => throw new IllegalStateException
    }
  }
}

object RPrinter extends RPrinter

object RWrappableParam {

  def rDefaultRender[T](value: T, jsonFunc: T => String): String = {
    RPrinter(jsonFunc(value).parseJson)
  }

  def rDefaultRender[T](value: T)(implicit dataFormat: JsonFormat[T]): String = {
    rDefaultRender(value, { v: T => v.toJson.compactPrint })
  }

  def rDefaultRender[T](value: T, param: Param[T]): String = {
    rDefaultRender(value, { v: T => param.jsonEncode(v) })
  }

}

trait RWrappableParam[T] extends Param[T] {

  val name: String

  type InnerType = T

  def rValue(v: T): String

  def rName(v: T): String = {
    name
  }

  def rConstructorLine(v: T): String = {
    s"""${rName(v)}=${rValue(v)}"""
  }

  def rSetterLine(v: T): String = {
    s"""set${rName(v).capitalize}(${rValue(v)})"""
  }

}
