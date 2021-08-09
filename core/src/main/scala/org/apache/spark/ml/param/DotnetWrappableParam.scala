// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import spray.json._
import java.lang.{StringBuilder => JStringBuilder}
import scala.reflect.runtime.universe._

trait DotnetPrinter extends CompactPrinter {

  override protected def printArray(elements: Seq[JsValue], sb: JStringBuilder): Unit = {
    sb.append("{")
    printSeq(elements, sb.append(','))(print(_, sb))
    sb.append("}")
  }

  override protected def printObject(members: Map[String, JsValue], sb: JStringBuilder): Unit = {
    sb.append('{')
    printSeq(members, sb.append(',')) { m =>
      printString(m._1, sb)
      sb.append(',')
      print(m._2, sb)
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

  type dotnetInnerType = T

  def dotnetValue(v: T): String

  def dotnetName(v: T): String = {
    name
  }

  def dotnetSetterLine(v: T): String = {
    v match {
      case _: Array[String] => s"""Set${dotnetName(v).capitalize}(new string[] ${dotnetValue(v)})"""
      case _: Array[Double] => s"""Set${dotnetName(v).capitalize}(new double[] ${dotnetValue(v)})"""
      case _: Array[Int] => s"""Set${dotnetName(v).capitalize}(new int[] ${dotnetValue(v)})"""
      case _: Array[Byte] => s"""Set${dotnetName(v).capitalize}(new byte[] ${dotnetValue(v)})"""
      case _: Map[_, _] =>
        s"""Set${dotnetName(v).capitalize}(new Dictionary<object, object>() ${dotnetValue(v)})"""
      case _ => s"""Set${dotnetName(v).capitalize}(${dotnetValue(v)})"""
    }
  }

//  def

}

trait ExternalDotnetWrappableParam[T] extends DotnetWrappableParam[T] {

  def dotnetLoadLine(modelNum: Int): String

}
