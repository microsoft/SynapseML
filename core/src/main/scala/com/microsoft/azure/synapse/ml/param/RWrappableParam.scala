// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import org.apache.spark.ml.param.Param
import spray.json._

import java.lang.{StringBuilder => JStringBuilder}
import scala.collection.immutable.Map

trait RPrinter extends CompactPrinter {

  override protected def printArray(elements: Seq[JsValue], sb: JStringBuilder): Unit = {
    if (elements.isEmpty) {
      sb.append("c()")
    } else {
      sb.append("'[")
      printSeq(elements, sb.append(',')) { e => sb.append(e.compactPrint) }
      sb.append("]'")
    }
  }


  override protected def printObject(members: Map[String, JsValue], sb: JStringBuilder): Unit = {
    if (members.isEmpty) {
      sb.append("new.env()")
    } else {
      //super.printObject(members, sb)
      printSeq(members, sb.append(',')) { m =>
        sb.append("list2env(list(")
        sb.append(m._1)
        sb.append('=')
        print(m._2, sb)
        sb.append("))")
      }

      /*
      printSeq(members, sb.append(',')) { m =>
        sb.append('{')
        printString(m._1, sb)
        sb.append(':')
        print(m._2, sb)
        sb.append('}')
      } */
    }
  }

  override protected def printLeaf(x: JsValue, sb: JStringBuilder): Unit = {
    x match {
      case JsNull      => sb.append("NULL")
      case JsTrue      => sb.append("TRUE")
      case JsFalse     => sb.append("FALSE")
      case JsNumber(x) => sb.append(if (x.toString.contains(".")) x else s"as.integer($x)")
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
    param match {
      case _: TypedIntArrayParam | _: TypedDoubleArrayParam =>
        rDefaultRender(value, { v: T => param.jsonEncode(v) })
      case p: JsonEncodableParam[_] =>
        if (value == Nil) {
          "NULL"
        } else {
          value match {
            case left: Left[_, _] =>
              rDefaultRender(value, { v: T => param.jsonEncode(v) })
            case right: Right[_, _] =>
              rDefaultRender(value, { v: T => param.jsonEncode(v) })
            //case
              //s""""${right.value.toString}""""
            case _ =>
              "'" + p.jsonEncode(value) + "'"
          }
        }
      case _ =>
        rDefaultRender(value, { v: T => param.jsonEncode(v) })
    }
  }

}

trait RWrappableParam[T] extends Param[T] {

  val name: String

  type RInnerType = T

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

trait ExternalRWrappableParam[T] extends RWrappableParam[T] {

  def rLoadLine(modelNum: Int): String

}