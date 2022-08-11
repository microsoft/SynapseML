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
      elements.head match {
        case _: spray.json.JsObject =>
          printSeq(elements, sb.append(',')) { e =>  print(e, sb)  }
        case _ => {
          sb.append("list(")
          printSeq(elements, sb.append(',')) { e =>
                try {
                  printLeaf(e, sb)
                }
                catch {
                  case _: IllegalStateException => sb.append(e.compactPrint)
                }
          }
          sb.append(")")
        }
      }
    }
  }

  override protected def printObject(members: Map[String, JsValue], sb: JStringBuilder): Unit = {
    if (members.isEmpty) {
      sb.append("c()")
    } else {
      sb.append("list2env(list(")
      printSeq(members, sb.append(',')) { m =>
        sb.append('"')
        sb.append(m._1)
        sb.append('"')
        sb.append('=')
        print(m._2, sb)
      }
      sb.append("))")
    }
  }

  override protected def printLeaf(x: JsValue, sb: JStringBuilder): Unit = {
    x match {
      case JsNull      => sb.append("NULL")
      case JsTrue      => sb.append("TRUE")
      case JsFalse     => sb.append("FALSE")
      case JsNumber(x) => sb.append(if (x.toString.contains(".")) x else s"${x}L")
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

  type RInnerType = T

  def rValue(v: T): String = RWrappableParam.rDefaultRender(v, this)

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
