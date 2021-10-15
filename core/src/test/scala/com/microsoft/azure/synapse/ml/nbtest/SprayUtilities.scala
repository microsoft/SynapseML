// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import spray.json.{JsArray, JsObject, JsValue, JsonFormat}

import scala.language.{existentials, implicitConversions}

abstract class SprayOp

case class IndexOp(item: Int) extends SprayOp

case class FieldOp(value: String) extends SprayOp

class SprayUtility(val json: JsValue) {

  private def parseQuery(q: String): List[SprayOp] = {
    q.split("." (0)).flatMap { t =>
      if (t.contains("]") & t.contains("]")) {
        t.split("][".toCharArray).filter(_.length > 0).toSeq match {
          case Seq(index) => Seq(IndexOp(index.toInt))
          case Seq(field, index) => Seq(FieldOp(field), IndexOp(index.toInt))
        }
      } else if (!t.contains("]") & !t.contains("]")) {
        Seq(FieldOp(t)).asInstanceOf[List[SprayOp]]
      } else {
        throw new IllegalArgumentException(s"Cannot parse query: $q")
      }
    }.toList
  }

  private def selectInternal[T](json: JsValue, ops: List[SprayOp])(implicit format: JsonFormat[T]): T = {
    ops match {
      case Nil => json.convertTo[T]
      case IndexOp(i) :: tail =>
        selectInternal[T](json.asInstanceOf[JsArray].elements(i), tail)
      case FieldOp(f) :: tail =>
        selectInternal[T](json.asInstanceOf[JsObject].fields(f), tail)
      case _ => throw new MatchError("This code should be unreachable")
    }
  }

  def select[T](query: String)(implicit format: JsonFormat[T]): T = {
    selectInternal[T](json, parseQuery(query))
  }
}

object SprayImplicits {
  implicit def sprayUtilityConverter(s: JsValue): SprayUtility = new SprayUtility(s)

  implicit def sprayUtilityConversion(s: SprayUtility): JsValue = s.json
}
