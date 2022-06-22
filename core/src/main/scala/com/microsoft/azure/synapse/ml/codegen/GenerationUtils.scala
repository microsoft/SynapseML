// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import org.apache.spark.ml.param._

object GenerationUtils {
  def indent(lines: String, numTabs: Int): String = {
    lines.split("\n".toCharArray).map(l => "    " * numTabs + l).mkString("\n")
  }

  def camelToSnake(str: String): String = {
    val headInUpperCase = str.takeWhile(c => c.isUpper || c.isDigit)
    val tailAfterHeadInUppercase = str.dropWhile(c => c.isUpper || c.isDigit)

    if (tailAfterHeadInUppercase.isEmpty) headInUpperCase.toLowerCase else {
      val firstWord = if (!headInUpperCase.dropRight(1).isEmpty) {
        headInUpperCase.last match {
          case c if c.isDigit => headInUpperCase
          case _ => headInUpperCase.dropRight(1).toLowerCase
        }
      } else {
        headInUpperCase.toLowerCase + tailAfterHeadInUppercase.takeWhile(c => c.isLower)
      }

      if (firstWord == str.toLowerCase) {
        firstWord
      } else {
        s"${firstWord}_${camelToSnake(str.drop(firstWord.length))}"
      }

    }
  }

  def pyRenderParam[T](pp: ParamPair[T]): String = {
    pyRenderParam(pp.param, pp.value)
  }

  def pyRenderParam[T](p: Param[T], v: T): String = {
    p match {
      case pwp: PythonWrappableParam[_] =>
        pwp.pyConstructorLine(v.asInstanceOf[pwp.PyInnerType])
      case tap: TypedArrayParam[_] =>
        s"""${tap.name}=${PythonWrappableParam.pyDefaultRender(v, tap)}"""
      case _: ComplexParam[_] =>
        throw new NotImplementedError("No translation found for complex parameter")
      case _ =>
        s"""${p.name}=${PythonWrappableParam.pyDefaultRender(v, p)}"""
    }
  }

  def rRenderParam[T](pp: ParamPair[T]): String = {
    rRenderParam(pp.param, pp.value)
  }

  def rRenderParam[T](p: Param[T], v: T): String = {
    p match {
      case rwp: RWrappableParam[_] =>
        rwp.rConstructorLine(v.asInstanceOf[rwp.RInnerType])
      case tap: TypedArrayParam[_] =>
        s"""${tap.name}=${RWrappableParam.rDefaultRender(v, tap)}"""
      case sp: ServiceParam[_] =>
        v match {
          case _: Right[_,_] =>
            s"""${sp.name}Col=${RWrappableParam.rDefaultRender(v, sp)}"""
          case _ =>
            s"""${sp.name}=${RWrappableParam.rDefaultRender(v, sp)}"""
        }
      case _: ComplexParam[_] =>
        throw new NotImplementedError("No translation found for complex parameter")
      case _ =>
        s"""${p.name}=${RWrappableParam.rDefaultRender(v, p)}"""
    }
  }

}
