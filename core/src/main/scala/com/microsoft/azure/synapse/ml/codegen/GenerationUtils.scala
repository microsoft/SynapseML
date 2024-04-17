// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.param.{PipelineStageWrappable, PythonWrappableParam}
import com.microsoft.azure.synapse.ml.param.RWrappableParam
import org.apache.spark.ml.param._

object GenerationUtils {
  def indent(lines: String, numTabs: Int): String = {
    lines.split("\n".toCharArray).map(l => "    " * numTabs + l).mkString("\n")
  }

  def camelToSnake(str: String): String = {
    val headInUpperCase = str.takeWhile(c => c.isUpper || c.isDigit)
    val tailAfterHeadInUppercase = str.dropWhile(c => c.isUpper || c.isDigit)

    if (tailAfterHeadInUppercase.isEmpty) headInUpperCase.toLowerCase else {
      val firstWord = if (headInUpperCase.dropRight(1).nonEmpty) {
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
        pwp.pyConstructorLine(v.asInstanceOf[pwp.InnerType])
      case _ =>
        s"""${p.name}=${PythonWrappableParam.pyDefaultRender(v, p)}"""
    }
  }

  def rRenderParam[T](pp: ParamPair[T]): String = {
    rRenderParam(pp.param, pp.value)
  }

  def rRenderParam[T](p: Param[T], v: T): String = {
    p match {
      case psw: PipelineStageWrappable[_] =>
        s"""${psw.name}=spark_jobj(${psw.rValue(v.asInstanceOf[psw.RInnerType])})"""
      case rp: RWrappableParam[_] =>
        rp.rConstructorLine(v.asInstanceOf[rp.RInnerType])
      case _ =>
        s"""${p.name}=${RWrappableParam.rDefaultRender(v, p)}"""
    }
  }

}
