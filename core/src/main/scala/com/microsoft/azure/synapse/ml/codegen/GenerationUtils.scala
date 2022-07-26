// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import com.microsoft.azure.synapse.ml.param.TypedArrayParam
import com.microsoft.azure.synapse.ml.param.{DotnetWrappableParam, PythonWrappableParam, RWrappableParam}
import org.apache.spark.ml._
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
        pwp.pyConstructorLine(v.asInstanceOf[pwp.InnerType])
      case _: ComplexParam[_] =>
        throw new NotImplementedError("No translation found for complex parameter")
      case _ =>
        s"""${p.name}=${PythonWrappableParam.pyDefaultRender(v, p)}"""
    }
  }

  def dotnetRenderParam[T](pp: ParamPair[T]): String = {
    dotnetRenderParam(pp.param, pp.value)
  }

  //noinspection ScalaStyle
  def dotnetRenderParam[T](p: Param[T], v: T): String = {
    import DefaultParamInfo._

    p match {
      case pwp: DotnetWrappableParam[T] =>
        "." + pwp.dotnetTestSetterLine(v)
      case _: StringArrayParam | _: DoubleArrayParam | _: IntArrayParam |
           _: DoubleArrayArrayParam =>
        s""".Set${p.name.capitalize}(new ${getGeneralParamInfo(p).dotnetType}
           |    ${DotnetWrappableParam.dotnetDefaultRender(v, p)})""".stripMargin
      case _ =>
        s""".Set${p.name.capitalize}(${DotnetWrappableParam.dotnetDefaultRender(v, p)})"""
    }
  }

  def rRenderParam[T](pp: ParamPair[T]): String = {
    rRenderParam(pp.param, pp.value)
  }

  def rRenderParam[T](p: Param[T], v: T): String = {
    p match {
      case rwp: RWrappableParam[_] =>
        v match {
          case _: PipelineStage =>
            s"""${rwp.name}=spark_jobj(${rwp.rValue(v.asInstanceOf[rwp.RInnerType])})"""
          case _ =>
            rwp.rConstructorLine(v.asInstanceOf[rwp.RInnerType])
        }
      //case tap: TypedArrayParam[_] =>
      //  s"""${tap.name}=${RWrappableParam.rDefaultRender(v, tap)}"""
      case _: ComplexParam[_] =>
        throw new NotImplementedError("No translation found for complex parameter")
      case _ =>
        s"""${p.name}=${RWrappableParam.rDefaultRender(v, p)}"""
    }
  }

}
