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
        "." + pwp.dotnetSetterLine(v)
      case _: ComplexParam[_] =>
        throw new NotImplementedError("No translation found for complex parameter")
      case _: StringArrayParam | _: DoubleArrayParam | _: IntArrayParam | _: ByteArrayParam | _: DoubleArrayArrayParam |
           _: StringStringMapParam | _: StringIntMapParam | _: ArrayMapParam |
           _: TypedIntArrayParam | _: TypedDoubleArrayParam | _: UntypedArrayParam =>
        s""".Set${p.name.capitalize}(new ${getGeneralParamInfo(p).dotnetType}
           |    ${DotnetWrappableParam.dotnetDefaultRender(v, p)})""".stripMargin
      case _ =>
        val capName = p.name match {
          case "xgboostDartMode" => "XGBoostDartMode"
          case "parallelism" => p.parent.split("_".toCharArray).head match {
            case "VowpalWabbitContextualBandit" => "ParallelismForParamListFit"
            case _ => p.name.capitalize
          }
          case _ => p.name.capitalize
        }
        s""".Set$capName(${DotnetWrappableParam.dotnetDefaultRender(v, p)})"""
    }
  }

}
