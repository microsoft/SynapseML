// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.codegen

import com.microsoft.ml.spark.core.serialize.ComplexParam
import org.apache.spark.ml.param._

import scala.reflect.runtime.universe._

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
        pwp.pyConstructorLine(v.asInstanceOf[pwp.pyInnerType])
      case _: ComplexParam[_] =>
        throw new NotImplementedError("No translation found for complex parameter")
      case _ =>
        s"""${p.name}=${PythonWrappableParam.pyDefaultRender(v, p)}"""
    }
  }

  def dotnetRenderParam[T: TypeTag](pp: ParamPair[T]): String = {
    dotnetRenderParam(pp.param, pp.value)
  }

  //noinspection ScalaStyle
  def dotnetRenderParam[T: TypeTag](p: Param[T], v: T): String = {
    p match {
      case pwp: DotnetWrappableParam[T] =>
        "." + pwp.dotnetSetterLine(v)
      case _: ComplexParam[_] =>
        throw new NotImplementedError("No translation found for complex parameter")
      case _: StringArrayParam =>
        s""".Set${p.name.capitalize}(new string[] ${DotnetWrappableParam.dotnetDefaultRender(v, p)})"""
      case _: DoubleArrayParam =>
        s""".Set${p.name.capitalize}(new double[] ${DotnetWrappableParam.dotnetDefaultRender(v, p)})"""
      case _: IntArrayParam =>
        s""".Set${p.name.capitalize}(new int[] ${DotnetWrappableParam.dotnetDefaultRender(v, p)})"""
      case _: ByteArrayParam =>
        s""".Set${p.name.capitalize}(new byte[] ${DotnetWrappableParam.dotnetDefaultRender(v, p)})"""
      // TODO: fix default render for double[][]
      case _: DoubleArrayArrayParam =>
        s""".Set${p.name.capitalize}(new double[][] ${DotnetWrappableParam.dotnetDefaultRender(v, p)})"""
      case _: StringStringMapParam =>
        s""".Set${p.name.capitalize}(
           |new Dictionary<string, string>() ${DotnetWrappableParam.dotnetDefaultRender(v, p)})""".stripMargin
      case _: StringIntMapParam =>
        s""".Set${p.name.capitalize}(
           |new Dictionary<string, int>() ${DotnetWrappableParam.dotnetDefaultRender(v, p)})""".stripMargin
      case _: TypedIntArrayParam =>
        s""".Set${p.name.capitalize}(new List<int>() ${DotnetWrappableParam.dotnetDefaultRender(v, p)})"""
      case _: TypedDoubleArrayParam =>
        s""".Set${p.name.capitalize}(new List<double>() ${DotnetWrappableParam.dotnetDefaultRender(v, p)})"""
      case _ =>
        s""".Set${p.name.capitalize}(${DotnetWrappableParam.dotnetDefaultRender(v, p)})"""
    }
  }

}
