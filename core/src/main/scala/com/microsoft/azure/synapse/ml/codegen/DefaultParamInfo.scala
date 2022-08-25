// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.param._
import org.apache.spark.internal.Logging
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.param._

import scala.reflect.ClassTag

case class ParamInfo[T <: Param[_]: ClassTag](pyType: String,
                                               pyTypeConverter: Option[String],
                                               rTypeConverter: Option[String],
                                               dotnetType: String,
                                               example: Any) {

  def this(pyType: String, typeConverterArg: String, rTypeConverterArg: String, dotnetType: String, example: Any) = {
    this(pyType, Some(typeConverterArg), Some(rTypeConverterArg), dotnetType, example)
  }

  def this(pyType: String, dotnetType: String, example: Any) = {
    this(pyType, None, None, dotnetType, example)
  }

}

object DefaultParamInfo extends Logging {

  val BooleanInfo = new ParamInfo[BooleanParam](
    "bool", "TypeConverters.toBoolean", "as.logical", "bool", true)
  val IntInfo = new ParamInfo[IntParam](
    "int", "TypeConverters.toInt", "as.integer", "int", 1)
  val LongInfo = new ParamInfo[LongParam](
    "long", None, Some("as.integer"), "long", 1L)
  val FloatInfo = new ParamInfo[FloatParam](
    "float", "TypeConverters.toFloat", "as.double", "float", 1.0)
  val DoubleInfo = new ParamInfo[DoubleParam](
    "float", "TypeConverters.toFloat", "as.double", "double", 1.0)
  val StringInfo = new ParamInfo[Param[String]](
    "str", Some("TypeConverters.toString"), None, "string", "foo")
  val StringArrayInfo = new ParamInfo[StringArrayParam](
    "list", "TypeConverters.toListString", "as.array", "string[]", Array("foo", "bar"))
  val DoubleArrayInfo = new ParamInfo[DoubleArrayParam](
    "list", "TypeConverters.toListFloat", "as.array", "double[]", Array(1.0, 2.0))
  val IntArrayInfo = new ParamInfo[IntArrayParam](
    "list", "TypeConverters.toListInt", "as.array", "int[]", Array(1, 2))
  val ByteArrayInfo = new ParamInfo[ByteArrayParam](
    "list", "byte[]", Array(1.toByte, 0.toByte))
  val DoubleArrayArrayInfo = new ParamInfo[DoubleArrayArrayParam](
    "object", "double[][]", Array(Array(1.0, 2.0)))
  val StringStringMapInfo = new ParamInfo[StringStringMapParam](
    "dict", "Dictionary<string, string>", Map("foo" -> "bar"))
  val StringIntMapInfo = new ParamInfo[StringIntMapParam](
    "dict", "Dictionary<string, int>", Map("foo" -> 1))
  val ArrayMapInfo = new ParamInfo[ArrayMapParam](
    "object", "Dictionary<string, object>[]", Array(Map("foo" -> 1)))
  val TypedIntArrayInfo = new ParamInfo[TypedIntArrayParam](
    "object", "int[]", Array(1, 2))
  val TypedDoubleArrayInfo = new ParamInfo[TypedDoubleArrayParam](
    "object", "double[]", Array(1.0, 2.0))
  val UntypedArrayInfo = new ParamInfo[UntypedArrayParam](
    "object", "object[]", Array(1.0, 2.0))
  val UnknownInfo = new ParamInfo[Param[_]](
    "object", "object", null)

  //noinspection ScalaStyle
  def getGeneralParamInfo(dataType: Param[_]): ParamInfo[_] = {
    dataType match {
      case _: BooleanParam => BooleanInfo
      case _: IntParam => IntInfo
      case _: LongParam => LongInfo
      case _: FloatParam => FloatInfo
      case _: DoubleParam => DoubleInfo
      case _: StringArrayParam => StringArrayInfo
      case _: DoubleArrayParam => DoubleArrayInfo
      case _: IntArrayParam => IntArrayInfo
      case _: ByteArrayParam => ByteArrayInfo
      case _: DoubleArrayArrayParam => DoubleArrayArrayInfo
      case _: StringStringMapParam => StringStringMapInfo
      case _: StringIntMapParam => StringIntMapInfo
      case _: ArrayMapParam => ArrayMapInfo
      case _: TypedIntArrayParam => TypedIntArrayInfo
      case _: TypedDoubleArrayParam => TypedDoubleArrayInfo
      case _: UntypedArrayParam => UntypedArrayInfo
      case p => {
        logWarning(s"unsupported type $p")
        UnknownInfo
      }
    }
  }

  def defaultGetParamInfo(stage: Params, p: Param[_]): ParamInfo[_] = {
    try {
      stage.getClass.getMethod(p.name)
        .getAnnotatedReturnType.getType.toString match {
        case "org.apache.spark.ml.param.Param<java.lang.String>" => StringInfo
        case _ => getGeneralParamInfo(p)
      }
    } catch {
      case _: Exception => getGeneralParamInfo(p)
    }
  }

}
