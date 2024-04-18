// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.param._
import org.apache.spark.internal.Logging
import org.apache.spark.ml.param._

import scala.reflect.ClassTag

case class ParamInfo[T <: Param[_]: ClassTag](pyType: String,
                                               pyTypeConverter: Option[String],
                                               rTypeConverter: Option[String],
                                               example: Any) {
  def this(pyType: String, typeConverterArg: String, rTypeConverterArg: String, example: Any) = {
    this(pyType, Some(typeConverterArg), Some(rTypeConverterArg), example)
  }

  def this(pyType: String, example: Any) = {
    this(pyType, None, None, example)
  }

}

object DefaultParamInfo extends Logging {

  val BooleanInfo = new ParamInfo[BooleanParam](
    "bool", "TypeConverters.toBoolean", "as.logical", true)
  val IntInfo = new ParamInfo[IntParam](
    "int", "TypeConverters.toInt", "as.integer", 1)
  val LongInfo = new ParamInfo[LongParam](
    "long", None, Some("as.integer"), 1L)
  val FloatInfo = new ParamInfo[FloatParam](
    "float", "TypeConverters.toFloat", "as.double", 1.0)
  val DoubleInfo = new ParamInfo[DoubleParam](
    "float", "TypeConverters.toFloat", "as.double", 1.0)
  val StringInfo = new ParamInfo[Param[String]](
    "str", Some("TypeConverters.toString"), None, "foo")
  val StringArrayInfo = new ParamInfo[StringArrayParam](
    "list", "TypeConverters.toListString", "as.array", Array("foo", "bar"))
  val DoubleArrayInfo = new ParamInfo[DoubleArrayParam](
    "list", "TypeConverters.toListFloat", "as.array", Array(1.0, 2.0))
  val IntArrayInfo = new ParamInfo[IntArrayParam](
    "list", "TypeConverters.toListInt", "as.array", Array(1, 2))
  val ByteArrayInfo = new ParamInfo[ByteArrayParam](
    "list", Array(1.toByte, 0.toByte))
  val DoubleArrayArrayInfo = new ParamInfo[DoubleArrayArrayParam](
    "object", Array(Array(1.0, 2.0)))
  val StringStringMapInfo = new ParamInfo[StringStringMapParam](
    "dict", Map("foo" -> "bar"))
  val StringIntMapInfo = new ParamInfo[StringIntMapParam](
    "dict", Map("foo" -> 1))
  val ArrayMapInfo = new ParamInfo[ArrayMapParam](
    "object", Array(Map("foo" -> 1)))
  val TypedIntArrayInfo = new ParamInfo[TypedIntArrayParam](
    "object", Array(1, 2))
  val TypedDoubleArrayInfo = new ParamInfo[TypedDoubleArrayParam](
    "object", Array(1.0, 2.0))
  val UntypedArrayInfo = new ParamInfo[UntypedArrayParam](
    "object", Array(1.0, 2.0))
  val UnknownInfo = new ParamInfo[Param[_]](
    "object", null) //scalastyle:ignore null

  //scalastyle:off cyclomatic.complexity
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
    //scalastyle:on cyclomatic.complexity
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
