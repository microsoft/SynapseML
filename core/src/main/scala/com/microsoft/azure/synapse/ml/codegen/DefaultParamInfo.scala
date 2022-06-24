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
                                              dotnetType: String) {

  def this(pyType: String, typeConverterArg: String, rTypeConverterArg: String, dotnetType: String) = {
    this(pyType, Some(typeConverterArg), Some(rTypeConverterArg), dotnetType)
  }

  def this(pyType: String, dotnetType: String) = {
    this(pyType, None, None, dotnetType)
  }

}

object DefaultParamInfo extends Logging {

  val BooleanInfo = new ParamInfo[BooleanParam]("bool", "TypeConverters.toBoolean", "as.logical", "bool")
  val IntInfo = new ParamInfo[IntParam]("int", "TypeConverters.toInt", "as.integer", "int")
  val LongInfo = new ParamInfo[LongParam]("long", None, Some("as.integer"), "long")
  val FloatInfo = new ParamInfo[FloatParam]("float", "TypeConverters.toFloat", "as.double", "float")
  val DoubleInfo = new ParamInfo[DoubleParam]("float", "TypeConverters.toFloat", "as.double", "double")
  val StringInfo = new ParamInfo[Param[String]]("str", Some("TypeConverters.toString"), None, "string")
  val StringArrayInfo = new ParamInfo[StringArrayParam]("list", "TypeConverters.toListString",
    "as.array", "string[]")
  val DoubleArrayInfo = new ParamInfo[DoubleArrayParam]("list", "TypeConverters.toListFloat",
    "as.array", "double[]")
  val IntArrayInfo = new ParamInfo[IntArrayParam]("list", "TypeConverters.toListInt",
    "as.array", "int[]")
  val ByteArrayInfo = new ParamInfo[ByteArrayParam]("list", "byte[]")
  val DoubleArrayArrayInfo = new ParamInfo[DoubleArrayArrayParam]("object", "double[][]")
  val StringStringMapInfo = new ParamInfo[StringStringMapParam]("dict", "Dictionary<string, string>")
  val StringIntMapInfo = new ParamInfo[StringIntMapParam]("dict", "Dictionary<string, int>")
  val ArrayMapInfo = new ParamInfo[ArrayMapParam]("object", "Dictionary<string, object>[]")
  val TypedIntArrayInfo = new ParamInfo[TypedIntArrayParam]("object", "int[]")
  val TypedDoubleArrayInfo = new ParamInfo[TypedDoubleArrayParam]("object", "double[]")
  val UntypedArrayInfo = new ParamInfo[UntypedArrayParam]("object", "object[]")

  val UnknownInfo = new ParamInfo[Param[_]]("object", "object")

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

}
