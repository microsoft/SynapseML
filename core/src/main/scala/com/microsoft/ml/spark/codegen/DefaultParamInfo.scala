// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.codegen

import com.microsoft.ml.spark.core.serialize.ComplexParam
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

trait DefaultParamInfo extends StageParam {
  val booleanInfo = new ParamInfo[BooleanParam]("bool", "TypeConverters.toBoolean", "as.logical", "bool")
  val intInfo = new ParamInfo[IntParam]("int", "TypeConverters.toInt", "as.integer", "int")
  val longInfo = new ParamInfo[LongParam]("long", None, Some("as.integer"), "long")
  val floatInfo = new ParamInfo[FloatParam]("float", "TypeConverters.toFloat", "as.double", "float")
  val doubleInfo = new ParamInfo[DoubleParam]("float", "TypeConverters.toFloat", "as.double", "double")
  val stringInfo = new ParamInfo[Param[String]]("str", Some("TypeConverters.toString"), None, "string")
  val stringArrayInfo = new ParamInfo[StringArrayParam]("list", "TypeConverters.toListString",
    "as.array", "string[]")
  val doubleArrayInfo = new ParamInfo[DoubleArrayParam]("list", "TypeConverters.toListFloat",
    "as.array", "double[]")
  val intArrayInfo = new ParamInfo[IntArrayParam]("list", "TypeConverters.toListInt",
    "as.array", "int[]")
  val byteArrayInfo = new ParamInfo[ByteArrayParam]("list", "byte[]")
  val doubleArrayArrayInfo = new ParamInfo[DoubleArrayArrayParam]("object", "double[][]")
  val stringStringMapInfo = new ParamInfo[StringStringMapParam]("dict", "Dictionary<string, string>")
  val stringIntMapInfo = new ParamInfo[StringIntMapParam]("dict", "Dictionary<string, int>")
  val arrayMapInfo = new ParamInfo[ArrayMapParam]("object", "Dictionary<string, object>[]")
  val typedIntArrayInfo = new ParamInfo[TypedIntArrayParam]("object", "int[]")
  val typedDoubleArrayInfo = new ParamInfo[TypedDoubleArrayParam]("object", "double[]")
  val untypedArrayInfo = new ParamInfo[UntypedArrayParam]("object", "object[]")

  val seqTimeSeriesPointInfo = new ParamInfo[ServiceParam[_]]("object", "TimeSeriesPoint[]")
  val seqTargetInputInfo = new ParamInfo[ServiceParam[_]]("object", "TargetInput[]")
  val seqStringTupleInfo = new ParamInfo[ServiceParam[_]]("object", "Tuple<string, string>[]")

  //noinspection ScalaStyle
  def getServiceParamInfo(dataType: ServiceParam[_]): ParamInfo[_] = {
    dataType.getType match {
      case "String" => stringInfo
      case "Boolean" => booleanInfo
      case "Double" => doubleInfo
      case "Int" => intInfo
      case "Seq[String]" => stringArrayInfo
      case "Seq[com.microsoft.ml.spark.cognitive.TimeSeriesPoint]" => seqTimeSeriesPointInfo
      case "Array[Byte]" => byteArrayInfo
      case "Seq[com.microsoft.ml.spark.cognitive.TargetInput]" => seqTargetInputInfo
      case "Seq[(String, String)]" => seqStringTupleInfo
      case _ => throw new Exception(s"unsupported type $dataType")
    }
  }

  //noinspection ScalaStyle
  def getComplexParamInfo(dataType: ComplexParam[_]): ParamInfo[_] = {
    dataType match {
      case w: WrappableParam[_] => new ParamInfo[ComplexParam[_]]("object", w.dotnetParamInfo)
      case _ => throw new Exception(s"unsupported Complex Param type $dataType")
    }
  }

  //noinspection ScalaStyle
  def getParamInfo(dataType: Param[_]): ParamInfo[_] = {
    dataType match {
      case _: BooleanParam => booleanInfo
      case _: IntParam => intInfo
      case _: LongParam => longInfo
      case _: FloatParam => floatInfo
      case _: DoubleParam => doubleInfo
      case _: StringArrayParam => stringArrayInfo
      case _: DoubleArrayParam => doubleArrayInfo
      case _: IntArrayParam => intArrayInfo
      case _: ByteArrayParam => byteArrayInfo
      case _: DoubleArrayArrayParam => doubleArrayArrayInfo
      case _: StringStringMapParam => stringStringMapInfo
      case _: StringIntMapParam => stringIntMapInfo
      case _: ArrayMapParam => arrayMapInfo
      case _: TypedIntArrayParam => typedIntArrayInfo
      case _: TypedDoubleArrayParam => typedDoubleArrayInfo
      case _: UntypedArrayParam => untypedArrayInfo
      case cp: ComplexParam[_] => getComplexParamInfo(cp)
      case p =>
        thisStage.getClass.getMethod(p.name)
          .getAnnotatedReturnType.getType.toString match {
          case "org.apache.spark.ml.param.Param<java.lang.String>" => stringInfo
          case _ => throw new Exception(s"unsupported type $dataType")
        }
    }
  }

}

trait StageParam extends Params {
  protected val thisStage: Params = this
}
