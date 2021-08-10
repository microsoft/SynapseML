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
  val typedIntArrayInfo = new ParamInfo[TypedIntArrayParam]("object", "List<int>")
  val typedDoubleArrayInfo = new ParamInfo[TypedDoubleArrayParam]("object", "List<double>")
  val untypedArrayInfo = new ParamInfo[UntypedArrayParam]("object", "object[]")
  // TODO: to be validated
  val seqStringInfo = stringArrayInfo
  // TODO: fix corresponding .net type
  val arrayParamMapInfo = new ParamInfo[ArrayParamMapParam]("object", "object")
  val ballTreeInfo = new ParamInfo[BallTreeParam]("object", "object")
  val conditionalBallTreeInfo = new ParamInfo[ConditionalBallTreeParam]("object", "object")
  val dataFrameParamInfo = new ParamInfo[DataFrameParam]("object", "DataFrame")
  val dataTypeInfo = new ParamInfo[DataTypeParam]("object", "DataType")
  val estimatorArrayInfo = new ParamInfo[EstimatorArrayParam]("object", "ScalaEstimator[]")
  val estimatorInfo = new ParamInfo[EstimatorParam]("object", "ScalaEstimator")
  val evaluatorInfo = new ParamInfo[EvaluatorParam]("object", "ScalaEvaluator")
  val paramSpaceInfo = new ParamInfo[ParamSpaceParam]("object", "object")
  val pipelineStageInfo = new ParamInfo[PipelineStageParam]("object", "ScalaPipelineStage")
  val transformerArrayInfo = new ParamInfo[TransformerArrayParam]("object", "ScalaTransformer[]")
  val transformerInfo = new ParamInfo[TransformerParam]("object", "ScalaTransFormer")
  val modelInfo = new ParamInfo[ModelParam]("object", "ScalaModel")
  val udfInfo = new ParamInfo[UDFParam]("object", "object")
  val udPyFInfo = new ParamInfo[UDPyFParam]("object", "object")
  val complexUnknownInfo = new ParamInfo[ComplexParam[_]]("object", "object")
  // TODO: add corresponding classes in .net in order for these to work
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
      case "Seq[String]" => seqStringInfo
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
      case _: ArrayParamMapParam => arrayParamMapInfo
      case _: BallTreeParam => ballTreeInfo
      case _: ConditionalBallTreeParam => conditionalBallTreeInfo
      case _: DataFrameParam => dataFrameParamInfo
      case _: DataTypeParam => dataTypeInfo
      case _: EstimatorArrayParam => estimatorArrayInfo
      case _: EstimatorParam => estimatorInfo
      case _: EvaluatorParam => evaluatorInfo
      case _: ParamSpaceParam => paramSpaceInfo
      case _: PipelineStageParam => pipelineStageInfo
      case _: TransformerArrayParam => transformerArrayInfo
      case _: TransformerParam => transformerInfo
      case _: ModelParam => modelInfo
      case _: UDFParam => udfInfo
      case _: UDPyFParam => udPyFInfo
      case _ => complexUnknownInfo
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
