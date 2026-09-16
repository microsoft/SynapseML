// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.param._
import org.apache.spark.internal.Logging
import org.apache.spark.ml.param._

import java.lang.reflect.{ParameterizedType, Type => JavaType}
import scala.reflect.ClassTag
import scala.util.Try

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

private[codegen] case class PythonTypeInfo(pyType: String, pyiType: String)

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
  private def generalParamInfo(dataType: Param[_]): ParamInfo[_] = {
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
      case _ => UnknownInfo
    }
    //scalastyle:on cyclomatic.complexity
  }

  def getGeneralParamInfo(dataType: Param[_]): ParamInfo[_] = {
    val result = generalParamInfo(dataType)
    if (result == UnknownInfo) {
      logWarning(s"unsupported type $dataType")
    }
    result
  }

  private val StubTypeOverrides: Seq[(ParamInfo[_], String)] = Seq(
    LongInfo -> "int",
    StringArrayInfo -> "List[str]",
    DoubleArrayInfo -> "List[float]",
    IntArrayInfo -> "List[int]",
    ByteArrayInfo -> "List[int]",
    DoubleArrayArrayInfo -> "List[List[float]]",
    StringStringMapInfo -> "Dict[str, str]",
    StringIntMapInfo -> "Dict[str, int]",
    ArrayMapInfo -> "List[Dict[str, Any]]",
    TypedIntArrayInfo -> "List[int]",
    TypedDoubleArrayInfo -> "List[float]",
    UntypedArrayInfo -> "List[Any]",
    UnknownInfo -> "Any"
  )

  private[codegen] def pythonTypeInfo(paramInfo: ParamInfo[_]): PythonTypeInfo = {
    val pyiType = StubTypeOverrides.collectFirst {
      case (info, stubType) if info.eq(paramInfo) => stubType
    }.getOrElse(paramInfo.pyType)
    PythonTypeInfo(paramInfo.pyType, pyiType)
  }

  private def genericTypeInfo(pyType: String, pyiType: String): PythonTypeInfo =
    PythonTypeInfo(pyType, pyiType)

  private val DirectTypeInfo: Map[Class[_], PythonTypeInfo] = Map(
    java.lang.Boolean.TYPE -> pythonTypeInfo(BooleanInfo),
    classOf[java.lang.Boolean] -> pythonTypeInfo(BooleanInfo),
    java.lang.Byte.TYPE -> pythonTypeInfo(IntInfo),
    classOf[java.lang.Byte] -> pythonTypeInfo(IntInfo),
    java.lang.Short.TYPE -> pythonTypeInfo(IntInfo),
    classOf[java.lang.Short] -> pythonTypeInfo(IntInfo),
    java.lang.Integer.TYPE -> pythonTypeInfo(IntInfo),
    classOf[java.lang.Integer] -> pythonTypeInfo(IntInfo),
    java.lang.Long.TYPE -> pythonTypeInfo(LongInfo),
    classOf[java.lang.Long] -> pythonTypeInfo(LongInfo),
    java.lang.Float.TYPE -> pythonTypeInfo(FloatInfo),
    classOf[java.lang.Float] -> pythonTypeInfo(FloatInfo),
    java.lang.Double.TYPE -> pythonTypeInfo(DoubleInfo),
    classOf[java.lang.Double] -> pythonTypeInfo(DoubleInfo),
    classOf[String] -> pythonTypeInfo(StringInfo)
  )

  private val NamedTypeInfo: Map[String, PythonTypeInfo] = Map(
    "org.apache.spark.ml.param.ParamMap" -> genericTypeInfo("ParamMap", "ParamMap"),
    "org.apache.spark.sql.Dataset" -> genericTypeInfo("DataFrame", "DataFrame"),
    "org.apache.spark.sql.types.DataType" -> genericTypeInfo("DataType", "DataType")
  )

  private def classInfo(clazz: Class[_]): Option[PythonTypeInfo] = {
    DirectTypeInfo.get(clazz)
      .orElse(NamedTypeInfo.get(clazz.getName))
      .orElse {
        if (clazz.isArray) {
          val innerType = javaTypeInfo(clazz.getComponentType).map(_.pyiType).getOrElse("Any")
          Some(genericTypeInfo("list", s"List[$innerType]"))
        } else {
          None
        }
      }
  }

  private def javaTypeInfo(javaType: JavaType): Option[PythonTypeInfo] = {
    javaType match {
      case clazz: Class[_] =>
        classInfo(clazz)
      case parameterizedType: ParameterizedType =>
        parameterizedInfo(parameterizedType)
      case _ =>
        None
    }
  }

  private[codegen] def pythonMethodArgumentType(javaType: JavaType): String = {
    javaType match {
      case parameterizedType: ParameterizedType
        if (parameterizedType.getRawType match {
          case clazz: Class[_] => classOf[Param[_]].isAssignableFrom(clazz)
          case _ => false
        }) =>
        "Param"
      case clazz: Class[_] if classOf[Param[_]].isAssignableFrom(clazz) =>
        "Param"
      case _ =>
        javaTypeInfo(javaType).map(_.pyiType).getOrElse("Any")
    }
  }

  private def parameterizedInfo(parameterizedType: ParameterizedType): Option[PythonTypeInfo] = {
    val rawType = parameterizedType.getRawType
    val arguments = parameterizedType.getActualTypeArguments
    rawType match {
      case clazz: Class[_] if classOf[Param[_]].isAssignableFrom(clazz) =>
        arguments.headOption.flatMap(javaTypeInfo)
      case clazz: Class[_] if Seq(
        "scala.collection.Seq",
        "scala.collection.immutable.Seq",
        "java.util.List"
      ).contains(clazz.getName) =>
        val innerType = arguments.headOption.flatMap(javaTypeInfo).map(_.pyiType).getOrElse("Any")
        Some(genericTypeInfo("list", s"List[$innerType]"))
      case clazz: Class[_] if Seq(
        "scala.collection.Map",
        "scala.collection.immutable.Map",
        "java.util.Map"
      ).contains(clazz.getName) =>
        val keyType = arguments.headOption.flatMap(javaTypeInfo).map(_.pyiType).getOrElse("Any")
        val valueType = arguments.drop(1).headOption.flatMap(javaTypeInfo).map(_.pyiType).getOrElse("Any")
        Some(genericTypeInfo("dict", s"Dict[$keyType, $valueType]"))
      case clazz: Class[_] if clazz.getName == "org.apache.spark.sql.Dataset" =>
        Some(genericTypeInfo("DataFrame", "DataFrame"))
      case _ =>
        None
    }
  }

  private def reflectedTypeInfo(stage: Params, methodName: String): Option[PythonTypeInfo] =
    Try(stage.getClass.getMethod(methodName)).toOption.flatMap(method => javaTypeInfo(method.getGenericReturnType))

  private[codegen] def defaultPythonTypeInfo(stage: Params, p: Param[_]): PythonTypeInfo = {
    val generalInfo = generalParamInfo(p)
    val reflectedInfo = p match {
      case _: ServiceParam[_] =>
        reflectedTypeInfo(stage, "get" + p.name.capitalize)
          .orElse(reflectedTypeInfo(stage, p.name))
      case _ if generalInfo == UnknownInfo =>
        reflectedTypeInfo(stage, p.name)
          .orElse(reflectedTypeInfo(stage, "get" + p.name.capitalize))
      case _ =>
        None
    }
    reflectedInfo.getOrElse(pythonTypeInfo(generalInfo))
  }

  def defaultGetParamInfo(stage: Params, p: Param[_]): ParamInfo[_] = {
    p match {
      case _: ServiceParam[_] =>
        UnknownInfo
      case _ =>
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

}
