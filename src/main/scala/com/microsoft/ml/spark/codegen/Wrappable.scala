// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.codegen

import java.lang.reflect.ParameterizedType
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import com.microsoft.ml.spark.core.env.FileUtilities
import com.microsoft.ml.spark.core.serialize.ComplexParam
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.{Estimator, Model, Transformer}

import scala.collection.Iterator.iterate
import scala.reflect.ClassTag


case class PyParamInfo[T <: Param[_]: ClassTag](pyType: String,
                                                typeConverter: Option[String]) {

  def this(pyType: String, typeConverterArg: String) = {
    this(pyType, Some(typeConverterArg))
  }

  def this(pyType: String) = {
    this(pyType, None)
  }

}

object DefaultParamInfo {
  val BooleanInfo = new PyParamInfo[BooleanParam]("bool", "TypeConverters.toBoolean")
  val IntInfo = new PyParamInfo[IntParam]("int", "TypeConverters.toInt")
  val LongInfo = new PyParamInfo[LongParam]("long")
  val FloatInfo = new PyParamInfo[FloatParam]("float", "TypeConverters.toFloat")
  val DoubleInfo = new PyParamInfo[DoubleParam]("float", "TypeConverters.toFloat")
  val StringInfo = new PyParamInfo[Param[String]]("str", "TypeConverters.toString")
  val StringArrayInfo = new PyParamInfo[StringArrayParam]("list", "TypeConverters.toListString")
  val DoubleArrayInfo = new PyParamInfo[DoubleArrayParam]("list", "TypeConverters.toListFloat")
  val IntArrayInfo = new PyParamInfo[IntArrayParam]("list", "TypeConverters.toListInt")
  val ByteArrayInfo = new PyParamInfo[ByteArrayParam]("list")
  val MapArrayInfo = new PyParamInfo[MapArrayParam]("dict")
  val MapInfo = new PyParamInfo[MapParam[_, _]]("dict")
  val UnknownInfo = new PyParamInfo[Param[_]]("object")

  //noinspection ScalaStyle
  def getParamInfo(dataType: Param[_]): PyParamInfo[_] = {
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
      case _: MapArrayParam => MapArrayInfo
      case _: MapParam[_, _] => MapInfo
      //case _: Param[String] => StringInfo //TODO fix erasure issues
      case _ => UnknownInfo
    }
  }
}


trait PythonWrappable extends Params {

  import GenerationUtils._
  import DefaultParamInfo._

  def pyAdditionalMethods: String = {
    ""
  }

  protected lazy val pyCopyrightLines: String =
    s"""|# Copyright (C) Microsoft Corporation. All rights reserved.
        |# Licensed under the MIT License. See LICENSE in project root for information.
        |""".stripMargin

  protected lazy val pyInternalWrapper = false

  protected lazy val pyClassNameHelper: String = this.getClass.getName.split(".".toCharArray).last

  protected lazy val pyClassName: String = {
    if (pyInternalWrapper) {
      "_" + pyClassNameHelper
    } else {
      "" + pyClassNameHelper
    }
  }

  protected lazy val pyObjectBaseClass: String = {
    this match {
      case _: Estimator[_] => "JavaEstimator"
      case _: Model[_] => "JavaModel"
      case _: Transformer => "JavaTransformer"
      case _: Evaluator => "JavaEvaluator"
    }
  }

  protected lazy val pyInheritedClasses =
    Seq("ComplexParamsMixin", "JavaMLReadable", "JavaMLWritable", pyObjectBaseClass)

  // TODO add default values
  protected lazy val pyClassDoc: String = {
    val argLines = this.params.map { p =>
      s"""${p.name} (${getParamInfo(p).pyType}): ${p.doc}"""
    }.mkString("\n")
    s"""|"\""
        |Args:
        |${indent(argLines, 1)}
        |"\""
        |""".stripMargin
  }

  protected lazy val pyParamsDefinitions: String = {
    this.params.map { p =>
      val typeConverterString = getParamInfo(p).typeConverter.map(", typeConverter=" + _).getOrElse("")
      s"""|${p.name} = Param(Params._dummy(), "${p.name}", "${p.doc}"$typeConverterString)
          |""".stripMargin
    }.mkString("\n")
  }

  protected def pyParamArg[T](p: Param[T]): String = {
    (p, this.getDefault(p)) match {
      case (_: ServiceParam[_], _) =>
        s"${p.name}=None,\n${p.name}Col=None"
      case (_: ComplexParam[_], _) | (_, None) =>
        s"${p.name}=None"
      case (_, Some(v)) =>
        s"""${p.name}=${PythonWrappableParam.defaultPythonize(v, p)}"""
    }
  }

  protected def pyParamDefault[T](p: Param[T]): Option[String] = {
    (p, this.getDefault(p)) match {
      case (_: ServiceParam[_], _) =>
        None
      case (_: ComplexParam[_], _) | (_, None) =>
        None
      case (_, Some(v)) =>
        Some(s"""self._setDefault(${pyParamArg(p)})""")
    }
  }

  protected def pyParamsArgs: String =
    this.params.map(pyParamArg(_)).mkString(",\n")

  protected def pyParamsDefaults: String =
    this.params.flatMap(pyParamDefault(_)).mkString("\n")

  protected def pyParamSetter(p: Param[_]): String = {
    val capName = p.name.capitalize
    val docString =
      s"""|"\""
          |Args:
          |    ${p.name}: ${p.doc}
          |"\""
          |""".stripMargin
    p match {
      case _: ServiceParam[_] =>
        s"""|def set$capName(self, value):
            |${indent(docString, 1)}
            |    if isinstance(value, list):
            |        value = SparkContext._active_spark_context._jvm.org.apache.spark.ml.param.ServiceParam.toSeq(value)
            |    self._java_obj = self._java_obj.set$capName(value)
            |    return self
            |
            |def set${capName}Col(self, value):
            |${indent(docString, 1)}
            |    self._java_obj = self._java_obj.set${capName}Col(value)
            |    return self
            |""".stripMargin
      case _ =>
        s"""|def set${p.name.capitalize}(self, value):
            |${indent(docString, 1)}
            |    self._set(${p.name}=value)
            |    return self
            |""".stripMargin
    }
  }

  protected def pyParamsSetters: String =
    this.params.map(pyParamSetter).mkString("\n")

  protected def companionModelClassName: String = {
    val superClass = iterate[Class[_]](this.getClass)(_.getSuperclass)
      .find(c => Set("Estimator", "ProbabilisticClassifier", "Predictor", "BaseRegressor", "Ranker")(
        c.getSuperclass.getSimpleName))
      .get
    val typeArgs = this.getClass.getGenericSuperclass.asInstanceOf[ParameterizedType].getActualTypeArguments
    val modelTypeArg = superClass.getSuperclass.getSimpleName match {
      case "Estimator" =>
        typeArgs.head
      case model if Set("ProbabilisticClassifier", "BaseRegressor", "Predictor", "Ranker")(model) =>
        typeArgs.last
    }
    modelTypeArg.getTypeName
  }

  protected def pyExtraEstimatorMethods: String = {
    this match {
      case _: Estimator[_] =>
        s"""|def _create_model(self, java_model):
            |    try:
            |        model = ${companionModelClassName.split(".".toCharArray).last}(java_obj=java_model)
            |        model._transfer_params_from_java()
            |    except TypeError:
            |        model = ${companionModelClassName.split(".".toCharArray).last}._from_java(java_model)
            |    return model
            |
            |def _fit(self, dataset):
            |    java_model = self._fit_java(dataset)
            |    return self._create_model(java_model)
            |""".stripMargin
      case _ =>
        ""
    }
  }

  protected def pyExtraEstimatorImports: String = {
    this match {
      case _: Estimator[_] =>
        val companionModelImport = companionModelClassName
          .replaceAllLiterally("com.microsoft.ml.spark", "mmlspark")
          .replaceAllLiterally("org.apache.spark", "pyspark")
          .split(".".toCharArray)
        val path = if (companionModelImport.head == "pyspark") {
          companionModelImport.dropRight(1).mkString(".")
        } else {
          companionModelImport.mkString(".")
        }
        val modelName = companionModelImport.last
        s"from $path import $modelName"
      case _ =>
        ""
    }
  }

  protected def pyParamGetter(p: Param[_]): String = {
    val capName = p.name.capitalize
    val docString =
      s"""|"\""
          |Returns:
          |    ${p.name}: ${p.doc}
          |"\""
          |""".stripMargin
    p match {
      case _: DataFrameParam =>
        s"""|
            |def get$capName(self):
            |${indent(docString, 1)}
            |    ctx = SparkContext._active_spark_context
            |    sql_ctx = SQLContext.getOrCreate(ctx)
            |    return DataFrame(self._java_obj.get$capName(), sql_ctx)
            |""".stripMargin
      case _ =>
        s"""|
            |def get$capName(self):
            |${indent(docString, 1)}
            |    return self.getOrDefault(self.${p.name})
            |""".stripMargin
    }
  }

  protected def pyParamsGetters: String =
    this.params.map(pyParamGetter).mkString("\n")

  def pyInitFunc(): String = {
    this match {
//      case _: Model[_] =>
//        ""
      case _ =>
        s"""
           |@keyword_only
           |def __init__(
           |    self,
           |    java_obj=None,
           |${indent(pyParamsArgs, 1)}
           |    ):
           |    super($pyClassName, self).__init__()
           |    if java_obj is None:
           |        self._java_obj = self._new_java_obj("${this.getClass.getName}", self.uid)
           |    else:
           |        self._java_obj = java_obj
           |${indent(pyParamsDefaults, 1)}
           |    if hasattr(self, \"_input_kwargs\"):
           |        kwargs = self._input_kwargs
           |    else:
           |        kwargs = self.__init__._input_kwargs
           |
           |    if java_obj is None:
           |        for k,v in kwargs.items():
           |            if v is not None:
           |                getattr(self, "set" + k[0].upper() + k[1:])(v)
           |""".stripMargin
    }
  }

  //noinspection ScalaStyle
  protected def pythonClass(): String = {
    s"""|$pyCopyrightLines
        |
        |import sys
        |if sys.version >= '3':
        |    basestring = str
        |
        |from pyspark import SparkContext, SQLContext
        |from pyspark.sql import DataFrame
        |from pyspark.ml.param.shared import *
        |from pyspark import keyword_only
        |from pyspark.ml.util import JavaMLReadable, JavaMLWritable
        |from mmlspark.core.serialize.java_params_patch import *
        |from pyspark.ml.wrapper import JavaTransformer, JavaEstimator, JavaModel
        |from pyspark.ml.evaluation import JavaEvaluator
        |from pyspark.ml.common import inherit_doc
        |from mmlspark.core.schema.Utils import *
        |from pyspark.ml.param import TypeConverters
        |from mmlspark.core.schema.TypeConversionUtils import generateTypeConverter, complexTypeConverter
        |$pyExtraEstimatorImports
        |
        |@inherit_doc
        |class $pyClassName(${pyInheritedClasses.mkString(", ")}):
        |${indent(pyClassDoc, 1)}
        |
        |${indent(pyParamsDefinitions, 1)}
        |
        |${indent(pyInitFunc(), 1)}
        |
        |    @keyword_only
        |    def setParams(
        |        self,
        |${indent(pyParamsArgs, 2)}
        |        ):
        |        "\""
        |        Set the (keyword only) parameters
        |        "\""
        |        if hasattr(self, \"_input_kwargs\"):
        |            kwargs = self._input_kwargs
        |        else:
        |            kwargs = self.__init__._input_kwargs
        |        return self._set(**kwargs)
        |
        |    @classmethod
        |    def read(cls):
        |        "\"" Returns an MLReader instance for this class. "\""
        |        return JavaMMLReader(cls)
        |
        |    @staticmethod
        |    def getJavaPackage():
        |        "\"" Returns package name String. "\""
        |        return "${this.getClass.getName}"
        |
        |    @staticmethod
        |    def _from_java(java_stage):
        |        module_name=$pyClassName.__module__
        |        module_name=module_name.rsplit(".", 1)[0] + ".$pyClassNameHelper"
        |        return from_java(java_stage, module_name)
        |
        |${indent(pyParamsSetters, 1)}
        |
        |${indent(pyParamsGetters, 1)}
        |
        |${indent(pyExtraEstimatorMethods, 1)}
        |
        |${indent(pyAdditionalMethods, 1)}
        """.stripMargin
  }

  def makePyFile(): Unit = {
    val importPath = this.getClass.getName.split(".".toCharArray).dropRight(1)
    val srcFolders = importPath.mkString(".")
      .replaceAllLiterally("com.microsoft.ml.spark", "mmlspark").split(".".toCharArray)
    val srcDir = FileUtilities.join((Seq(Config.PySrcDir.toString) ++ srcFolders.toSeq): _*)
    srcDir.mkdirs()
    Files.write(
      FileUtilities.join(srcDir, pyClassName + ".py").toPath,
      pythonClass().getBytes(StandardCharsets.UTF_8))
  }

}

trait Wrappable extends PythonWrappable

