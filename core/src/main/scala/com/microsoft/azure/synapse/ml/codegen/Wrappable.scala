// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.core.env.FileUtilities
import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import com.microsoft.azure.synapse.ml.param._
import org.apache.commons.lang.StringEscapeUtils
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.{Estimator, Model, Transformer}

import scala.reflect.runtime.universe._
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.WeakHashMap


trait BaseWrappable extends Params {

  protected val thisStage: Params = this

  protected lazy val copyrightLines: String =
    s"""|# Copyright (C) Microsoft Corporation. All rights reserved.
        |# Licensed under the MIT License. See LICENSE in project root for information.
        |""".stripMargin

  protected lazy val classNameHelper: String = thisStage.getClass.getName.split(".".toCharArray).last


  protected def companionModelClassName: String = {
    val symbol = scala.reflect.runtime.currentMirror.classSymbol(thisStage.getClass)

    val superClassSymbol = symbol.baseClasses
      .find(s => Set("Estimator", "ProbabilisticClassifier", "Predictor", "BaseRegressor", "Ranker")
        .contains(s.name.toString))
      .getOrElse(throw new NoSuchElementException("Matching superclass was not found: " + symbol.baseClasses))

    val typeArgs = symbol.toType.baseType(superClassSymbol).typeArgs

    val modelTypeArg = superClassSymbol.name.toString match {
      case "Estimator" => typeArgs.head
      case _ => typeArgs.last
    }

    val modelName = modelTypeArg.typeSymbol.asClass.fullName
    modelName
  }

  def getParamInfo(p: Param[_]): ParamInfo[_] = {
    DefaultParamInfo.defaultGetParamInfo(thisStage, p)
  }

}

private[codegen] object PythonStubCodegen {
  val PythonMethodPattern =
    """^\s*def\s+([A-Za-z_][A-Za-z0-9_]*)\(([^)]*)\)(?:\s*->\s*([^:]+))?:\s*$""".r

  private val TypeInfoByStage = new WeakHashMap[Params, Map[String, PythonTypeInfo]]()

  def pythonTypeInfo(stage: Params, param: Param[_]): PythonTypeInfo = synchronized {
    val typeInfoByName = Option(TypeInfoByStage.get(stage)).getOrElse {
      val computed = stage.params
        .map(p => p.name -> DefaultParamInfo.defaultPythonTypeInfo(stage, p))
        .toMap
      TypeInfoByStage.put(stage, computed)
      computed
    }
    typeInfoByName.getOrElse(param.name, DefaultParamInfo.defaultPythonTypeInfo(stage, param))
  }
}

trait PythonWrappable extends BaseWrappable {

  import GenerationUtils._
  import PythonStubCodegen.PythonMethodPattern

  private def getPythonTypeInfo(p: Param[_]): PythonTypeInfo =
    PythonStubCodegen.pythonTypeInfo(thisStage, p)

  def pyAdditionalMethods: String = {
    ""
  }

  protected lazy val pyInternalWrapper = false

  protected lazy val pyClassName: String = {
    if (pyInternalWrapper) {
      "_" + classNameHelper
    } else {
      "" + classNameHelper
    }
  }

  protected lazy val pyObjectBaseClass: String = {
    thisStage match {
      case _: Estimator[_] => "JavaEstimator"
      case _: Model[_] => "JavaModel"
      case _: Transformer => "JavaTransformer"
      case _: Evaluator => "JavaEvaluator"
    }
  }

  protected lazy val pyInheritedClasses: Seq[String] =
    Seq("ComplexParamsMixin", "JavaMLReadable", "JavaMLWritable", pyObjectBaseClass)

  // TODO add default values
  protected lazy val pyClassDoc: String = {
    val argLines = thisStage.params.map { p =>
      s"""${p.name} (${getPythonTypeInfo(p).pyType}): ${p.doc}"""
    }.mkString("\n")
    s"""|"\""
        |Args:
        |${indent(argLines, 1)}
        |"\""
        |""".stripMargin
  }

  private def escape(raw: String): String = {
    StringEscapeUtils.escapeJava(raw)
  }

  protected lazy val pyParamsDefinitions: String = {
    thisStage.params.map { p =>
      val typeConverterString = getParamInfo(p).pyTypeConverter.map(", typeConverter=" + _).getOrElse("")
      p match {
        case sp: ServiceParam[_] =>
          // Helps when we call _transfer_params_to_java the underlying java_obj set service params correctly
          s"""|${sp.name} = Param(Params._dummy(), "${sp.name}", "ServiceParam: ${escape(sp.doc)}"$typeConverterString)
              |""".stripMargin
        case _ =>
          s"""|${p.name} = Param(Params._dummy(), "${p.name}", "${escape(p.doc)}"$typeConverterString)
              |""".stripMargin
      }
    }.mkString("\n")
  }

  protected def pyParamArg[T](p: Param[T]): String = {
    (p, thisStage.getDefault(p)) match {
      case (_: ServiceParam[_], _) =>
        s"${p.name}=None,\n${p.name}Col=None"
      case (_: ComplexParam[_], _) | (_, None) =>
        s"${p.name}=None"
      case (_, Some(v)) =>
        s"""${p.name}=${PythonWrappableParam.pyDefaultRender(v, p)}"""
    }
  }

  protected def pyParamDefault[T](p: Param[T]): Option[String] = {
    (p, thisStage.getDefault(p)) match {
      case (_: ServiceParam[_], _) =>
        None
      case (_: ComplexParam[_], _) | (_, None) =>
        None
      case (_, Some(v)) =>
        Some(s"""self._setDefault(${pyParamArg(p)})""")
    }
  }

  protected def pyParamsArgs: String =
    thisStage.params.map(pyParamArg(_)).mkString(",\n")

  protected def pyParamsDefaults: String =
    thisStage.params.flatMap(pyParamDefault(_)).mkString("\n")

  protected def pyParamSetter(p: Param[_]): String = {
    val capName = p.name.capitalize
    val docString =
      s"""|"\""
          |Args:
          |    ${p.name}: ${p.doc}
          |"\""
          |""".stripMargin
    // scalastyle:off line.size.limit
    p match {
      case sp: ServiceParam[_] =>
        s"""|def set$capName(self, value):
            |${indent(docString, 1)}
            |    if isinstance(value, list):
            |        value = SparkContext._active_spark_context._jvm.com.microsoft.azure.synapse.ml.param.ServiceParam.toSeq(value)
            |    elif isinstance(value, dict):
            |        # Recursively convert Python dict/list to Java LinkedHashMap/ArrayList to preserve order
            |        sc = SparkContext._active_spark_context
            |        jvm = sc._jvm
            |        def _convert(val):
            |            if isinstance(val, dict):
            |                jmap = jvm.java.util.LinkedHashMap()
            |                for k, v in val.items():
            |                    jmap.put(k, _convert(v))
            |                return jmap
            |            elif isinstance(val, list):
            |                jlist = jvm.java.util.ArrayList()
            |                for it in val:
            |                    jlist.add(_convert(it))
            |                return jlist
            |            else:
            |                return val
            |        value = jvm.com.microsoft.azure.synapse.ml.param.ServiceParam.toMap(_convert(value))
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
    // scalastyle:on line.size.limit
  }

  protected def pyParamsSetters: String =
    thisStage.params.map(pyParamSetter).mkString("\n")

  protected def pyExtraEstimatorMethods: String = {
    thisStage match {
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
    thisStage match {
      case _: Estimator[_] =>
        val companionModelImport = companionModelClassName
          .replaceAllLiterally("com.microsoft.azure.synapse.ml", "synapse.ml")
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
      case _: TransformerParam | _: EstimatorParam | _: PipelineStageParam =>
        s"""|
            |def get$capName(self):
            |${indent(docString, 1)}
            |    return JavaParams._from_java(self._java_obj.get$capName())
            |""".stripMargin
      case _: ServiceParam[_] =>
        s"""|
            |def get$capName(self):
            |${indent(docString, 1)}
            |    return self._java_obj.get$capName()
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
    thisStage.params.map(pyParamGetter).mkString("\n")

  private def pyStubOptionalType(pyiType: String): String = {
    if (pyiType == "Any") "Any" else s"Optional[$pyiType]"
  }

  private def pyStubParamArgs(p: Param[_]): Seq[String] = {
    val pyiType = getPythonTypeInfo(p).pyiType
    (p, thisStage.getDefault(p)) match {
      case (_: ServiceParam[_], _) =>
        Seq(
          s"${p.name}: ${pyStubOptionalType(pyiType)} = ...",
          s"${p.name}Col: Optional[str] = ..."
        )
      case (_: ComplexParam[_], _) | (_, None) =>
        Seq(s"${p.name}: ${pyStubOptionalType(pyiType)} = ...")
      case _ =>
        Seq(s"${p.name}: $pyiType = ...")
    }
  }

  private def pyStubParamsArgs: Seq[String] =
    thisStage.params.flatMap(pyStubParamArgs)

  private def pyStubKeywordOnlyMethod(
      methodName: String,
      args: Seq[String],
      returnType: String,
      fluent: Boolean = false): String = {
    val selfArgument = if (fluent) "self: _T" else "self"
    if (args.isEmpty) {
      s"def $methodName($selfArgument) -> $returnType: ..."
    } else {
      s"""|def $methodName(
          |    $selfArgument,
          |    *,
          |${indent(args.mkString(",\n"), 1)}
          |) -> $returnType: ...""".stripMargin
    }
  }

  private def pyStubInitFunc: String =
    pyStubKeywordOnlyMethod(
      "__init__",
      Seq("java_obj: Any = ...") ++ pyStubParamsArgs,
      "None"
    )

  private def pyStubSetParamsFunc: String =
    pyStubKeywordOnlyMethod("setParams", pyStubParamsArgs, "_T", fluent = true)

  private def pyStubParamDefinition(p: Param[_]): String =
    s"${p.name}: Param"

  private def pyStubParamSetter(p: Param[_]): String = {
    val capName = p.name.capitalize
    val pyiType = getPythonTypeInfo(p).pyiType
    p match {
      case _: ServiceParam[_] =>
        s"""|def set$capName(self: _T, value: $pyiType) -> _T: ...
            |def set${capName}Col(self: _T, value: str) -> _T: ...""".stripMargin
      case _ =>
        s"def set$capName(self: _T, value: $pyiType) -> _T: ..."
    }
  }

  private def pyStubParamGetter(p: Param[_]): String = {
    val capName = p.name.capitalize
    s"def get$capName(self) -> ${getPythonTypeInfo(p).pyiType}: ..."
  }

  private def pyStubAdditionalArgument(
      argument: String,
      fluent: Boolean,
      inferredType: Option[String]): String = {
    val trimmed = argument.trim
    if (trimmed == "self" && fluent) {
      "self: _T"
    } else if (trimmed == "self" || trimmed == "cls") {
      trimmed
    } else {
      val hasDefault = trimmed.contains("=")
      val argumentParts = trimmed.split("=", 2).map(_.trim)
      val withoutDefault = argumentParts.head
      val typedArgument = if (withoutDefault.contains(":")) {
        withoutDefault
      } else {
        val argumentType = inferredType.getOrElse("Any")
        val optionalType =
          if (argumentParts.lift(1).contains("None") && argumentType != "Any") {
            s"Optional[$argumentType]"
          } else {
            argumentType
          }
        s"$withoutDefault: $optionalType"
      }
      if (hasDefault) s"$typedArgument = ..." else typedArgument
    }
  }

  private def pyStubAdditionalArgumentTypes(methodName: String, argumentCount: Int): Seq[String] = {
    thisStage.getClass.getMethods
      .filter(method => method.getName == methodName && method.getParameterCount == argumentCount)
      .sortBy(_.toGenericString)
      .headOption
      .map(_.getGenericParameterTypes.toSeq.map(DefaultParamInfo.pythonMethodArgumentType))
      .getOrElse(Seq.fill(argumentCount)("Any"))
  }

  private def isFluentStubMethod(methodName: String): Boolean =
    methodName.startsWith("set") || methodName == "copy"

  private def pyStubAdditionalReturnType(
      methodName: String,
      returnType: String,
      fluent: Boolean): String = {
    Option(returnType).map(_.trim).filter(_.nonEmpty).getOrElse {
      if (fluent) "_T"
      else if (methodName == "clear") "None"
      else "Any"
    }
  }

  private def pyStubAdditionalMethods: String = {
    pyAdditionalMethods.split("\n").flatMap {
      case PythonMethodPattern(methodName, args, returnType) if !methodName.startsWith("_") =>
        val fluent = isFluentStubMethod(methodName)
        val methodArgs = args.split(",").filter(_.trim.nonEmpty)
        val valueArgCount = methodArgs.count(arg => !Set("self", "cls").contains(arg.trim))
        val inferredTypes = pyStubAdditionalArgumentTypes(methodName, valueArgCount).iterator
        val typedArgs = methodArgs.map { argument =>
          val inferredType =
            if (Set("self", "cls").contains(argument.trim)) None
            else Some(inferredTypes.next())
          pyStubAdditionalArgument(argument, fluent, inferredType)
        }.mkString(", ")
        val typedReturn = pyStubAdditionalReturnType(methodName, returnType, fluent)
        Some(s"def $methodName($typedArgs) -> $typedReturn: ...")
      case _ =>
        None
    }.distinct.mkString("\n")
  }

  private def pyStubExtraEstimatorMethods: String = {
    thisStage match {
      case _: Estimator[_] =>
        val modelName = companionModelClassName.split(".".toCharArray).last
        s"""|def _create_model(self, java_model: Any) -> $modelName: ...
            |def _fit(self, dataset: DataFrame) -> $modelName: ...""".stripMargin
      case _ =>
        ""
    }
  }

  def pyInitFunc(): String = {
    s"""
       |@keyword_only
       |def __init__(
       |    self,
       |    java_obj=None,
       |${indent(pyParamsArgs, 1)}
       |    ):
       |    super($pyClassName, self).__init__()
       |    if java_obj is None:
       |        self._java_obj = self._new_java_obj("${thisStage.getClass.getName}", self.uid)
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

  protected def pySetParamsFunc: String = {
    s"""|@keyword_only
        |def setParams(
        |    self,
        |${indent(pyParamsArgs, 1)}
        |    ):
        |    "\""
        |    Set the (keyword only) parameters
        |    "\""
        |    if hasattr(self, "_input_kwargs"):
        |        kwargs = self._input_kwargs
        |    else:
        |        kwargs = self.__init__._input_kwargs
        |    return self._set(**kwargs)
        |""".stripMargin
  }

  //scalastyle:off method.length
  protected def pythonClass(): String = {
    s"""|$copyrightLines
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
        |from synapse.ml.core.platform import running_on_synapse_internal
        |from synapse.ml.core.serialize.java_params_patch import *
        |from pyspark.ml.wrapper import JavaTransformer, JavaEstimator, JavaModel
        |from pyspark.ml.evaluation import JavaEvaluator
        |from pyspark.ml.common import inherit_doc
        |from synapse.ml.core.schema.Utils import *
        |from pyspark.ml.param import TypeConverters
        |from synapse.ml.core.schema.TypeConversionUtils import generateTypeConverter, complexTypeConverter
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
        |${indent(pySetParamsFunc, 1)}
        |
        |    @classmethod
        |    def read(cls):
        |        "\"" Returns an MLReader instance for this class. "\""
        |        return JavaMMLReader(cls)
        |
        |    @staticmethod
        |    def getJavaPackage():
        |        "\"" Returns package name String. "\""
        |        return "${thisStage.getClass.getName}"
        |
        |    @staticmethod
        |    def _from_java(java_stage):
        |        module_name=$pyClassName.__module__
        |        module_name=module_name.rsplit(".", 1)[0] + ".$classNameHelper"
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
  //scalastyle:on method.length

  private def pythonStubClass(): String = {
    val paramDefinitions = thisStage.params.map(pyStubParamDefinition).mkString("\n")
    val paramSetters = thisStage.params.map(pyStubParamSetter).mkString("\n")
    val paramGetters = thisStage.params.map(pyStubParamGetter).mkString("\n")
    s"""|$copyrightLines
        |
        |from typing import Any, Dict, List, Optional, TypeVar
        |
        |from pyspark.ml.evaluation import JavaEvaluator
        |from pyspark.ml.param import Param, ParamMap
        |from pyspark.ml.util import JavaMLReadable, JavaMLWritable
        |from pyspark.ml.wrapper import JavaEstimator, JavaModel, JavaTransformer
        |from pyspark.sql import DataFrame
        |from pyspark.sql.types import DataType
        |from synapse.ml.core.serialize.java_params_patch import ComplexParamsMixin
        |$pyExtraEstimatorImports
        |
        |_T = TypeVar("_T", bound="$pyClassName")
        |
        |class $pyClassName(${pyInheritedClasses.mkString(", ")}):
        |${indent(pyClassDoc, 1)}
        |
        |${indent(paramDefinitions, 1)}
        |
        |${indent(pyStubInitFunc, 1)}
        |
        |${indent(pyStubSetParamsFunc, 1)}
        |
        |    @classmethod
        |    def read(cls) -> Any: ...
        |
        |    @staticmethod
        |    def getJavaPackage() -> str: ...
        |
        |    @staticmethod
        |    def _from_java(java_stage: Any) -> $pyClassName: ...
        |
        |${indent(paramSetters, 1)}
        |
        |${indent(paramGetters, 1)}
        |
        |${indent(pyStubExtraEstimatorMethods, 1)}
        |
        |${indent(pyStubAdditionalMethods, 1)}
        |""".stripMargin
  }

  def makePyFile(conf: CodegenConfig): Unit = {
    val importPath = thisStage.getClass.getName.split(".".toCharArray).dropRight(1)
    val srcFolders = importPath.mkString(".")
      .replaceAllLiterally("com.microsoft.azure.synapse.ml", "synapse.ml").split(".".toCharArray)
    val srcDir = FileUtilities.join(Seq(conf.pySrcDir.toString) ++ srcFolders.toSeq: _*)
    srcDir.mkdirs()
    Files.write(
      FileUtilities.join(srcDir, pyClassName + ".py").toPath,
      pythonClass().getBytes(StandardCharsets.UTF_8))
    Files.write(
      FileUtilities.join(srcDir, pyClassName + ".pyi").toPath,
      pythonStubClass().getBytes(StandardCharsets.UTF_8))
  }

}

trait RWrappable extends BaseWrappable {

  import GenerationUtils._

  protected lazy val rInternalWrapper = false

  protected lazy val rFuncName: String = {
    if (rInternalWrapper) {
      "_ml_" + camelToSnake(classNameHelper)
    } else {
      "ml_" + camelToSnake(classNameHelper)
    }
  }

  protected def rParamsArgs: String =
    thisStage.params.map(rParamArg(_) + ",\n").mkString("")

  protected def rParamArg[T](p: Param[T]): String = {
    (p, thisStage.getDefault(p)) match {
      case (_: ServiceParam[_], _) =>
        s"${p.name}=NULL,\n${p.name}Col=NULL"
      case (_: ComplexParam[_], _) | (_, None) =>
        s"${p.name}=NULL"
      case (_, Some(v)) =>
        s"""${p.name}=${RWrappableParam.rDefaultRender(v, p)}"""
    }
  }

  protected def rDocString: String = {
    val paramDocLines = thisStage.params.map(p =>
      s"#' @param ${p.name} ${p.doc}"
    ).mkString("\n")
    s"""
       |#' $classNameHelper
       |#'
       |$paramDocLines
       |#' @export
       |""".stripMargin
  }

  protected def rSetterLines: String = {
    thisStage.params.map { p =>
      val value = getParamInfo(p).rTypeConverter.map(tc => s"$tc(${p.name})").getOrElse(p.name)
      p match {
        case sp: ServiceParam[_] =>
          val colName = sp.name + "Col"
          getRConditionalSetterLine(colName, colName) + "\n" + getRConditionalSetterLine(sp.name, value)
        case _ =>
          getRConditionalSetterLine(p.name, value)
      }
    }.mkString("\n")
  }

  private def getRConditionalSetterLine(name: String, value: String, setterSuffix: String = ""): String = {
    s"""if (!is.null($name)) mod <- mod %>% """ +
      s"""invoke("set${name.capitalize}$setterSuffix", $value)"""
  }

  protected def rExtraInitLines: String = {
    "unfit.model=FALSE,\nonly.model=FALSE,\n"
  }

  protected def rExtraBodyLines: String = {
    this match {
      case _: Estimator[_] =>
        s"""
           |if (unfit.model)
           |    return(mod)
           |transformer <- mod %>%
           |    invoke("fit", df)
           |scala_transformer_class <- "$companionModelClassName"
           |""".stripMargin
      case _ =>
        s"""
           |transformer <- mod
           |scala_transformer_class <- scala_class
           |""".stripMargin
    }
  }

  protected def rClass(): String = {
    s"""
       |$copyrightLines
       |
       |$rDocString
       |$rFuncName <- function(
       |    x,
       |${indent(rParamsArgs, 1)}
       |${indent(rExtraInitLines, 1)}
       |    uid=random_string("$rFuncName"),
       |    ...)
       |{
       |   if (unfit.model) {
       |       sc <- x
       |    } else {
       |        df <- spark_dataframe(x)
       |        sc <- spark_connection(df)
       |    }
       |    scala_class <- "${thisStage.getClass.getName}"
       |    mod <- invoke_new(sc, scala_class, uid = uid)
       |${indent(rSetterLines, 1)}
       |${indent(rExtraBodyLines, 1)}
       |    if (only.model)
       |        return(sparklyr:::new_ml_transformer(transformer, class=scala_transformer_class))
       |    transformed <- invoke(transformer, "transform", df)
       |    sdf_register(transformed)
       |}
       |""".stripMargin

  }

  def makeRFile(conf: CodegenConfig): Unit = {
    conf.rSrcDir.mkdirs()
    Files.write(
      FileUtilities.join(conf.rSrcDir, rFuncName + ".R").toPath,
      rClass().getBytes(StandardCharsets.UTF_8))
  }

}

trait Wrappable extends PythonWrappable with RWrappable
