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
import org.apache.commons.lang.StringEscapeUtils

import scala.collection.Iterator.iterate

trait BaseWrappable extends DefaultParamInfo {

  protected lazy val copyrightLines: String =
    s"""|# Copyright (C) Microsoft Corporation. All rights reserved.
        |# Licensed under the MIT License. See LICENSE in project root for information.
        |""".stripMargin

  protected lazy val classNameHelper: String = thisStage.getClass.getName.split(".".toCharArray).last

  protected def companionModelClassName: String = {
    val superClass = iterate[Class[_]](thisStage.getClass)(_.getSuperclass)
      .find(c => Set("Estimator", "ProbabilisticClassifier", "Predictor", "BaseRegressor", "Ranker")(
        c.getSuperclass.getSimpleName))
      .get
    val typeArgs = thisStage.getClass.getGenericSuperclass.asInstanceOf[ParameterizedType].getActualTypeArguments
    val modelTypeArg = superClass.getSuperclass.getSimpleName match {
      case "Estimator" =>
        typeArgs.head
      case model if Set("ProbabilisticClassifier", "BaseRegressor", "Predictor", "Ranker")(model) =>
        typeArgs.last
    }
    modelTypeArg.getTypeName
  }

}

trait DotnetWrappable extends BaseWrappable {

  import GenerationUtils._

  protected lazy val dotnetCopyrightLines: String =
    s"""|// Copyright (C) Microsoft Corporation. All rights reserved.
        |// Licensed under the MIT License. See LICENSE in project root for information.
        |""".stripMargin

  protected lazy val dotnetNamespace: String =
    thisStage.getClass.getName.replace("com.microsoft.ml.spark", "Microsoft.ML.Spark")
      .split(".".toCharArray).dropRight(1).mkString(".")

  protected lazy val dotnetInternalWrapper = false

  protected lazy val dotnetClassName: String = {
    if (dotnetInternalWrapper) {
      "_" + classNameHelper
    } else {
      "" + classNameHelper
    }
  }

  protected def unCapitalize(name: String): String = {
    Character.toLowerCase(name.charAt(0)) + name.substring(1)
  }

  protected lazy val dotnetClassNameString: String = s"s_${unCapitalize(dotnetClassName)}ClassName"

  protected lazy val dotnetClassWrapperName: String = "WrapAs" + dotnetClassName

  protected lazy val dotnetObjectBaseClass: String = {
    thisStage match {
      case _: Estimator[_] => s"ScalaEstimator<$dotnetClassName, " +
        s"${companionModelClassName.split(".".toCharArray).last}>"
      case _: Model[_] => s"ScalaModel<$dotnetClassName>"
      case _: Transformer => s"ScalaTransformer<$dotnetClassName>"
      case _: Evaluator => s"ScalaEvaluator<$dotnetClassName>"
    }
  }

  protected def dotnetMLReadWriteMethods: String = {
    s"""|/// <summary>
        |/// Loads the <see cref=\"$dotnetClassName\"/> that was previously saved using Save(string).
        |/// </summary>
        |/// <param name=\"path\">The path the previous <see cref=\"$dotnetClassName\"/> was saved to</param>
        |/// <returns>New <see cref=\"$dotnetClassName\"/> object, loaded from path.</returns>
        |public static $dotnetClassName Load(string path) => $dotnetClassWrapperName(
        |    SparkEnvironment.JvmBridge.CallStaticJavaMethod($dotnetClassNameString, "load", path));
        |
        |/// <summary>
        |/// Saves the object so that it can be loaded later using Load. Note that these objects
        |/// can be shared with Scala by Loading or Saving in Scala.
        |/// </summary>
        |/// <param name="path">The path to save the object to</param>
        |public void Save(string path) => Reference.Invoke("save", path);
        |
        |/// <returns>a <see cref=\"ScalaMLWriter\"/> instance for this ML instance.</returns>
        |public ScalaMLWriter Write() =>
        |    new ScalaMLWriter((JvmObjectReference)Reference.Invoke("write"));
        |
        |/// <returns>an <see cref=\"ScalaMLReader\"/> instance for this ML instance.</returns>
        |public ScalaMLReader<$dotnetClassName> Read() =>
        |    new ScalaMLReader<$dotnetClassName>((JvmObjectReference)Reference.Invoke("read"));
        |""".stripMargin
  }

  protected def dotnetWrapAsTypeMethod: String = {
    s"""|private static $dotnetClassName $dotnetClassWrapperName(object obj) =>
        |    new $dotnetClassName((JvmObjectReference)obj);
        |""".stripMargin
  }

  def dotnetAdditionalMethods: String = {
    ""
  }

  protected def dotnetParamSetter(p: Param[_]): String = {
    val capName = p.name.capitalize
    val docString =
      s"""|/// <summary>
          |/// Sets ${p.name} value for <see cref=\"${p.name}\"/>
          |/// </summary>
          |/// <param name=\"${p.name}\">
          |/// ${p.doc}
          |/// </param>
          |/// <returns> New $dotnetClassName object </returns>""".stripMargin
    p match {
      case sp: ServiceParam[_] =>
        s"""|$docString
            |public $dotnetClassName Set$capName(${getServiceParamInfo(sp).dotnetType} value) =>
            |    $dotnetClassWrapperName(Reference.Invoke(\"set$capName\", (object)value));
            |
            |public $dotnetClassName Set${capName}Col(string value) =>
            |    $dotnetClassWrapperName(Reference.Invoke(\"set${capName}Col\", value));
            |""".stripMargin
      case _ =>
        s"""|$docString
            |public $dotnetClassName Set$capName(${getParamInfo(p).dotnetType} value) =>
            |    $dotnetClassWrapperName(Reference.Invoke(\"set$capName\", (object)value));
            |""".stripMargin
    }
  }

  protected def dotnetParamSetters: String =
    thisStage.params.map(dotnetParamSetter).mkString("\n")

  protected def dotnetParamGetter(p: Param[_]): String = {
    val capName = p.name.capitalize
    val docString =
    s"""|/// <summary>
        |/// Gets ${p.name} value for <see cref=\"${p.name}\"/>
        |/// </summary>
        |/// <returns>
        |/// ${p.name}: ${p.doc}
        |/// </returns>""".stripMargin
    p match {
      case _: ComplexParam[_] =>
        s"""
           |$docString
           |public ${getParamInfo(p).dotnetType} Get$capName() =>
           |    new ${getParamInfo(p).dotnetType}((JvmObjectReference)Reference.Invoke(\"get$capName\"));
           |""".stripMargin
      case sp: ServiceParam[_] =>
        s"""
           |$docString
           |public ${getServiceParamInfo(sp).dotnetType} Get$capName() =>
           |    (${getServiceParamInfo(sp).dotnetType})Reference.Invoke(\"get$capName\");
           |""".stripMargin
      case _ =>
        s"""
           |$docString
           |public ${getParamInfo(p).dotnetType} Get$capName() =>
           |    (${getParamInfo(p).dotnetType})Reference.Invoke(\"get$capName\");
           |""".stripMargin
    }
  }

  protected def dotnetParamGetters: String =
    thisStage.params.map(dotnetParamGetter).mkString("\n")

  //noinspection ScalaStyle
  protected def dotnetExtraMethods: String = {
    thisStage match {
      case _: Estimator[_] =>
        s"""|/// <summary>Fits a model to the input data.</summary>
            |/// <param name=\"dataset\">The <see cref=\"DataFrame\"/> to fit the model to.</param>
            |/// <returns><see cref=\"${companionModelClassName.split(".".toCharArray).last}\"/></returns>
            |override public ${companionModelClassName.split(".".toCharArray).last} Fit(DataFrame dataset) =>
            |    new ${companionModelClassName.split(".".toCharArray).last}(
            |        (JvmObjectReference)Reference.Invoke("fit", dataset));
            |""".stripMargin
      case _ =>
        ""
    }
  }

  protected def dotnetExtraEstimatorImports: String = {
    thisStage match {
      case _: Estimator[_] =>
        val companionModelImport = companionModelClassName
          .replaceAllLiterally("com.microsoft.ml.spark", "Microsoft.ML.Spark")
          .replaceAllLiterally("org.apache.spark", "Microsoft.Spark")
          .split(".".toCharArray)
          .dropRight(1)
          .mkString(".")
        s"using $companionModelImport;"
      case _ =>
        ""
    }
  }

  //noinspection ScalaStyle
  protected def dotnetClass(): String = {
    s"""|$dotnetCopyrightLines
        |
        |using System;
        |using System.Collections.Generic;
        |using Microsoft.Spark.Interop;
        |using Microsoft.Spark.Interop.Ipc;
        |using Microsoft.Spark.Sql;
        |using Microsoft.Spark.Sql.Types;
        |using mmlspark.dotnet.wrapper;
        |using mmlspark.dotnet.utils;
        |$dotnetExtraEstimatorImports
        |
        |namespace $dotnetNamespace
        |{
        |    /// <summary>
        |    /// <see cref=\"$dotnetClassName\"/> implements $dotnetClassName
        |    /// </summary>
        |    public class $dotnetClassName : $dotnetObjectBaseClass, ScalaMLWritable, ScalaMLReadable<$dotnetClassName>
        |    {
        |        private static readonly string $dotnetClassNameString = \"${thisStage.getClass.getName}\";
        |
        |        /// <summary>
        |        /// Creates a <see cref=\"$dotnetClassName\"/> without any parameters.
        |        /// </summary>
        |        public $dotnetClassName() : base($dotnetClassNameString)
        |        {
        |        }
        |
        |        /// <summary>
        |        /// Creates a <see cref=\"$dotnetClassName\"/> with a UID that is used to give the
        |        /// <see cref=\"$dotnetClassName\"/> a unique ID.
        |        /// </summary>
        |        /// <param name=\"uid\">An immutable unique ID for the object and its derivatives.</param>
        |        public $dotnetClassName(string uid) : base($dotnetClassNameString, uid)
        |        {
        |        }
        |
        |        internal $dotnetClassName(JvmObjectReference jvmObject) : base(jvmObject)
        |        {
        |        }
        |
        |${indent(dotnetParamSetters, 2)}
        |${indent(dotnetParamGetters, 2)}
        |${indent(dotnetExtraMethods, 2)}
        |${indent(dotnetMLReadWriteMethods, 2)}
        |${indent(dotnetWrapAsTypeMethod, 2)}
        |${indent(dotnetAdditionalMethods, 2)}
        |    }
        |}
        |
        """.stripMargin
  }

  def makeDotnetFile(conf: CodegenConfig): Unit = {
    val importPath = thisStage.getClass.getName.split(".".toCharArray).dropRight(1)
    val srcFolders = importPath.mkString(".")
      .replaceAllLiterally("com.microsoft.ml.spark", "mmlspark").split(".".toCharArray)
    val srcDir = FileUtilities.join((Seq(conf.dotnetSrcDir.toString) ++ srcFolders.toSeq): _*)
    srcDir.mkdirs()
    Files.write(
      FileUtilities.join(srcDir, dotnetClassName + ".cs").toPath,
      dotnetClass().getBytes(StandardCharsets.UTF_8))
  }

}

trait PythonWrappable extends BaseWrappable {

  import GenerationUtils._

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
    val argLines = thisStage.params.map {
      case sp: ServiceParam[_] =>
        s"""${sp.name} (${getServiceParamInfo(sp).pyType}): ${sp.doc}"""
      case p => s"""${p.name} (${getParamInfo(p).pyType}): ${p.doc}"""
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
      val typeConverterString = p match {
        case sp: ServiceParam[_] => getServiceParamInfo(sp).pyTypeConverter.map(", typeConverter=" + _).getOrElse("")
        case _ => getParamInfo(p).pyTypeConverter.map(", typeConverter=" + _).getOrElse("")
      }
      s"""|${p.name} = Param(Params._dummy(), "${p.name}", "${escape(p.doc)}"$typeConverterString)
          |""".stripMargin
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
      case _: TransformerParam | _: EstimatorParam | _: PipelineStageParam =>
        s"""|
            |def get$capName(self):
            |${indent(docString, 1)}
            |    return JavaParams._from_java(self._java_obj.get$capName())
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

  //noinspection ScalaStyle
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

  def makePyFile(conf: CodegenConfig): Unit = {
    val importPath = thisStage.getClass.getName.split(".".toCharArray).dropRight(1)
    val srcFolders = importPath.mkString(".")
      .replaceAllLiterally("com.microsoft.ml.spark", "mmlspark").split(".".toCharArray)
    val srcDir = FileUtilities.join((Seq(conf.pySrcDir.toString) ++ srcFolders.toSeq): _*)
    srcDir.mkdirs()
    Files.write(
      FileUtilities.join(srcDir, pyClassName + ".py").toPath,
      pythonClass().getBytes(StandardCharsets.UTF_8))
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
       |${paramDocLines}
       |#' @export
       |""".stripMargin
  }

  protected def rSetterLines: String = {
    thisStage.params.map {
      case sp: ServiceParam[_] => {
        val value = getServiceParamInfo(sp).rTypeConverter.map(tc => s"$tc(${sp.name})").getOrElse(sp.name)
        s"""invoke("set${sp.name.capitalize}Col", ${sp.name}Col) %>%\ninvoke("set${sp.name.capitalize}", $value)"""
      }
      case p => {
        val value = getParamInfo(p).rTypeConverter.map(tc => s"$tc(${p.name})").getOrElse(p.name)
        s"""invoke("set${p.name.capitalize}", $value)"""
      }
    }.mkString(" %>%\n")
  }

  protected def rExtraInitLines: String = {
    this match {
      case _: Estimator[_] =>
        "unfit.model=FALSE,\nonly.model=FALSE,\n"
      case _ =>
        "only.model=FALSE,\n"
    }
  }

  protected def rExtraBodyLines: String = {
    this match {
      case _: Estimator[_] =>
        s"""
           |if (unfit.model)
           |    return(mod_parameterized)
           |transformer <- mod_parameterized %>%
           |    invoke("fit", df)
           |scala_transformer_class <- "${companionModelClassName}"
           |""".stripMargin
      case _ =>
        s"""
           |transformer <- mod_parameterized
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
       |    uid=random_string("${rFuncName}"),
       |    ...)
       |{
       |    if (unfit.model) {
       |        sc <- x
       |    } else {
       |        df <- spark_dataframe(x)
       |        sc <- spark_connection(df)
       |    }
       |    scala_class <- "${thisStage.getClass.getName}"
       |    mod <- invoke_new(sc, scala_class, uid = uid)
       |    mod_parameterized <- mod %>%
       |${indent(rSetterLines, 2)}
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

trait Wrappable extends PythonWrappable with RWrappable with DotnetWrappable

