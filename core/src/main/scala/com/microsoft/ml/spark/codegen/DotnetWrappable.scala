// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.codegen

import com.microsoft.ml.spark.core.env.FileUtilities
import com.microsoft.ml.spark.core.serialize.ComplexParam
import org.apache.commons.lang.StringUtils.capitalize
import org.apache.spark.ml.{Estimator, Model, Pipeline, PipelineStage, Transformer}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.collection.JavaConverters._

object PipelineHelper {

  def setStages(pipeline: Pipeline, value: java.util.ArrayList[_ <: PipelineStage]): Pipeline =
    pipeline.setStages(value.asScala.toArray)
}

trait DotnetWrappable extends BaseWrappable {

  import DefaultParamInfo._
  import GenerationUtils._

  protected lazy val dotnetCopyrightLines: String =
    s"""|// Copyright (C) Microsoft Corporation. All rights reserved.
        |// Licensed under the MIT License. See LICENSE in project root for information.
        |""".stripMargin

  protected lazy val dotnetNamespace: String =
    thisStage.getClass.getName
      .replace("com.microsoft.ml.spark", "Microsoft.ML.Spark")
      .replace("org.apache.spark.ml", "Microsoft.Spark.ML")
      .split(".".toCharArray).map(capitalize).dropRight(1).mkString(".")

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
      case _: Estimator[_] => s"ScalaEstimator<${companionModelClassName.split(".".toCharArray).last}>"
      case _: Model[_] => s"ScalaModel<$dotnetClassName>"
      case _: Transformer => s"ScalaTransformer"
      case _: Evaluator => s"ScalaEvaluator"
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

  //noinspection ScalaStyle
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
      case _: DataTypeParam =>
        s"""|$docString
            |public $dotnetClassName Set$capName(${getParamInfo(p).dotnetType} value) =>
            |    $dotnetClassWrapperName(Reference.Invoke(\"set$capName\",
            |    DataType.FromJson(Reference.Jvm, value.Json)));
            |""".stripMargin
      case _: EstimatorParam | _: ModelParam =>
        s"""|$docString
            |public $dotnetClassName Set${capName}<M>(${getParamInfo(p).dotnetType} value) where M : ScalaModel<M> =>
            |    $dotnetClassWrapperName(Reference.Invoke(\"set$capName\", (object)value));
            |""".stripMargin
      case _: ArrayParamMapParam | _: TransformerArrayParam | _: EstimatorArrayParam =>
        s"""|$docString
            |public $dotnetClassName Set$capName(${getParamInfo(p).dotnetType} value)
            |{
            |    var arrayList = new ArrayList(SparkEnvironment.JvmBridge);
            |    foreach (var v in value)
            |    {
            |        arrayList.Add(v);
            |    }
            |    return $dotnetClassWrapperName(Reference.Invoke(\"set$capName\", (object)arrayList));
            |}
            |""".stripMargin
      // TODO: Fix UDF & UDPyF confusion
      case _: UDFParam | _: UDPyFParam =>
        s"""|$docString
            |public $dotnetClassName Set$capName(object value) =>
            |    $dotnetClassWrapperName(Reference.Invoke(\"set$capName\", value));
            |""".stripMargin
      // TODO: Fix these objects
      case _: ParamSpaceParam | _: BallTreeParam | _: ConditionalBallTreeParam =>
        s"""|$docString
            |public $dotnetClassName Set$capName(object value) =>
            |    $dotnetClassWrapperName(Reference.Invoke(\"set$capName\", value));
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

  //noinspection ScalaStyle
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
      case sp: ServiceParam[_] =>
        val dType = getServiceParamInfo(sp).dotnetType
        dType match {
          case "TimeSeriesPoint[]" |
               "TargetInput[]" |
               "TextAndTranslation[]" =>
            s"""
               |$docString
               |public $dType Get$capName()
               |{
               |    JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke(\"get$capName\");
               |    JvmObjectReference[] jvmObjects = (JvmObjectReference[])jvmObject.Invoke("array");
               |    $dType results =
               |        new ${dType.substring(0, dType.length - 2)}[jvmObjects.Length];
               |    for (int i = 0; i < results.Length; i++)
               |    {
               |        results[i] = new ${dType.substring(0, dType.length - 2)}(jvmObjects[i]);
               |    }
               |    return results;
               |}
               |""".stripMargin
          case _ =>
            s"""
               |$docString
               |public $dType Get$capName() =>
               |    ($dType)Reference.Invoke(\"get$capName\");
               |""".stripMargin
        }
      case _: DataFrameParam =>
        s"""
           |$docString
           |public ${getParamInfo(p).dotnetType} Get$capName() =>
           |    new ${getParamInfo(p).dotnetType}((JvmObjectReference)Reference.Invoke(\"get$capName\"));
           |""".stripMargin
      case _: DataTypeParam =>
        s"""
           |$docString
           |public ${getParamInfo(p).dotnetType} Get$capName()
           |{
           |    JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke(\"get$capName\");
           |    string json = (string)jvmObject.Invoke(\"json\");
           |    return DataType.ParseDataType(json);
           |}
           |""".stripMargin
      case _: TypedIntArrayParam | _: TypedDoubleArrayParam =>
        s"""
           |$docString
           |public ${getParamInfo(p).dotnetType} Get$capName()
           |{
           |    JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke(\"get$capName\");
           |    return (${getParamInfo(p).dotnetType})jvmObject.Invoke(\"array\");
           |}
           |""".stripMargin
      case _: EstimatorParam | _: ModelParam | _: TransformerParam | _: EvaluatorParam | _: PipelineStageParam =>
        val dType = p match {
          case _: EstimatorParam | _: ModelParam =>
            getParamInfo(p).dotnetType.substring(5, getParamInfo(p).dotnetType.length - 3) + "<object>"
          case _ => getParamInfo(p).dotnetType
        }
        s"""
           |$docString
           |public $dType Get$capName()
           |{
           |    JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke(\"get$capName\");
           |    var (constructorClass, methodName) = Helper.GetUnderlyingType(jvmObject);
           |    Type type = Type.GetType(constructorClass);
           |    MethodInfo method = type.GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Static);
           |    return ($dType)method.Invoke(null, new object[] {jvmObject});
           |}
           |""".stripMargin
      case _: ArrayParamMapParam =>
        s"""
           |$docString
           |public ParamMap[] Get$capName()
           |{
           |    JvmObjectReference[] jvmObjects = (JvmObjectReference[])Reference.Invoke(\"get$capName\");
           |    ParamMap[] result = new ParamMap[jvmObjects.Length];
           |    for (int i=0; i < jvmObjects.Length; i++)
           |    {
           |        result[i] = new ParamMap(jvmObjects[i]);
           |    }
           |    return result;
           |}
           |""".stripMargin
      case _: TransformerArrayParam | _: EstimatorArrayParam =>
        val dType = getParamInfo(p).dotnetType.substring(0, getParamInfo(p).dotnetType.length - 2)
        s"""
           |$docString
           |public $dType[] Get$capName()
           |{
           |    JvmObjectReference[] jvmObjects = (JvmObjectReference[])Reference.Invoke(\"get$capName\");
           |    $dType[] result = new $dType[jvmObjects.Length];
           |    for (int i=0; i < jvmObjects.Length; i++)
           |    {
           |        var (constructorClass, methodName) = Helper.GetUnderlyingType(jvmObjects[i]);
           |        Type type = Type.GetType(constructorClass);
           |        MethodInfo method = type.GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Static);
           |        result[i] = ($dType)method.Invoke(null, new object[] {jvmObjects[i]});
           |    }
           |    return result;
           |}
           |""".stripMargin
      case _: ComplexParam[_] =>
        s"""
           |$docString
           |public object Get$capName() => Reference.Invoke(\"get$capName\");
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
          .replaceAllLiterally("org.apache.spark.ml", "Microsoft.Spark.ML")
          .replaceAllLiterally("org.apache.spark", "Microsoft.Spark")
          .split(".".toCharArray)
          .map(capitalize)
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
        |using System.Linq;
        |using System.Reflection;
        |using Microsoft.Spark.ML.Feature.Param;
        |using Microsoft.Spark.Interop;
        |using Microsoft.Spark.Interop.Ipc;
        |using Microsoft.Spark.Interop.Internal.Java.Util;
        |using Microsoft.Spark.Sql;
        |using Microsoft.Spark.Sql.Types;
        |using MMLSpark.Dotnet.Wrapper;
        |using MMLSpark.Dotnet.Utils;
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
