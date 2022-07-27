// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.core.env.FileUtilities
import com.microsoft.azure.synapse.ml.param.{ServiceParam, WrappableParam}
import org.apache.commons.lang.StringUtils.capitalize
import org.apache.spark.ml._
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.collection.JavaConverters._

// TODO: delete as moved this to dotnet/spark repo
object DotnetHelper {

  def setPipelineStages(pipeline: Pipeline, value: java.util.ArrayList[_ <: PipelineStage]): Pipeline =
    pipeline.setStages(value.asScala.toArray)

  def convertToJavaMap(value: Map[_, _]): java.util.Map[_, _] = value.asJava

  // TODO: support more types for UntypedArrayParam
  def mapScalaToJava(value: java.lang.Object): Any = {
    value match {
      case i: java.lang.Integer => i.toInt
      case d: java.lang.Double => d.toDouble
      case f: java.lang.Float => f.toFloat
      case b: java.lang.Boolean => b.booleanValue()
      case l: java.lang.Long => l.toLong
      case s: java.lang.Short => s.toShort
      case by: java.lang.Byte => by.toByte
      case c: java.lang.Character => c.toChar
      case _ => value
    }
  }
}

trait DotnetWrappable extends BaseWrappable {

  import GenerationUtils._

  protected lazy val dotnetCopyrightLines: String =
    s"""|// Copyright (C) Microsoft Corporation. All rights reserved.
        |// Licensed under the MIT License. See LICENSE in project root for information.
        |""".stripMargin

  protected lazy val dotnetNamespace: String =
    thisStage.getClass.getName
      .replace("com.microsoft.azure.synapse.ml", "Synapse.ML")
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

  protected lazy val dotnetClassNameString: String = "s_className"

  protected lazy val dotnetClassWrapperName: String = "WrapAs" + dotnetClassName

  protected lazy val dotnetObjectBaseClass: String = {
    thisStage match {
      case _: Estimator[_] => s"JavaEstimator<${companionModelClassName.split(".".toCharArray).last}>"
      case _: Model[_] => s"JavaModel<$dotnetClassName>"
      case _: Transformer => s"JavaTransformer"
      case _: Evaluator => s"JavaEvaluator"
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
        |/// <returns>a <see cref=\"JavaMLWriter\"/> instance for this ML instance.</returns>
        |public JavaMLWriter Write() =>
        |    new JavaMLWriter((JvmObjectReference)Reference.Invoke("write"));
        |
        |/// <summary>
        |/// Get the corresponding JavaMLReader instance.
        |/// </summary>
        |/// <returns>an <see cref=\"JavaMLReader&lt;$dotnetClassName&gt;\"/> instance for this ML instance.</returns>
        |public JavaMLReader<$dotnetClassName> Read() =>
        |    new JavaMLReader<$dotnetClassName>((JvmObjectReference)Reference.Invoke("read"));
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
          |/// Sets value for ${p.name}
          |/// </summary>
          |/// <param name=\"value\">
          |/// ${p.doc}
          |/// </param>
          |/// <returns> New $dotnetClassName object </returns>""".stripMargin
    p match {
      // TODO: Fix UDF & UDPyF confusion; ParamSpaceParam, BallTreeParam, ConditionalBallTreeParam type
      case sp: ServiceParam[_] =>
        s"""|$docString
            |${sp.dotnetSetter(dotnetClassName, capName, dotnetClassWrapperName)}
            |
            |${docString.replaceFirst(sp.name, s"${sp.name} column")}
            |${sp.dotnetSetterForSrvParamCol(dotnetClassName, capName, dotnetClassWrapperName)}
            |""".stripMargin
      case wp: WrappableParam[_] =>
        s"""|$docString
            |${wp.dotnetSetter(dotnetClassName, capName, dotnetClassWrapperName)}
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
          |/// Gets ${p.name} value
          |/// </summary>
          |/// <returns>
          |/// ${p.name}: ${p.doc}
          |/// </returns>""".stripMargin
    p match {
      case wp: WrappableParam[_] =>
        s"""|$docString
            |${wp.dotnetGetter(capName)}
            |""".stripMargin
      case _ =>
        s"""|$docString
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
          .replaceAllLiterally("com.microsoft.azure.synapse.ml", "Synapse.ML")
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
        |using Microsoft.Spark.ML.Feature;
        |using Microsoft.Spark.ML.Feature.Param;
        |using Microsoft.Spark.Interop;
        |using Microsoft.Spark.Interop.Ipc;
        |using Microsoft.Spark.Interop.Internal.Java.Util;
        |using Microsoft.Spark.Sql;
        |using Microsoft.Spark.Sql.Types;
        |using Microsoft.Spark.Utils;
        |using SynapseML.Dotnet.Utils;
        |using Synapse.ML.LightGBM.Param;
        |$dotnetExtraEstimatorImports
        |
        |namespace $dotnetNamespace
        |{
        |    /// <summary>
        |    /// <see cref=\"$dotnetClassName\"/> implements $dotnetClassName
        |    /// </summary>
        |    public class $dotnetClassName : $dotnetObjectBaseClass, IJavaMLWritable, IJavaMLReadable<$dotnetClassName>
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
      .replaceAllLiterally("com.microsoft.azure.synapse.ml", "synapse.ml").split(".".toCharArray)
    val srcDir = FileUtilities.join((Seq(conf.dotnetSrcDir.toString) ++ srcFolders.toSeq): _*)
    if (!srcDir.exists()) {
      srcDir.mkdirs()
    }
    Files.write(
      FileUtilities.join(srcDir, dotnetClassName + ".cs").toPath,
      dotnetClass().getBytes(StandardCharsets.UTF_8))
  }

}
