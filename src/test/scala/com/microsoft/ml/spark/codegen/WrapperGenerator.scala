// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.codegen

import java.io.File
import java.lang.reflect.{ParameterizedType, Type}

import com.microsoft.ml.spark.codegen.Config._
import com.microsoft.ml.spark.core.env.FileUtilities._
import com.microsoft.ml.spark.core.env.InternalWrapper
import com.microsoft.ml.spark.core.utils.JarLoadingUtils
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.{Estimator, Transformer}

import scala.collection.Iterator.iterate
import scala.language.existentials
import scala.reflect.runtime.universe._

//noinspection ScalaStyle
abstract class WrapperGenerator {

  def wrapperName(myClass: Class[_]): String

  def modelWrapperName(myClass: Class[_], modelName: String): String

  def generateEvaluatorWrapper(entryPoint: Evaluator,
                               entryPointName: String,
                               entryPointQualifiedName: String): WritableWrapper


  def generateEstimatorWrapper(entryPoint: Estimator[_],
                               entryPointName: String,
                               entryPointQualifiedName: String,
                               companionModelName: String,
                               companionModelQualifiedName: String): WritableWrapper


  def generateTransformerWrapper(entryPoint: Transformer,
                                 entryPointName: String,
                                 entryPointQualifiedName: String): WritableWrapper


  def wrapperDir: File

  def writeWrappersToFile(myClass: Class[_], qualifiedClassName: String): Unit = {
    try {
      val classInstance = myClass.newInstance()

      val wrapper: WritableWrapper =
        classInstance match {
          case t: Transformer =>
            val className = wrapperName(myClass)
            generateTransformerWrapper(t, className, qualifiedClassName)
          case e: Estimator[_] =>
            val sc = iterate[Class[_]](myClass)(_.getSuperclass)
              .find(c => Set("Estimator", "ProbabilisticClassifier", "Predictor", "BaseRegressor", "Ranker")(
                c.getSuperclass.getSimpleName))
              .get
            val typeArgs = sc.getGenericSuperclass.asInstanceOf[ParameterizedType]
              .getActualTypeArguments
            val getModelFromGenericType = (modelType: Type) => {
              val modelClass = modelType.getTypeName.split("<").head
              (modelWrapperName(myClass, modelClass.split("\\.").last), modelClass)
            }
            val (modelClass, modelQualifiedClass) = sc.getSuperclass.getSimpleName match {
              case "Estimator" =>
                val modelClass = myClass.getGenericSuperclass.asInstanceOf[ParameterizedType]
                  .getActualTypeArguments.head.getTypeName
                (modelWrapperName(myClass, modelClass.split("\\.").last), modelClass)
              case model if Set("ProbabilisticClassifier", "BaseRegressor", "Predictor", "Ranker")(model) =>
                getModelFromGenericType(typeArgs(2))
            }

            val className = wrapperName(myClass)
            generateEstimatorWrapper(e, className, qualifiedClassName, modelClass, modelQualifiedClass)
          case ev: Evaluator =>
            val className = wrapperName(myClass)
            generateEvaluatorWrapper(ev, className, qualifiedClassName)
          case _ => return
        }
      wrapper.writeWrapperToFile(wrapperDir)
      if (DebugMode) println(s"Generated wrapper for class ${myClass.getSimpleName}")
    } catch {
      // Classes without default constructor
      case ie: InstantiationException =>
        if (DebugMode) println(s"Could not generate wrapper for class ${myClass.getSimpleName}: $ie")
      case e: Exception =>
        println(s"Could not generate wrapper for class ${myClass.getSimpleName}: ${e.printStackTrace}")
    }
  }

  def generateWrappers(): Unit = {
    JarLoadingUtils.WrappableClasses.foreach(cl => writeWrappersToFile(cl, cl.getName))
  }

}

object PySparkWrapperGenerator {
  def apply(): Unit = {
    new PySparkWrapperGenerator().generateWrappers()
  }
}

class PySparkWrapperGenerator extends WrapperGenerator {
  override def wrapperDir: File = new File(PySrcDir, "mmlspark")

  // check if the class is annotated with InternalWrapper
  private[spark] def needsInternalWrapper(myClass: Class[_]): Boolean = {
    val typ: ClassSymbol = runtimeMirror(myClass.getClassLoader).classSymbol(myClass)
    typ.annotations.exists(a => a.tree.tpe =:= typeOf[InternalWrapper])
  }

  def wrapperName(myClass: Class[_]): String = {
    val prefix = if (needsInternalWrapper(myClass)) InternalPrefix else ""
    prefix + myClass.getSimpleName
  }

  def modelWrapperName(myClass: Class[_], modelName: String): String = {
    val prefix = if (needsInternalWrapper(myClass)) InternalPrefix else ""
    prefix + modelName
  }

  def generateEvaluatorWrapper(entryPoint: Evaluator,
                               entryPointName: String,
                               entryPointQualifiedName: String): WritableWrapper = {
    new PySparkEvaluatorWrapper(entryPoint,
      entryPointName,
      entryPointQualifiedName)
  }

  def generateEstimatorWrapper(entryPoint: Estimator[_],
                               entryPointName: String,
                               entryPointQualifiedName: String,
                               companionModelName: String,
                               companionModelQualifiedName: String): WritableWrapper = {
    new PySparkEstimatorWrapper(entryPoint,
      entryPointName,
      entryPointQualifiedName,
      companionModelName,
      companionModelQualifiedName)
  }

  def generateTransformerWrapper(entryPoint: Transformer,
                                 entryPointName: String,
                                 entryPointQualifiedName: String): WritableWrapper = {
    new PySparkTransformerWrapper(entryPoint, entryPointName, entryPointQualifiedName)
  }

}

object SparklyRWrapperGenerator {
  def apply(mmlVer: String): Unit = {
    new SparklyRWrapperGenerator(mmlVer).generateWrappers()
  }
}

class SparklyRWrapperGenerator(mmlVer: String) extends WrapperGenerator {
  override def wrapperDir: File = RSrcDir

  // description file; need to encode version as decimal
  val today = new java.text.SimpleDateFormat("yyyy-MM-dd")
    .format(new java.util.Date())
  val ver0 = "\\.dev|\\+".r.replaceAllIn(mmlVer, "-")
  val ver = "\\.g([0-9a-f]+)".r.replaceAllIn(ver0, m =>
    "." + scala.math.BigInt(m.group(1), 16).toString)
  val actualVer = if (ver == mmlVer) "" else s"\nMMLSparkVersion: $mmlVer"
  RSrcDir.mkdirs()
  writeFile(new File(RSrcDir, "DESCRIPTION"),
    s"""|Package: mmlspark
        |Title: Access to MMLSpark via R
        |Description: Provides an interface to MMLSpark.
        |Version: $ver$actualVer
        |Date: $today
        |Author: Microsoft Corporation
        |Maintainer: MMLSpark Team <mmlspark-support@microsoft.com>
        |URL: https://github.com/Azure/mmlspark
        |BugReports: https://github.com/Azure/mmlspark/issues
        |Depends:
        |    R (>= 2.12.0)
        |Imports:
        |    sparklyr
        |License: MIT
        |""".stripMargin)

  // generate a new namespace file, import sparklyr
  writeFile(SparklyRNamespacePath,
    s"""|$CopyrightLines
        |import(sparklyr)
        |
        |export(sdf_transform)
        |export(smd_model_downloader)
        |export(smd_download_by_name)
        |export(smd_local_models)
        |export(smd_remote_models)
        |export(smd_get_model_name)
        |export(smd_get_model_uri)
        |export(smd_get_model_type)
        |export(smd_get_model_hash)
        |export(smd_get_model_size)
        |export(smd_get_model_input_node)
        |export(smd_get_model_num_layers)
        |export(smd_get_model_layer_names)
        |""".stripMargin)

  def formatWrapperName(name: String): String =
    name.foldLeft((true, ""))((base, c) => {
      val ignoreCaps = base._1
      val partialStr = base._2
      if (!c.isUpper) (false, partialStr + c)
      else if (ignoreCaps) (true, partialStr + c.toLower)
      else (true, partialStr + "_" + c.toLower)
    })._2

  def wrapperName(myClass: Class[_]): String = formatWrapperName(myClass.getSimpleName)

  def modelWrapperName(myClass: Class[_], modelName: String): String = formatWrapperName(modelName)

  def generateEstimatorWrapper(entryPoint: Estimator[_],
                               entryPointName: String,
                               entryPointQualifiedName: String,
                               companionModelName: String,
                               companionModelQualifiedName: String): WritableWrapper = {
    new SparklyREstimatorWrapper(entryPoint,
      entryPointName,
      entryPointQualifiedName,
      companionModelName,
      companionModelQualifiedName)
  }

  def generateTransformerWrapper(entryPoint: Transformer,
                                 entryPointName: String,
                                 entryPointQualifiedName: String): WritableWrapper = {
    new SparklyRTransformerWrapper(entryPoint, entryPointName, entryPointQualifiedName)
  }


  override def generateEvaluatorWrapper(entryPoint: Evaluator, entryPointName: String,
                                        entryPointQualifiedName: String): WritableWrapper =
    new SparklyREvaluatorWrapper(entryPoint, entryPointName, entryPointQualifiedName)

}
