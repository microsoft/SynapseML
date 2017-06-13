// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.codegen

import collection.JavaConverters._
import java.io.File
import java.lang.reflect.{Type, ParameterizedType}
import java.util.jar._

import scala.reflect.internal.util.ScalaClassLoader.URLClassLoader
import org.apache.spark.ml.{Estimator, Transformer}

import com.microsoft.ml.spark.FileUtilities._
import Config._

import scala.language.existentials
import com.microsoft.ml.spark.InternalWrapper
import scala.reflect.runtime.universe._

object PySparkWrapperGenerator {

  // check if the class is annotated with InternalWrapper
  private[spark] def needsInternalWrapper(myClass: Class[_]):Boolean = {
    val typ: ClassSymbol = runtimeMirror(myClass.getClassLoader).classSymbol(myClass)
    typ.annotations.exists(a => a.tree.tpe =:= typeOf[InternalWrapper])
  }

  private[spark] def pyWrapperName(myClass: Class[_]):String = {
    val prefix = if (needsInternalWrapper(myClass)) internalPrefix else ""
    prefix + myClass.getSimpleName
  }

  def writeWrappersToFile(myClass: Class[_], qualifiedClassName: String): Unit = {
    try {
      val classInstance = myClass.newInstance()

      val (wrapper: PySparkWrapper, wrapperTests: PySparkWrapperTest) =
        classInstance match {
          case t: Transformer =>
            val className = pyWrapperName(myClass)
            (new SparkTransformerWrapper(t, className, qualifiedClassName),
             new SparkTransformerWrapperTest(t, className, qualifiedClassName))
          case e: Estimator[_] =>
            var sc = myClass
            while (!Seq("Estimator", "Predictor").contains(sc.getSuperclass.getSimpleName)) {
              sc = sc.getSuperclass
            }
            val typeArgs = sc.getGenericSuperclass.asInstanceOf[ParameterizedType]
              .getActualTypeArguments
            val getModelFromGenericType = (modelType: Type) => {
              val modelClass = modelType.getTypeName.split("<").head
              (modelClass.split("\\.").last, modelClass)
            }
            val (modelClass, modelQualifiedClass) = sc.getSuperclass.getSimpleName match {
              case "Estimator" => getModelFromGenericType(typeArgs.head)
              case "Predictor" => getModelFromGenericType(typeArgs(2))
            }

            val className = pyWrapperName(myClass)
            (new SparkEstimatorWrapper(e, className, qualifiedClassName, modelClass, modelQualifiedClass),
             new SparkEstimatorWrapperTest(e, className, qualifiedClassName, modelClass, modelQualifiedClass))
          case _ => return
        }
      wrapper.writeWrapperToFile(toZipDir)
      wrapperTests.writeWrapperToFile(pyTestDir)
      if (debugMode) println(s"Generated wrapper for class ${myClass.getSimpleName}")
    } catch {
      // Classes without default constructor
      case ie: InstantiationException =>
        if (debugMode) println(s"Could not generate wrapper for class ${myClass.getSimpleName}: $ie")
      // Classes with "private" modifiers on constructors
      case iae: IllegalAccessException =>
        if (debugMode) println(s"Could not generate wrapper for class ${myClass.getSimpleName}: $iae")
      // Classes that require runtime library loading
      case ule: UnsatisfiedLinkError =>
        if (debugMode) println(s"Could not generate wrapper for class ${myClass.getSimpleName}: $ule")
      case e: Exception =>
        println(s"Could not generate wrapper for class ${myClass.getSimpleName}: ${e.printStackTrace}")
    }
  }

  def getWrappersFromJarFile(jarFilePath: String, cl2: URLClassLoader): Unit = {
    val cld = new URLClassLoader(Array(new File(jarFilePath).toURI.toURL), cl2)
    val jfd = new JarFile(jarFilePath)

    using(Seq(cld, jfd)) { s =>
      val cl = s(0).asInstanceOf[URLClassLoader]
      val jarFile = s(1).asInstanceOf[JarFile]
      val _ = jarFile.entries.asScala
        .filter(e => e.getName.endsWith(".class"))
        .map(e => e.getName.replace("/", ".").stripSuffix(".class"))
        .filter(q => {
           val clazz = cl.loadClass(q)
           try {
             clazz.getEnclosingClass == null
           } catch {
             case _: java.lang.NoClassDefFoundError => false
           }
         })
        .foreach(q => writeWrappersToFile(cl.loadClass(q), q))
    }.get
  }

  def apply(): Unit = {
    val jarFiles = outputDir.listFiles.filter(_.getName.endsWith(".jar"))
    val jarUrls = jarFiles.map(_.toURI.toURL)
    using(Seq(new URLClassLoader(jarUrls, this.getClass.getClassLoader))) { s =>
      jarFiles.foreach(f => getWrappersFromJarFile(f.getAbsolutePath, s(0)))
    }.get
  }

}
