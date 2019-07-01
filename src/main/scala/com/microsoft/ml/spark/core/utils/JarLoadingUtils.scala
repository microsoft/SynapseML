// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.core.utils

import java.io.{IOException, InputStream, ObjectInputStream, ObjectStreamClass}
import java.net.{URL, URLClassLoader}
import java.util.jar.JarFile

import com.microsoft.ml.spark.core.env.FileUtilities
import com.microsoft.ml.spark.core.env.FileUtilities.File
import org.scalatest.exceptions.TestFailedException

import scala.reflect.{ClassTag, _}
import com.microsoft.ml.spark.core.env.FileUtilities._
import org.spark_project.guava.reflect.ClassPath
import org.spark_project.guava.reflect.ClassPath.ClassInfo

import collection.JavaConverters._
import scala.util.Try

/** Contains logic for loading classes. */
object JarLoadingUtils {

  def className(filename: String): String = {
    val classNameEnd = filename.length() - ".class".length()
    filename.substring(0, classNameEnd).replace('/', '.')
  }

   private lazy val allClasses = {
    ClassPath.from(getClass.getClassLoader)
      //.getTopLevelClassesRecursive("com.microsoft").asScala.toList
      .getResources().asScala.toList
      .map(ri => className(ri.getResourceName))
      .filter(_.startsWith("com.microsoft.ml"))
      .flatMap { cn =>
        try {
          Some(Class.forName(cn))
        } catch {
          case _: Throwable => None: Option[Class[_]]
        }
      }
  }

  private def catchInstantiationErrors[T](clazz: Class[_], func: Function[Class[_], T], debug: Boolean): Option[T] = {
    def log(message: String) = {
      if (debug) println(message)
    }

    try {
      Some(func(clazz))
    } catch {
      // Classes without default constructor
      case ie: InstantiationException =>
        log(s"Could not generate wrapper without default constructor for " +
          s"class ${clazz.getSimpleName}: $ie")
        None
      // Classes with "private" modifiers on constructors
      case iae: IllegalAccessException =>
        log(s"Could not generate wrapper due to private modifiers or constructors for " +
          s"class ${clazz.getSimpleName}: $iae")
        None
      case ncd: NoClassDefFoundError =>
        log(s"Could not generate wrapper because no class definition found for class " +
          s"${clazz.getSimpleName}: $ncd")
        None
      case ule: UnsatisfiedLinkError =>
        log(s"Could not generate wrapper due to link error from: " +
          s"${clazz.getSimpleName}: $ule")
        None
      case e: TestFailedException =>
        log(s"Could not generate wrapper due to TestFailedException")
        None
      case e: Exception =>
        log(s"Could not generate wrapper for class ${clazz.getSimpleName}: ${e.printStackTrace()}")
        None
    }
  }

  def load[T: ClassTag](instantiate: Class[_] => Any, debug: Boolean): List[T] = {
    allClasses.filter(lc => classTag[T].runtimeClass.isAssignableFrom(lc)).flatMap { lc =>
      catchInstantiationErrors(lc, instantiate, debug)
    }.asInstanceOf[List[T]]
  }

  def loadClass[T: ClassTag](debug: Boolean): List[T] = load[T](lc => lc.newInstance(), debug)

  def loadTest[T: ClassTag](instantiate: Class[_] => Any, debug: Boolean): List[T] = {
    val testClasses = allClasses.filter(lc => classTag[T].runtimeClass.isAssignableFrom(lc))
    testClasses.flatMap { lc =>
      catchInstantiationErrors(lc, instantiate, debug)
    }.asInstanceOf[List[T]]
  }

  def loadTestClass[T: ClassTag](debug: Boolean): List[T] = loadTest[T](lc => lc.newInstance(), debug)

  def loadObject[T: ClassTag](debug: Boolean): List[T] = load[T](
    lc => {
      val cons = lc.getDeclaredConstructors()(0)
      cons.setAccessible(true)
      cons.newInstance()
    }
    ,
    debug)

}

class ContextObjectInputStream(input: InputStream) extends ObjectInputStream(input) {
  protected override def resolveClass(desc: ObjectStreamClass): Class[_] = {
    try {
      Class.forName(desc.getName, false, Thread.currentThread().getContextClassLoader)
    } catch {
      case _: ClassNotFoundException => super.resolveClass(desc)
    }
  }
}
