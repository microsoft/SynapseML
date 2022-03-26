// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import com.microsoft.azure.synapse.ml.codegen.Wrappable

import java.lang.reflect.Modifier
import org.sparkproject.guava.reflect.ClassPath

import java.io.{File, InputStreamReader, IOException}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.reflect.{ClassTag, classTag}

/** Contains logic for loading classes. */
object JarLoadingUtils {

  private var CachedAllClasses: List[Class[_]] = List[Class[_]]()

  def className(filename: String): String = {
    if (filename.endsWith(".class")) {
      val classNameEnd = filename.length() - ".class".length()
      filename.substring(0, classNameEnd).replace('/', '.')
    } else {
      filename
    }
  }

  import java.io.BufferedReader
  //import java.util.stream.Collectors
  //import java.util.Collection

  private def getClassFromName(className: String, packageName: String): Class[_] = {
    try {
      Class.forName(packageName + "." + className.substring(0, className.lastIndexOf('.')))
    }
    catch {
      case e: ClassNotFoundException =>
      null
      // handle the exception
    }
  }

  private[ml] val AllClasses = {
    if (CachedAllClasses.isEmpty) CachedAllClasses = getAllClasses
    CachedAllClasses
  }

  private[ml] val WrappableClasses = {
    AllClasses.filter(classOf[Wrappable].isAssignableFrom(_))
  }

  def instantiateServices[T: ClassTag](instantiate: Class[_] => Any, jarName: Option[String]): List[T] = {
    AllClasses
      .filter(classTag[T].runtimeClass.isAssignableFrom(_))
      .filter(c => jarName.forall(c.getResource(c.getSimpleName + ".class").toString.contains(_)))
      .filter(clazz => !Modifier.isAbstract(clazz.getModifiers))
      .map(instantiate(_)).asInstanceOf[List[T]]
  }

  def instantiateServices[T: ClassTag](jarName: Option[String] = None): List[T] = instantiateServices[T]({
    clazz: Class[_] => clazz.getConstructor().newInstance()
  }, jarName)

  def instantiateObjects[T: ClassTag](jarName: Option[String] = None): List[T] = instantiateServices[T](
    { clazz: Class[_] => {
      val cons = clazz.getDeclaredConstructors()(0)
      cons.setAccessible(true)
      cons.newInstance()
    }
    },
    jarName)

  /**
    * Get all relevant classes from the ClassLoader.
    *
    * Note that the spark ClassPath utility only works for ClassLoaders derived from UrlClassLoader (which sbt uses).
    * In IntelliJ, the ClassLoader is not a UrlClassLoader (standard App/Ext/Boot loaders), so in that case
    * we use a different slower method.
    *
    * @return A list of classes in the main package and its modules
    */
  private def getAllClasses: List[Class[_]] = {
    // ClassPath is more performant, so we try that first
    val urlBasedClasses = ClassPath.from(getClass.getClassLoader)
      .getResources.asScala.toList
      .map(ri => className(ri.getResourceName))
      .filter(_.startsWith("com.microsoft.azure.synapse"))
      .flatMap { cn =>
        try {
          Some(Class.forName(cn))
        } catch {
          case _: Throwable => None: Option[Class[_]]
        }
      }

    // If the list is empty, likely we are running in IntelliJ, so use a different slower method to list the classes
    if (!urlBasedClasses.isEmpty) urlBasedClasses else getClassesFromPackage("com.microsoft.azure.synapse")
  }

  /**
    * Scans all classes accessible from the context class loader which belong to the given package and subpackages.
    *
    * @param packageName The base package
    * @return The classes
    * @throws ClassNotFoundException
    * @throws IOException
    */
  @throws[ClassNotFoundException]
  @throws[IOException]
  private def getClassesFromPackage(packageName: String): List[Class[_]] = {
    val classLoader = Thread.currentThread.getContextClassLoader
    assert(classLoader != null)

    // ClassLoader does not expose a class list (except private vars), so scan the jar files directly
    val path = packageName.replace('.', '/')
    val resources = classLoader.getResources(path).asIterator()
    val dirs = new ListBuffer[File]
    for (resource <- resources.asScala) {
      dirs += new File(resource.getFile)
    }

    val classes = new ListBuffer[Class[_]]
    for (directory <- dirs) {
      classes ++= findClassesInDirectory(directory, packageName)
    }
    classes.toList
  }

  /**
    * Recursive method used to find all classes in a given directory and subdirs.
    *
    * @param directory   The base directory
    * @param packageName The package name for classes found inside the base directory
    * @return The classes
    * @throws ClassNotFoundException
    */
  @throws[ClassNotFoundException]
  private def findClassesInDirectory(directory: File, packageName: String): ListBuffer[Class[_]] = {
    val classes = new ListBuffer[Class[_]]
    if (directory.exists) {
      val files = directory.listFiles
      for (file <- files) {
        if (file.isDirectory) {
          assert(!file.getName.contains("."))
          classes ++= findClassesInDirectory(file, packageName + "." + file.getName)
        }
        else if (file.getName.endsWith(".class")) {
          classes += Class.forName(packageName + '.' + file.getName.substring(0, file.getName.length - 6))
        }
      }
    }
    classes
  }
}
