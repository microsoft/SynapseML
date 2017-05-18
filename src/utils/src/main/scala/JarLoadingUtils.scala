// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.net.URLClassLoader
import java.util.jar.JarFile

import FileUtilities._

import scala.reflect.ClassTag
import scala.reflect._
import collection.JavaConverters._

/**
  * Contains logic for loading classes
  */
object JarLoadingUtils {

  private val jarRelPath = "target/scala-" + sys.env("SCALA_VERSION")
  private val testRelPath = "test-classes"
  private val projectRoots = "project/project-roots.txt"

  private val outputDirs = {
    val topDir = List(".", "..", "src").find(root => new File(root, projectRoots).exists)
    if (topDir.isEmpty) {
      sys.error(s"Could not find roots file at $projectRoots")
    }
    val rootsFile = new File(topDir.get, projectRoots)
    val roots = readFile(rootsFile, _.getLines.toList)
    roots.map { root =>
      new File(new File(topDir.get, root), jarRelPath)
    }
  }

  private val testOutputDirs = {
    outputDirs.flatMap(dir => {
      val filePath = new File(dir, testRelPath)
      if (filePath.exists()) {
        Some(filePath)
      } else {
        None
      }
    })
  }

  private val jarFileLocs = outputDirs.flatMap(dir =>
    FileUtilities.allFiles(dir, file => file.getName.endsWith(".jar")))

  private val testFileLocs = testOutputDirs.flatMap(dir =>
    FileUtilities.allFiles(dir, file => file.getName.endsWith(".class"))
      .map(file => file.getCanonicalPath.replace(dir.getCanonicalPath + "/", "")))

  private val jarURLs = jarFileLocs.map(_.toURI.toURL)

  val classLoader = new URLClassLoader(jarURLs.union(testOutputDirs
    .map(file => new File(file.getCanonicalPath).toURI.toURL)).toArray,
    this.getClass.getClassLoader)

  private lazy val loadedClasses: List[Class[_]] = {
    val jarFiles = jarFileLocs.map(jf => new JarFile(jf.getAbsolutePath))
    try {
      val classNames = jarFiles.flatMap(_.entries().asScala)
        .filter(je => je.getName.endsWith(".class"))
        .map(je => je.getName.replace("/", ".").stripSuffix(".class"))
      classNames.map(name => classLoader.loadClass(name))
    } finally {
      jarFiles.foreach(jf => jf.close())
    }
  }

  private lazy val loadedTestClasses: List[Class[_]] = {
    val classNames = testFileLocs.map(je => je.stripSuffix(".class").replace("/", "."))
    classNames.map(name => {
      try {
        classLoader.loadClass(name)
      } catch {
        case e: Throwable => { println(s"Encountered error $e when loading class"); null }
      }
    }).filter(_ != null)
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
      case e: Exception =>
        log(s"Could not generate wrapper for class ${clazz.getSimpleName}: ${e.printStackTrace()}")
        None
    }
  }

  def load[T: ClassTag](instantiate: Class[_] => Any, debug: Boolean): List[T] = {
    loadedClasses.filter(lc => classTag[T].runtimeClass.isAssignableFrom(lc)).flatMap { lc =>
      catchInstantiationErrors(lc, instantiate, debug)
    }.asInstanceOf[List[T]]
  }

  def loadClass[T: ClassTag](debug: Boolean): List[T] = load[T](lc => lc.newInstance(), debug)

  def loadTest[T: ClassTag](instantiate: Class[_] => Any, debug: Boolean): List[T] = {
    loadedTestClasses.filter(lc => classTag[T].runtimeClass.isAssignableFrom(lc)).flatMap { lc =>
      catchInstantiationErrors(lc, instantiate, debug)
    }.asInstanceOf[List[T]]
  }

  def loadTestClass[T: ClassTag](debug: Boolean): List[T] = loadTest[T](lc => lc.newInstance(), debug)

  def loadObject[T: ClassTag](debug: Boolean): List[T] = load[T](
    lc =>{
      val cons = lc.getDeclaredConstructors()(0)
      cons.setAccessible(true)
      cons.newInstance()}
    ,
    debug)

}
