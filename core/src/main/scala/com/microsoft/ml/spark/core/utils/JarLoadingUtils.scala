// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.core.utils

import java.lang.reflect.Modifier

import com.microsoft.ml.spark.codegen.Wrappable
import org.sparkproject.guava.reflect.ClassPath
import scala.collection.JavaConverters._
import scala.reflect.{ClassTag, classTag}

/** Contains logic for loading classes. */
object JarLoadingUtils {

  def className(filename: String): String = {
    if (filename.endsWith(".class")) {
      val classNameEnd = filename.length() - ".class".length()
      filename.substring(0, classNameEnd).replace('/', '.')
    } else {
      filename
    }
  }

  private[spark] val AllClasses = {
    ClassPath.from(getClass.getClassLoader)
      .getResources.asScala.toList
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

  private[spark] val WrappableClasses = {
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

}
