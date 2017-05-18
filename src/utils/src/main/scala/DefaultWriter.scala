// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.hadoop.fs.Path
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.util.{MLReadable, MLReader, MLWritable, MLWriter}
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

trait DefaultWritable[S <: PipelineStage] extends MLWritable {

  val ttag: ClassTag[S]

  val uid: String

  val objectsToSave: List[Any]

  override def write: MLWriter = new DefaultWriter[S](uid, objectsToSave, ttag) {}
}

abstract class DefaultWriter[S <: PipelineStage](val uid: String, objectsToSave: List[Any], ttag: ClassTag[S])
  extends MLWriter {

  override protected def saveImpl(path: String): Unit = {
    val baseDir = PipelineUtilities.makeQualifiedPath(sc, path)
    // Required in order to allow this to be part of an ML pipeline
    PipelineUtilities.saveMetadata(uid, ttag, new Path(baseDir, "metadata").toString, sc, shouldOverwrite)

    val objectsToWrite = objectsToSave
    objectsToWrite.zipWithIndex.foreach { case (obj, i) =>
      val path = WriterUtilities.getPath(baseDir, i)
      obj match {
        case stage: PipelineStage =>
          WriterUtilities.writeStage(stage, path, shouldOverwrite)
        case writable: MLWritable =>
          WriterUtilities.writeMLWritable(writable, path, shouldOverwrite)
        case df: Dataset[_] =>
          WriterUtilities.writeDF(df.toDF(), path, shouldOverwrite)
        case any =>
          WriterUtilities.writeObject(any, path, sc, shouldOverwrite)

      }
    }
  }
}

trait DefaultReadable[T <: DefaultWritable[_]] extends MLReadable[T] {

  val typesToRead: List[Class[_]]

  val ttag: ClassTag[T]

  private def instantiate[S](clazz: Class[_])(args: AnyRef*): S = {
    val constructor = clazz.getConstructors()(0)
    constructor.newInstance(args: _*).asInstanceOf[S]
  }

  def constructor: List[AnyRef] => T = instantiate[T](ttag.runtimeClass)

  override def read: MLReader[T] = new DefaultReader(typesToRead, constructor)

}

class DefaultReader[T](val objectsToLoad: List[Class[_]], constructor: List[AnyRef] => T)
  extends MLReader[T] {

  override def load(path: String): T = {
    val baseDir = PipelineUtilities.makeQualifiedPath(sc, path)

    val loadedObjects = objectsToLoad.zipWithIndex.map { case (clazz, i) =>
      val path = WriterUtilities.getPath(baseDir, i)
      if (classOf[PipelineStage].isAssignableFrom(clazz)) {
        WriterUtilities.loadStage(path)
      } else if (classOf[MLReadable[_]].isAssignableFrom(clazz)) {
        val cons = clazz.getDeclaredConstructors()(0)
        cons.setAccessible(true)
        val factory = cons.newInstance().asInstanceOf[MLReadable[_]]
        WriterUtilities.loadMLReadable(factory, path)
      } else if (classOf[Dataset[_]].isAssignableFrom(clazz)) {
        WriterUtilities.loadDF(path, sparkSession)
      } else {
        WriterUtilities.loadObject[AnyRef](path, sc)
      }
    }
    constructor(loadedObjects)
  }
}
