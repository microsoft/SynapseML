// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.io.{InputStream, ObjectInputStream, ObjectOutputStream, ObjectStreamClass}

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import FileUtilities._

class ObjectInputStreamContextClassLoader(input: InputStream) extends ObjectInputStream(input) {
  protected override def resolveClass(desc: ObjectStreamClass): Class[_] = {
    try {
      Class.forName(desc.getName, false, Thread.currentThread().getContextClassLoader())
    } catch {
      case _: ClassNotFoundException => super.resolveClass(desc)
    }
  }
}

/** Contains logic for reading and writing objects. */
object ObjectUtilities {

  /** Loads the object from the given path.
    * @param corePath The main path for model to load the object from.
    * @param objectSubPath The path to the object.
    * @param sc The current spark context.
    * @tparam ObjectType The type of the object to load.
    * @return The loaded object.
    */
  def loadObject[ObjectType](corePath: Path, objectSubPath: String, sc: SparkContext): ObjectType = {
    val hadoopConf = sc.hadoopConfiguration
    val inputPath = new Path(corePath, objectSubPath)
    using(Seq(inputPath.getFileSystem(hadoopConf))) { fs =>
      val inputStream = fs(0).open(inputPath)
      using(Seq(new ObjectInputStreamContextClassLoader(inputStream))) {
        objectStream =>
          objectStream(0).readObject().asInstanceOf[ObjectType]
      }.get
    }.get
  }

  /** Writes the object to the given path.
    * @param objToWrite The object to write.
    * @param corePath The main path for model to write the object to.
    * @param objectSubPath The path to the object.
    * @param sc The current spark context.
    * @tparam ObjectType The type of the object to load.
    */
  def writeObject[ObjectType](objToWrite: ObjectType,
                              corePath: Path,
                              objectSubPath: String,
                              sc: SparkContext,
                              overwrite: Boolean): Unit = {
    val hadoopConf = sc.hadoopConfiguration
    val outputPath = new Path(corePath, objectSubPath)
    using(Seq(outputPath.getFileSystem(hadoopConf))) { fs =>
      val outputStream = fs(0).create(outputPath, overwrite)
      using(Seq(new ObjectOutputStream(outputStream))) {
        objectStream =>
          objectStream(0).writeObject(objToWrite)
      }.get
    }.get
  }

}
