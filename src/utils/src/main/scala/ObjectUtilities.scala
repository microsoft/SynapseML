// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.io.{InputStream, ObjectInputStream, ObjectOutputStream, ObjectStreamClass}

import com.microsoft.ml.spark.FileUtilities._
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.util.{MLReadable, MLWritable}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

class ObjectInputStreamContextClassLoader(input: InputStream) extends ObjectInputStream(input) {
  protected override def resolveClass(desc: ObjectStreamClass): Class[_] = {
    try {
      Class.forName(desc.getName, false, Thread.currentThread().getContextClassLoader)
    } catch {
      case _: ClassNotFoundException => super.resolveClass(desc)
    }
  }
}

/**
  * Contains logic for reading and writing objects.
  */
object WriterUtilities {

  def getPath(baseDir: Path, i: Int): Path = {
    new Path(baseDir, s"data_$i")
  }

  /**
    * Writes the object to the given path.
    *
    * @param objToWrite The object to write.
    * @param outputPath Where to write the object
    * @param sc         The current spark context.
    * @tparam T The type of the object to load.
    */
  def writeObject[T](objToWrite: T,
                     outputPath: Path,
                     sc: SparkContext,
                     overwrite: Boolean): Unit = {
    val hadoopConf = sc.hadoopConfiguration
    using(outputPath.getFileSystem(hadoopConf)) { fs =>
      val outputStream = fs.create(outputPath, overwrite)
      using(new ObjectOutputStream(outputStream)) {
        objectStream =>
          objectStream.writeObject(objToWrite)
      }.get
    }.get
  }

  /**
    * Loads the object from the given path.
    *
    * @param inputPath The main path for model to load the object from.
    * @param sc        The current spark context.
    * @tparam T The type of the object to load.
    * @return The loaded object.
    */
  def loadObject[T](inputPath: Path, sc: SparkContext): T = {
    val hadoopConf = sc.hadoopConfiguration
    using(inputPath.getFileSystem(hadoopConf)) { fs =>
      val inputStream = fs.open(inputPath)
      using(new ObjectInputStreamContextClassLoader(inputStream)) {
        objectStream => objectStream.readObject().asInstanceOf[T]
      }.get
    }.get
  }

  def writeDF(df: DataFrame, outputPath: Path, overwrite: Boolean): Unit = {
    val saveMode =
      if (overwrite) SaveMode.Overwrite
      else SaveMode.ErrorIfExists

    df.write.mode(saveMode).parquet(outputPath.toString)
  }

  def loadDF(inputPath: Path, spark: SparkSession): Dataset[_] = {
    spark.read.format("parquet").load(inputPath.toString)
  }

  def writeStage(stage: PipelineStage, outputPath: Path, overwrite: Boolean): Unit = {
    val pipe = new Pipeline().setStages(Array(stage))
    writeMLWritable(pipe, outputPath, overwrite)
  }

  def writeMLWritable(stage: MLWritable, outputPath: Path, overwrite: Boolean): Unit = {
    val writer = if (overwrite) {
      stage.write.overwrite()
    } else {
      stage.write
    }
    writer.save(outputPath.toString)
  }

  def loadMLReadable[T](factory: MLReadable[_], inputPath: Path): T = {
    factory.load(inputPath.toString).asInstanceOf[T]
  }

  def loadStage[T](inputPath: Path): T = {
    Pipeline.load(inputPath.toString).getStages(0).asInstanceOf[T]
  }
}
