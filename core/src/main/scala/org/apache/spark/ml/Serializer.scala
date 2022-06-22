// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities._
import com.microsoft.azure.synapse.ml.core.utils.ContextObjectInputStream
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql._

import java.io.{InputStream, ObjectOutputStream, OutputStream}
import scala.language.existentials
import scala.reflect.runtime.universe._

abstract class Serializer[O] {
  def write(obj: O, path: Path, overwrite: Boolean): Unit
  def read(path: Path): O
}

object Serializer {

  val ContextClassLoader: ClassLoader = Thread.currentThread().getContextClassLoader

  val Mirror: Mirror = runtimeMirror(Serializer.ContextClassLoader)

  def getPath(baseDir: Path, i: Int): Path = {
    new Path(baseDir, s"data_$i")
  }

  def typeToTypeTag[T](tpe: Type): TypeTag[T] = {
    TypeTag(Mirror, new reflect.api.TypeCreator {
      def apply[U <: reflect.api.Universe with Singleton](m: reflect.api.Mirror[U]): U#Type = {
        assert(m eq Mirror, s"TypeTag[$tpe] defined in $Mirror cannot be migrated to $m.")
        tpe.asInstanceOf[U#Type]
      }
    })
  }

  def typeToSerializer[T](tpe: Type, sparkSession: SparkSession): Serializer[T] = {
    (if (tpe <:< typeOf[PipelineStage])              new PipelineSerializer()
     else if (tpe <:< typeOf[Array[PipelineStage]])  new PipelineArraySerializer()
     else if (tpe <:< typeOf[Dataset[_]])            new DFSerializer(sparkSession)
     else new ObjectSerializer(sparkSession.sparkContext)(typeToTypeTag(tpe)))
      .asInstanceOf[Serializer[T]]
  }

  def writeMLWritable(stage: MLWritable, outputPath: Path, overwrite: Boolean): Unit = {
    val writer = if (overwrite) stage.write.overwrite()
                 else stage.write
    writer.save(outputPath.toString)
  }

  def write[A](o: A, outputStream: OutputStream)(implicit ttag: TypeTag[A]): Unit = {
    using(new ObjectOutputStream(outputStream)) { out =>
      out.writeObject(o)
    }.get
  }

  def read[A](is: InputStream)(implicit ttag: TypeTag[A]): A = {
    using(new ContextObjectInputStream(is)) { in =>
      in.readObject.asInstanceOf[A]
    }.get
  }

  /** Writes the object to the given path.
    *
    * @param obj        The object to write.
    * @param outputPath Where to write the object
    */
  def writeToHDFS[O](sc: SparkContext, obj: O, outputPath: Path, overwrite: Boolean)
                    (implicit ttag: TypeTag[O]): Unit = {
    val hadoopConf = sc.hadoopConfiguration
    using(outputPath.getFileSystem(hadoopConf).create(outputPath, overwrite)) { os =>
      write[O](obj, os)(ttag)
    }.get
  }

  /** Loads the object from the given path.
    *
    * @param path The main path for model to load the object from.
    * @return The loaded object.
    */
  def readFromHDFS[O](sc: SparkContext, path: Path)(implicit ttag: TypeTag[O]): O = {
    val hadoopConf = sc.hadoopConfiguration
    using(path.getFileSystem(hadoopConf).open(path)) { in =>
      read[O](in)(ttag)
    }.get
  }

  def makeQualifiedPath(sc: SparkContext, path: String): Path = {
    val modelPath = new Path(path)
    val hadoopConf = sc.hadoopConfiguration
    // Note: to get correct working dir, must use root path instead of root + part
    val fs = modelPath.getFileSystem(hadoopConf)
    modelPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
  }

}

class ObjectSerializer[O](sc: SparkContext)(implicit ttag: TypeTag[O]) extends Serializer[O] {
  def write(obj: O, path: Path, overwrite: Boolean): Unit = Serializer.writeToHDFS(sc, obj, path, overwrite)

  def read(path: Path): O = Serializer.readFromHDFS(sc, path)
}

class DFSerializer(spark: SparkSession) extends Serializer[DataFrame] {
  def write(df: DataFrame, outputPath: Path, overwrite: Boolean): Unit = {
    val saveMode =
      if (overwrite) SaveMode.Overwrite
      else SaveMode.ErrorIfExists

    df.write.mode(saveMode).parquet(outputPath.toString)
  }

  def read(path: Path): DataFrame = {
    spark.read.format("parquet").load(path.toString)
  }
}

class PipelineSerializer extends Serializer[PipelineStage] {
  def write(stage: PipelineStage, outputPath: Path, overwrite: Boolean): Unit = {
    val pipe = new Pipeline().setStages(Array(stage))
    Serializer.writeMLWritable(pipe, outputPath, overwrite)
  }

  def read(path: Path): PipelineStage = {
    Pipeline.load(path.toString).getStages(0)
  }
}

class PipelineArraySerializer extends Serializer[Array[PipelineStage]] {
  def write(stages: Array[PipelineStage], outputPath: Path, overwrite: Boolean): Unit = {
    val pipe = new Pipeline().setStages(stages)
    Serializer.writeMLWritable(pipe, outputPath, overwrite)
  }

  def read(path: Path): Array[PipelineStage] = {
    Pipeline.load(path.toString).getStages
  }
}
