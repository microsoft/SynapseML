// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities._
import com.microsoft.azure.synapse.ml.core.utils.{
  ContextObjectInputStream,
  DeserializationClassFilter,
  SafeObjectInputStream
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql._

import java.io.{InputStream, InvalidClassException, ObjectOutputStream, OutputStream}
import scala.language.existentials
import scala.reflect.runtime.universe._
import scala.util.control.NonFatal

abstract class Serializer[O] {
  def write(obj: O, path: Path, overwrite: Boolean): Unit
  def read(path: Path): O
}

object Serializer {

  /** Compatibility switch for trusted legacy artifacts whose object graphs cannot be constrained. */
  val LegacyObjectDeserializationConfig: String =
    "spark.synapseml.legacy.allowUnsafeJavaDeserialization"

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
    typeToSerializer(tpe, sparkSession, None)
  }

  def typeToSerializer[T](
      tpe: Type,
      sparkSession: SparkSession,
      classFilter: Option[DeserializationClassFilter]): Serializer[T] = {
    (if (tpe <:< typeOf[PipelineStage])              new PipelineSerializer()
     else if (tpe <:< typeOf[Array[PipelineStage]])  new PipelineArraySerializer()
     else if (tpe <:< typeOf[Dataset[_]])            new DFSerializer(sparkSession)
     else classFilter match {
       case Some(filter) =>
         new FilteredObjectSerializer(sparkSession, filter)(typeToTypeTag(tpe))
       case None =>
         new ObjectSerializer(sparkSession)(typeToTypeTag(tpe))
     })
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

  private val PrimitiveByteArrayFilter = DeserializationClassFilter()

  private def defaultClassFilter(tpe: Type): Option[DeserializationClassFilter] = {
    if (tpe =:= typeOf[Array[Byte]]) Some(PrimitiveByteArrayFilter) else None
  }

  private def legacyObjectDeserializationEnabled(spark: SparkSession): Boolean = {
    spark.conf.getOption(LegacyObjectDeserializationConfig).exists(_.equalsIgnoreCase("true"))
  }

  private def disabledDeserializationException(tpe: Type, guidance: String): SecurityException = {
    new SecurityException(
      s"Java deserialization is disabled for $tpe. " +
        s"$guidance Deserializing this artifact may execute arbitrary code."
    )
  }

  private def closeAndThrow(input: InputStream, error: Throwable): Nothing = {
    try input.close() catch {
      case NonFatal(closeError) => error.addSuppressed(closeError)
    }
    throw error
  }

  def read[A](
      is: InputStream,
      classFilter: DeserializationClassFilter)(implicit ttag: TypeTag[A]): A = {
    val safeInput = try {
      new SafeObjectInputStream(is, classFilter)
    } catch {
      case NonFatal(error) => closeAndThrow(is, error)
    }
    using(safeInput) { in =>
      in.readObject.asInstanceOf[A]
    }.get
  }

  def read[A](is: InputStream)(implicit ttag: TypeTag[A]): A = {
    defaultClassFilter(ttag.tpe) match {
      case Some(filter) => read(is, filter)
      case None =>
        closeAndThrow(
          is,
          disabledDeserializationException(
            ttag.tpe,
            "Call Serializer.readUnsafe only for a trusted legacy artifact."
          )
        )
    }
  }

  /** Reads an object without a class policy. The caller must already trust the artifact. */
  def readUnsafe[A](is: InputStream)(implicit ttag: TypeTag[A]): A = {
    using(new ContextObjectInputStream(is)) { in =>
      in.readObject.asInstanceOf[A]
    }.get
  }

  /** Hadoop configuration derived from the session instead of the SparkContext.
    *
    * `SparkSession.sparkContext` is unavailable under Spark Connect and is explicitly unsupported
    * on Databricks Unity Catalog standard access mode, so deriving the configuration from the
    * session is what keeps model persistence working there. This mirrors Spark MLlib's own
    * `session.sessionState.newHadoopConf()`. It also layers in session-level conf, which
    * `sparkContext.hadoopConfiguration` alone does not.
    */
  private[ml] def sessionHadoopConf(spark: SparkSession): Configuration =
    spark.sessionState.newHadoopConf()

  /** Writes the object to the given path.
    *
    * @param obj        The object to write.
    * @param outputPath Where to write the object
    */
  def writeToHDFS[O](spark: SparkSession, obj: O, outputPath: Path, overwrite: Boolean)
                    (implicit ttag: TypeTag[O]): Unit = {
    using(outputPath.getFileSystem(sessionHadoopConf(spark)).create(outputPath, overwrite)) { os =>
      write[O](obj, os)(ttag)
    }.get
  }

  /** Loads the object from the given path.
    *
    * @param path The main path for model to load the object from.
    * @return The loaded object.
    */
  def readFromHDFS[O](spark: SparkSession, path: Path)(implicit ttag: TypeTag[O]): O = {
    defaultClassFilter(ttag.tpe) match {
      case Some(filter) => readFromHDFS(spark, path, filter)
      case None if legacyObjectDeserializationEnabled(spark) =>
        readFromHDFSUnsafe(spark, path)
      case None =>
        throw disabledDeserializationException(
          ttag.tpe,
          s"Set $LegacyObjectDeserializationConfig=true only when loading a trusted legacy model."
        )
    }
  }

  def readFromHDFS[O](
      spark: SparkSession,
      path: Path,
      classFilter: DeserializationClassFilter)(implicit ttag: TypeTag[O]): O = {
    try {
      using(path.getFileSystem(sessionHadoopConf(spark)).open(path)) { in =>
        read[O](in, classFilter)(ttag)
      }.get
    } catch {
      case _: InvalidClassException if legacyObjectDeserializationEnabled(spark) =>
        readFromHDFSUnsafe(spark, path)
      case error: InvalidClassException =>
        val securityError = disabledDeserializationException(
          ttag.tpe,
          s"The object graph contains ${error.classname}, which is outside its approved class policy. " +
            s"Set $LegacyObjectDeserializationConfig=true only when loading a trusted legacy model."
        )
        securityError.initCause(error)
        throw securityError
    }
  }

  /** Reads an object without a class policy. The caller must already trust the artifact. */
  def readFromHDFSUnsafe[O](
      spark: SparkSession,
      path: Path)(implicit ttag: TypeTag[O]): O = {
    using(path.getFileSystem(sessionHadoopConf(spark)).open(path)) { in =>
      readUnsafe[O](in)(ttag)
    }.get
  }

  def makeQualifiedPath(spark: SparkSession, path: String): Path = {
    makeQualifiedPath(sessionHadoopConf(spark), path)
  }

  private def makeQualifiedPath(hadoopConf: Configuration, path: String): Path = {
    val modelPath = new Path(path)
    // Note: to get correct working dir, must use root path instead of root + part
    val fs = modelPath.getFileSystem(hadoopConf)
    modelPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
  }

}

class ObjectSerializer[O](spark: SparkSession)(implicit ttag: TypeTag[O]) extends Serializer[O] {

  def write(obj: O, path: Path, overwrite: Boolean): Unit = Serializer.writeToHDFS(spark, obj, path, overwrite)

  def read(path: Path): O = Serializer.readFromHDFS(spark, path)
}

private[ml] class FilteredObjectSerializer[O](
    spark: SparkSession,
    classFilter: DeserializationClassFilter)(implicit ttag: TypeTag[O]) extends Serializer[O] {

  def write(obj: O, path: Path, overwrite: Boolean): Unit =
    Serializer.writeToHDFS(spark, obj, path, overwrite)

  def read(path: Path): O = Serializer.readFromHDFS(spark, path, classFilter)
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
