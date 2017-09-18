// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.image

import com.microsoft.ml.spark.ImageReader
import com.microsoft.ml.spark.schema.ImageSchema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce._
import org.apache.spark.TaskContext
import org.apache.spark.binary._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

class ImageFileFormat extends TextBasedFileFormat with DataSourceRegister with Serializable {

  override def isSplitable(sparkSession: SparkSession,
                           options: Map[String, String],
                           path: Path): Boolean = false

  override def shortName(): String = "image"

  override def inferSchema(sparkSession: SparkSession,
                           options: Map[String, String],
                           files: Seq[FileStatus]): Option[StructType] = {
    Some(ImageSchema.schema)
  }

  override def prepareWrite(sparkSession: SparkSession,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {
    throw new NotImplementedError("writing to image files is not supported")
  }

  override def buildReader(sparkSession: SparkSession,
                           dataSchema: StructType,
                           partitionSchema: StructType,
                           requiredSchema: StructType,
                           filters: Seq[Filter],
                           options: Map[String, String],
                           hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val subsample = options.getOrElse("subsample","1.0").toDouble
    assert(subsample>=0.0 & subsample <=1.0)
    val inspectZip = options.getOrElse("inspectZip", "false").toBoolean

    (file: PartitionedFile) => {
      val fileReader = new HadoopFileReader(file, broadcastedHadoopConf.value.value, subsample, inspectZip)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => fileReader.close()))
      fileReader.flatMap {bytes =>
        val byteArray = bytes.getBytes
        ImageReader.OpenCVLoader
        val rowOpt = ImageReader.decode(file.filePath, byteArray)

        rowOpt match {
          case None => None
          case Some(row) =>
            val imGenRow = new GenericInternalRow(1)
            val genRow = new GenericInternalRow(ImageSchema.columnSchema.fields.length)
            genRow.update(0, UTF8String.fromString(row.getString(0)))
            genRow.update(1, row.getInt(1))
            genRow.update(2, row.getInt(2))
            genRow.update(3, row.getInt(3))
            genRow.update(4, row.getAs[Array[Byte]](4))
            imGenRow.update(0, genRow)
            Some(imGenRow)
        }
      }
    }
  }

  override def toString: String = "Image"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[BinaryFileFormat]

}
