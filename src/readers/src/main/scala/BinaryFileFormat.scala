// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.binary

import java.io.{Closeable, InputStream}
import java.net.URI

import com.microsoft.ml.spark.StreamUtilities.ZipIterator
import com.microsoft.ml.spark.schema.BinaryFileSchema
import org.apache.commons.io.{FilenameUtils, IOUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.log4j.Logger
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

import scala.util.Random

/**
  * Actually reads the records from files
  *
  * @param subsample  what ratio to subsample
  * @param inspectZip whether to inspect zip files
  */
private[spark] class BinaryRecordReader(val subsample: Double, val inspectZip: Boolean)
  extends RecordReader[String, BytesWritable] {

  private var done: Boolean = false
  private var inputStream: InputStream = _
  private var filename: String = _
  private var recordValue: BytesWritable = _
  private var progress: Float = 0.0F
  private val rng: Random = new Random()
  private var zipIterator: ZipIterator = _

  override def close(): Unit = {
    if (inputStream != null) {
      inputStream.close()
    }
  }

  override def getCurrentKey: String = {
    filename
  }

  override def getCurrentValue: BytesWritable = {
    recordValue
  }

  override def getProgress: Float = {
    progress
  }

  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    // the file input
    val fileSplit = inputSplit.asInstanceOf[FileSplit]

    // the actual file we will be reading from
    val file = fileSplit.getPath
    // job configuration
    val conf = context.getConfiguration
    // get the filesystem
    val fs = file.getFileSystem(conf)
    // open the File
    filename = file.toString

    inputStream = fs.open(file)
    rng.setSeed(filename.hashCode.toLong)
    if (inspectZip && FilenameUtils.getExtension(filename) == "zip") {
        zipIterator = new ZipIterator(inputStream, filename, rng, subsample)
    }
  }

  def markAsDone(): Unit = {
    done = true
    progress = 1.0F
  }

  override def nextKeyValue(): Boolean = {
    if (done) {
      return false
    }

    if (zipIterator != null) {
      if (zipIterator.hasNext) {
        val (fn, barr) = zipIterator.next
        filename = fn
        recordValue = new BytesWritable()
        recordValue.set(barr, 0, barr.length)
        true
      } else {
        markAsDone()
        false
      }
    } else {
      if (rng.nextDouble() <= subsample) {
        val barr = IOUtils.toByteArray(inputStream)
        recordValue = new BytesWritable()
        recordValue.set(barr, 0, barr.length)
        markAsDone()
        true
      } else {
        markAsDone()
        false
      }
    }
  }
}

/**
  * File format used for structured streaming of binary files
  */
class BinaryFileFormat extends TextBasedFileFormat with DataSourceRegister {

  override def isSplitable(sparkSession: SparkSession,
                           options: Map[String, String],
                           path: Path): Boolean = false

  override def shortName(): String = "binary"

  override def inferSchema(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = {
    if (files.isEmpty) {
      None
    } else {
      Some(BinaryFileSchema.schema)
    }
  }

  override def prepareWrite(sparkSession: SparkSession,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {
    throw new NotImplementedError("writing to binary files is not supported")
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

    val subsample = options.getOrElse("subsample", "1.0").toDouble
    val inspectZip = options.getOrElse("inspectZip", "false").toBoolean

    assert(subsample >= 0.0 & subsample <= 1.0)
    (file: PartitionedFile) => {
      val fileReader = new HadoopFileReader(file, broadcastedHadoopConf.value.value, subsample, inspectZip)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => fileReader.close()))
      fileReader.map { bytes =>
        val row = new GenericInternalRow(2)
        row.update(0, UTF8String.fromString(file.filePath))
        row.update(1, bytes.getBytes)
        val outerRow = new GenericInternalRow(1)
        outerRow.update(0,row)
        outerRow
      }
    }
  }

  override def toString: String = "Binary"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[BinaryFileFormat]
}

/**
  * Thin wrapper class analogous to others in the spark ecosystem
  */
private[spark] class HadoopFileReader(file: PartitionedFile,
                                      conf: Configuration,
                                      subsample: Double,
                                      inspectZip: Boolean)
  extends Iterator[BytesWritable] with Closeable {

  private val iterator = {
    val fileSplit = new FileSplit(
      new Path(new URI(file.filePath)),
      file.start,
      file.length,
      Array.empty)
    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
    val reader = new BinaryRecordReader(subsample, inspectZip)
    reader.initialize(fileSplit, hadoopAttemptContext)
    new RecordReaderIterator(reader)
  }

  override def hasNext: Boolean = iterator.hasNext

  override def next(): BytesWritable = iterator.next()

  override def close(): Unit = iterator.close()
}
