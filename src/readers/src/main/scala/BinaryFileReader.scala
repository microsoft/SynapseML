// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.schema.BinaryFileSchema
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.language.existentials
import com.microsoft.ml.spark.FileUtilities.{ZipIterator}
import com.microsoft.ml.spark.hadoop.{SamplePathFilter, RecursiveFlag}

object BinaryFileReader {

  //single column of images named "image"
  private val binaryDFSchema = StructType(StructField("value", BinaryFileSchema.columnSchema, true) :: Nil)

  /** Read the directory of images from the local or remote source
    *
    * @param path      Path to the image directory
    * @param recursive Recursive search flag
    * @return Dataframe with a single column of "images", see imageSchema for details
    */
  private[spark] def readRDD(path: String, recursive: Boolean, spark: SparkSession,
                               sampleRatio: Double, inspectZip: Boolean)
  : RDD[(String, Array[Byte])] = {

    require(sampleRatio <= 1.0 && sampleRatio >= 0, "sampleRatio should be between 0 and 1")

    val oldRecursiveFlag = RecursiveFlag.setRecursiveFlag(Some(recursive.toString), spark)
    val oldPathFilter: Option[Class[_]] =
      if (sampleRatio < 1)
        SamplePathFilter.setPathFilter(Some(classOf[SamplePathFilter]), Some(sampleRatio), Some(inspectZip), spark)
      else
        None

    var data: RDD[(String, Array[Byte])] = null
    try {
      val streams = spark.sparkContext.binaryFiles(path, spark.sparkContext.defaultParallelism)

      // Create files RDD and load bytes
      data = if (!inspectZip) {
        streams.mapValues((stream: PortableDataStream) => stream.toArray)
      } else {
        // if inspectZip is enabled, examine/sample the contents of zip files
        streams.flatMap({ case (filename: String, stream: PortableDataStream) =>
          if (SamplePathFilter.isZipFile(filename)) {
            new ZipIterator(stream, filename, sampleRatio)
          } else {
            Some((filename, stream.toArray))
          }
        })
      }
    }
    finally {
      // return Hadoop flag to its original value
      RecursiveFlag.setRecursiveFlag(oldRecursiveFlag, spark = spark)
      SamplePathFilter.setPathFilter(oldPathFilter, spark = spark)
      ()
    }

    data
  }

  def read(path: String, recursive: Boolean, spark: SparkSession,
           sampleRatio: Double = 1, inspectZip: Boolean = true): DataFrame = {
    val rowRDD = readRDD(path, recursive, spark, sampleRatio, inspectZip)
      .map({row:(String, Array[Byte]) => Row(Row(row._1, row._2))})

    spark.createDataFrame(rowRDD, binaryDFSchema)
  }
}

