// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.schema.ImageSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.opencv.core.{Core, MatOfByte}
import org.opencv.imgcodecs.Imgcodecs

object ImageReader {

  //single column of images named "image"
  private val imageDFSchema = StructType(StructField("image", ImageSchema.columnSchema, true) :: Nil)

  /**
    * Convert the image from compressd (jpeg, etc.) into OpenCV representation and store it in Row
    * See ImageSchema for details.
    *
    * @param filename arbitrary string
    * @param bytes image bytes (for example, jpeg)
    * @return returns None if decompression fails
    */
  private[spark] def decode(filename: String, bytes: Array[Byte]): Option[Row] = {
    val mat = new MatOfByte(bytes: _*)
    val decoded = Imgcodecs.imdecode(mat, Imgcodecs.CV_LOAD_IMAGE_COLOR)

    if (decoded.empty()) {
      None
    } else {
      val ocvBytes = new Array[Byte](decoded.total.toInt * decoded.elemSize.toInt)

      // extract OpenCV bytes
      decoded.get(0, 0, ocvBytes)

      // type: CvType.CV_8U
      Some(Row(filename, decoded.height, decoded.width, decoded.`type`, ocvBytes))
    }
  }

  /**
    * Read the directory of images from the local or remote source
    *
    * @param path      Path to the image directory
    * @param recursive Recursive search flag
    * @return Dataframe with a single column of "images", see imageSchema for details
    */
  def read(path: String, recursive: Boolean, spark: SparkSession,
           sampleRatio: Double = 1, inspectZip: Boolean = true): DataFrame = {

    val binaryRDD = BinaryFileReader.readRDD(path, recursive, spark, sampleRatio, inspectZip)
    val binaryRDDlib = ImageSchema.loadLibraryForAllPartitions(binaryRDD, Core.NATIVE_LIBRARY_NAME)

    val validImages = binaryRDDlib.flatMap {
      case (filename, bytes) => {
        decode(filename, bytes).map(x => Row(x))
      }
    }

    spark.createDataFrame(validImages, imageDFSchema)
  }
}
