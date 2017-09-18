// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.BinaryReader.recursePath
import com.microsoft.ml.spark.schema.ImageSchema
import org.apache.hadoop.fs.Path
import org.apache.spark.image.ImageFileFormat
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.opencv.core.{CvException, MatOfByte}
import org.opencv.imgcodecs.Imgcodecs

object ImageReader {

  /** This object will load the openCV binaries when the object is referenced
    * for the first time, subsequent references will not re-load the binaries.
    * In spark, this loads one copy for each running jvm, instead of once per partition.
    * This technique is similar to that used by the cntk_jni jar,
    * but in the case where microsoft cannot edit the jar
    */
  object OpenCVLoader {
    import org.opencv.core.Core
    new NativeLoader("/nu/pattern/opencv").loadLibraryByName(Core.NATIVE_LIBRARY_NAME)
  }

  private[spark] def loadOpenCVFunc[A](it: Iterator[A]) = {
    OpenCVLoader
    it
  }

  private[spark] def loadOpenCV(df: DataFrame):DataFrame ={
    val encoder = RowEncoder(df.schema)
    df.mapPartitions(loadOpenCVFunc)(encoder)
  }

  /** Convert the image from compressd (jpeg, etc.) into OpenCV representation and store it in Row
    * See ImageSchema for details.
    *
    * @param filename arbitrary string
    * @param bytes image bytes (for example, jpeg)
    * @return returns None if decompression fails
    */
  def decode(filename: String, bytes: Array[Byte]): Option[Row] = {
    val mat = new MatOfByte(bytes: _*)
    val decodedOpt = try {
      Some(Imgcodecs.imdecode(mat, Imgcodecs.CV_LOAD_IMAGE_COLOR))
    } catch {
      case _:CvException => None
    }

    decodedOpt match {
      case Some(decoded) if !decoded.empty() =>
        val ocvBytes = new Array[Byte](decoded.total.toInt * decoded.elemSize.toInt)
        // extract OpenCV bytes
        decoded.get(0, 0, ocvBytes)
        // type: CvType.CV_8U
        Some(Row(filename, decoded.height, decoded.width, decoded.`type`, ocvBytes))
      case _ => None
    }

  }

  /** Read the directory of images from the local or remote source
    *
    * @param path      Path to the image directory
    * @param recursive Recursive search flag
    * @return          DataFrame with a single column of "images", see "columnSchema" for details
    */
  def read(path: String, recursive: Boolean, spark: SparkSession,
           sampleRatio: Double = 1, inspectZip: Boolean = true): DataFrame = {
    val p = new Path(path)
    val globs = if (recursive){
      recursePath(p.getFileSystem(spark.sparkContext.hadoopConfiguration), p, {fs => fs.isDirectory})
        .map(g => g) ++ Array(p)
    }else{
      Array(p)
    }
    spark.read.format(classOf[ImageFileFormat].getName)
      .option("subsample", sampleRatio)
      .option("inspectZip", inspectZip).load(globs.map(_.toString):_*)
  }

  /** Read the directory of image files from the local or remote source
    *
    * @param path       Path to the directory
    * @return           DataFrame with a single column of "imageFiles", see "columnSchema" for details
    */
  def stream(path: String, spark: SparkSession,
             sampleRatio: Double = 1, inspectZip: Boolean = true): DataFrame = {
    val p = new Path(path)
    spark.readStream.format(classOf[ImageFileFormat].getName)
      .option("subsample", sampleRatio)
      .option("inspectZip",inspectZip).schema(ImageSchema.schema).load(p.toString)
  }

}
