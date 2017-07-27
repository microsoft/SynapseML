// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.schema

import com.microsoft.ml.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

import scala.reflect.ClassTag
import scala.collection.JavaConverters._

object ImageSchema {

  /** Schema for the image column: Row(String, Int, Int, Int, Array[Byte]) */
  val columnSchema = StructType(
    StructField("path",   StringType,  true) ::
    StructField("height", IntegerType, true) ::
    StructField("width",  IntegerType, true) ::
    StructField("type", IntegerType, true) ::                 //OpenCV type: CV_8U in most cases
    StructField("bytes",  BinaryType, true) :: Nil)   //OpenCV bytes: row-wise BGR in most cases

  def getPath(row: Row): String = row.getString(0)
  def getHeight(row: Row): Int = row.getInt(1)
  def getWidth(row: Row): Int = row.getInt(2)
  def getType(row: Row): Int = row.getInt(3)
  def getBytes(row: Row): Array[Byte] = row.getAs[Array[Byte]](4)

  /** Check if the dataframe column contains images (i.e. has imageSchema)
    *
    * @param df
    * @param column
    * @return
    */
  def isImage(df: DataFrame, column: String): Boolean =
    df.schema(column).dataType == columnSchema

  // TODO: Figure out a better way to list all dependencies to be extracted in opencv jar
  val dependencies = List("libgomp.so.1", "libjnicvkernels.so", "libjniopencv_bioinspired.so",
    "libjniopencv_calib3d.so", "libjniopencv_core.so", "libjniopencv_dnn.so", "libjniopencv_face.so",
    "libjniopencv_features2d.so", "libjniopencv_flann.so", "libjniopencv_highgui.so", "libjniopencv_imgcodecs.so",
    "libjniopencv_imgproc.so", "libjniopencv_ml.so", "libjniopencv_objdetect.so", "libjniopencv_optflow.so",
    "libjniopencv_photo.so", "libjniopencv_shape.so", "libjniopencv_stitching.so", "libjniopencv_superres.so",
    "libjniopencv_text.so", "libjniopencv_video.so", "libjniopencv_videoio.so", "libjniopencv_videostab.so",
    "libjniopencv_xfeatures2d.so", "libjniopencv_ximgproc.so", "libopencv_bioinspired.so.3.2",
    "libopencv_calib3d.so.3.2", "libopencv_core.so.3.2", "libopencv_dnn.so.3.2", "libopencv_face.so.3.2",
    "libopencv_features2d.so.3.2", "libopencv_flann.so.3.2", "libopencv_highgui.so.3.2", "libopencv_imgcodecs.so.3.2",
    "libopencv_imgproc.so.3.2", "libopencv_ml.so.3.2", "libopencv_objdetect.so.3.2", "libopencv_optflow.so.3.2",
    "libopencv_photo.so.3.2", "libopencv_shape.so.3.2", "libopencv_stitching.so.3.2", "libopencv_superres.so.3.2",
    "libopencv_text.so.3.2", "libopencv_video.so.3.2", "libopencv_videoio.so.3.2", "libopencv_videostab.so.3.2",
    "libopencv_xfeatures2d.so.3.2", "libopencv_ximgproc.so.3.2")

  private[spark] def loadLibraryForAllPartitions[T:ClassTag](rdd: RDD[T], lib: String):RDD[T] = {
    def perPartition(it: Iterator[T]):Iterator[T] = {
      val nativeLoader = new NativeLoader("/org/bytedeco/javacpp")
      nativeLoader.loadLibraryWithDepsByName("libopencv_core.so.3.2", dependencies.asJava)
      nativeLoader.loadLibraryWithDepsByName(lib, dependencies.asJava); it }
    rdd.mapPartitions(perPartition, preservesPartitioning = true)
  }
}
