// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.schema

import com.microsoft.ml.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import scala.reflect.ClassTag

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

  /** This object will load the openCV binaries when the object is referenced
    * for the first time, subsequent references will not re-load the binaries.
    * In spark, this loads one copy for each running jvm, instead of once per partition.
    * This technique is similar to that used by the cntk_jni jar,
    * but in the case where microsoft cannot edit the jar
    */
  private[spark] object OpenCVLoader {
    import org.opencv.core.Core
    new NativeLoader("/nu/pattern/opencv").loadLibraryByName(Core.NATIVE_LIBRARY_NAME)
  }

  private[spark] def loadOpenCV[T:ClassTag](rdd: RDD[T]):RDD[T] =
    rdd.mapPartitions({it => OpenCVLoader; it}, preservesPartitioning = true)
}
