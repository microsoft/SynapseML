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

  private[spark] def loadLibraryForAllPartitions[T:ClassTag](rdd: RDD[T], lib: String):RDD[T] = {
    def perPartition(it: Iterator[T]):Iterator[T] = {
      new NativeLoader("/org/bytedeco/javacpp").loadLibraryByName(lib); it }
    rdd.mapPartitions(perPartition, preservesPartitioning = true)
  }
}
