// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.io

import org.apache.spark.binary.BinaryFileFormat
import org.apache.spark.ml.image.ImageSchema
import org.apache.spark.ml.source.image.PatchedImageFileFormat
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter}
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter}

import scala.language.implicitConversions

case class DataStreamReaderExtensions2(dsr: DataStreamReader) {

  def image: DataStreamReader = {
    dsr.format(classOf[PatchedImageFileFormat].getName)
      .schema(ImageSchema.imageSchema)
  }

  def binary: DataStreamReader = {
    dsr.format(classOf[BinaryFileFormat].getName)
  }

}

case class DataStreamWriterExtensions2[T](dsw: DataStreamWriter[T]) {

  def image: DataStreamWriter[T] = {
    dsw.format(classOf[PatchedImageFileFormat].getName)
  }

  def binary: DataStreamWriter[T] = {
    dsw.format(classOf[BinaryFileFormat].getName)
  }

}

case class DataFrameReaderExtensions2(dsr: DataFrameReader) {

  def image: DataFrameReader = {
    dsr.format(classOf[PatchedImageFileFormat].getName)
  }

  def binary: DataFrameReader = {
    dsr.format(classOf[BinaryFileFormat].getName)
  }

}

case class DataFrameWriterExtensions2[T](dsw: DataFrameWriter[T]) {

  def image: DataFrameWriter[T] = {
    dsw.format(classOf[PatchedImageFileFormat].getName)
  }

  def binary: DataFrameWriter[T] = {
    dsw.format(classOf[BinaryFileFormat].getName)
  }

}

object IOImplicits {
  implicit def dsrToDsre(dsr: DataStreamReader): DataStreamReaderExtensions2 =
    DataStreamReaderExtensions2(dsr)

  implicit def dsreToDsr(dsre: DataStreamReaderExtensions2): DataStreamReader =
    dsre.dsr

  implicit def dswToDswe[T](dsw: DataStreamWriter[T]): DataStreamWriterExtensions2[T] =
    DataStreamWriterExtensions2(dsw)

  implicit def dsweToDsw[T](dswe: DataStreamWriterExtensions2[T]): DataStreamWriter[T] =
    dswe.dsw

  implicit def dfrToDfre(dsr: DataFrameReader): DataFrameReaderExtensions2 =
    DataFrameReaderExtensions2(dsr)

  implicit def dfreToDfr(dsre: DataFrameReaderExtensions2): DataFrameReader =
    dsre.dsr

  implicit def dfwToDfwe[T](dsw: DataFrameWriter[T]): DataFrameWriterExtensions2[T] =
    DataFrameWriterExtensions2(dsw)

  implicit def dfweToDfw[T](dswe: DataFrameWriterExtensions2[T]): DataFrameWriter[T] =
    dswe.dsw

}
