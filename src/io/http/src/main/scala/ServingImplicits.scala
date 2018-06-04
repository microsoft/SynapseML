// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter}
import scala.language.implicitConversions

case class DataStreamReaderExtensions(dsr: DataStreamReader) {

  def server: DataStreamReader = {
    dsr.format(classOf[HTTPSourceProvider].getName)
  }

  def distributedServer: DataStreamReader = {
    dsr.format(classOf[DistributedHTTPSourceProvider].getName)
  }

  def address(host: String, port: Int, api: String): DataStreamReader = {
    dsr.option("host", host).option("port", port.toLong).option("name", api)
  }

}

case class DataStreamWriterExtensions[T](dsw: DataStreamWriter[T]) {

  def server: DataStreamWriter[T] = {
    dsw.format(classOf[HTTPSinkProvider].getName)
  }

  def distributedServer: DataStreamWriter[T] = {
    dsw.format(classOf[DistributedHTTPSinkProvider].getName)
  }

}

object ServingImplicits {
  implicit def dsrToDsre(dsr: DataStreamReader): DataStreamReaderExtensions =
    DataStreamReaderExtensions(dsr)

  implicit def dsreToDsr(dsre: DataStreamReaderExtensions): DataStreamReader =
    dsre.dsr

  implicit def dswToDswe[T](dsw: DataStreamWriter[T]): DataStreamWriterExtensions[T] =
    DataStreamWriterExtensions(dsw)

  implicit def dsweToDsw[T](dswe: DataStreamWriterExtensions[T]): DataStreamWriter[T] =
    dswe.dsw
}
