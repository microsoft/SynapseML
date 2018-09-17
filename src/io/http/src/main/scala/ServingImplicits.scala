// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.HTTPSchema._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.continuous.{HTTPSinkProviderV2, HTTPSourceProviderV2}
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.language.implicitConversions

case class DataStreamReaderExtensions(dsr: DataStreamReader) {

  def server: DataStreamReader = {
    dsr.format(classOf[HTTPSourceProvider].getName)
  }

  def distributedServer: DataStreamReader = {
    dsr.format(classOf[DistributedHTTPSourceProvider].getName)
  }

  def continuousServer: DataStreamReader = {
    dsr.format(classOf[HTTPSourceProviderV2].getName)
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

  def continuousServer: DataStreamWriter[T] = {
    dsw.format(classOf[HTTPSinkProviderV2].getName)
  }

  def replyTo(name: String): DataStreamWriter[T] = {
    dsw.option("name", name)
  }

}

case class DataFrameServingExtensions(df: DataFrame) {

  def parseRequest(schema: DataType,
                    idCol: String = "id",
                    requestCol: String = "request"): DataFrame = {
    assert(df.schema(idCol).dataType == StringType &&
      df.schema(requestCol).dataType == HTTPRequestData.schema)
    schema match {
      case BinaryType =>
        df.select(col(idCol), col(requestCol).getItem("entity").getItem("content").alias("bytes"))
      case _ =>
        df.withColumn("variables", from_json(HTTPSchema.request_to_string(col(requestCol)), schema))
          .select(idCol,"variables.*")
    }
  }

  def makeReply(replyCol: String, name: String = "reply"): DataFrame ={
    def jsonReply(c: Column) = df.withColumn(name, string_to_response(to_json(c)))
    df.schema(replyCol).dataType match {
      case StringType => df.withColumn(name, string_to_response(col(replyCol)))
      case BinaryType => df.withColumn(name, binary_to_response(col(replyCol)))
      case _: StructType => jsonReply(col(replyCol))
      case _: MapType => jsonReply(col(replyCol))
      case at: ArrayType => at.elementType match {
        case _: StructType => jsonReply(col(replyCol))
        case _: MapType => jsonReply(col(replyCol))
        case _ => jsonReply(struct(col(replyCol)))
      }
      case _ => jsonReply(struct(col(replyCol)))
    }
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

  implicit def dfToDfse[T](df: DataFrame): DataFrameServingExtensions =
    DataFrameServingExtensions(df)

  implicit def dfseToDf[T](dfse: DataFrameServingExtensions): DataFrame =
    dfse.df

}
