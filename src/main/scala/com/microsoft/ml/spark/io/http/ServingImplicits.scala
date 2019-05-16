// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.io.http

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.continuous._
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
    dsr.option("host", host).option("port", port.toLong).option("path", api)
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

  private def jsonParsingError(schema: DataType)(body: String): String = {
    s"JSON Parsing error, expected schema:\n ${schema.simpleString}\n recieved:\n $body"
  }

  private def fullJsonParsingSuccess(a: Any): Boolean = {
    a match {
      case s: Seq[_] => s.forall(fullJsonParsingSuccess)
      case a: Row => a.toSeq.forall(fullJsonParsingSuccess)
      case null => false
      case _ => true
    }
  }

  /**
    *
    * @param apiName
    * @param schema
    * @param idCol
    * @param requestCol
    * @param parsingCheck "none": to accept all requests,
    *                     "full": to ensure all fields and subfields are non-null,
    *                     "partial": to ensure the root structure was parsed correctly
    * @return
    */
  def parseRequest(apiName: String,
                   schema: DataType,
                   idCol: String = "id",
                   requestCol: String = "request",
                   parsingCheck: String = "none"): DataFrame = {
    assert(df.schema(idCol).dataType == HTTPSourceV2.ID_SCHEMA &&
      df.schema(requestCol).dataType == HTTPRequestData.schema)
    schema match {
      case BinaryType =>
        df.select(col(idCol), col(requestCol).getItem("entity").getItem("content").alias("bytes"))
      case _ =>
        val parsedDf = df
          .withColumn("body", HTTPSchema.request_to_string(col(requestCol)))
          .withColumn("parsed", from_json(col("body"), schema))
        if (parsingCheck.toLowerCase == "none") {
          parsedDf.select(idCol, "parsed.*")
        } else {
          val successCol = parsingCheck.toLowerCase  match {
            case "full"  => udf({x: Any => !fullJsonParsingSuccess(x)}, BooleanType)(col("parsed"))
            case "partial" => col("parsed").isNull
            case _ => throw new IllegalArgumentException(
              s"Need to use either full, partial, or none. Received $parsingCheck")
          }

          val df1 = parsedDf
            .withColumn("didReply",
              when(successCol,
                ServingUDFs.sendReplyUDF(
                  lit(apiName),
                  ServingUDFs.makeReplyUDF(
                    udf(jsonParsingError(schema) _, StringType)(col("body")),
                    StringType,
                    code = lit(400),
                    reason = lit("JSON Parsing Failure")),
                  col("id")
                )
              )
                .otherwise(lit(null)))
            .filter(col("didReply").isNull)

          df1.withColumn("parsed", udf({ x: Row =>
            println(x)
            x
          }, df1.schema("parsed").dataType)(col("parsed")))
            .select(idCol, "parsed.*")

        }
    }
  }

  def makeReply(replyCol: String, name: String = "reply"): DataFrame = {
    df.withColumn(name, ServingUDFs.makeReplyUDF(col(replyCol), df.schema(replyCol).dataType))
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
