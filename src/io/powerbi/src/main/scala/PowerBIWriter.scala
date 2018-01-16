// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

private class PowerBIClient(concurrency: Int, timeout: Duration)
                           (implicit ec: ExecutionContext)
  extends BatchedAsyncClient[String, String](concurrency, timeout)(ec)
  with JsonClient

private object PowerBITransformer extends DefaultParamsReadable[PowerBITransformer]

private class PowerBITransformer(uid: String)
    extends WrappedHTTPTransformer[String, String](uid)
    with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("PowerBITransformer"))

  override def getBaseClient: BaseClient[String, String] with ColumnSchema =
    new PowerBIClient(2, Duration.Inf)(ExecutionContext.global)

  override def transformColumnSchema(schema: DataType): DataType = new StructType().add("code",StringType)

}

private class StreamMaterializer extends ForeachWriter[Row] {

  override def open(partitionId: Long, version: Long): Boolean = true

  override def process(value: Row): Unit = {}

  override def close(errorOrNull: Throwable): Unit = {}

}

object PowerBIWriter {

  val logger: Logger = LogManager.getRootLogger

  private def prepareDF(df: DataFrame, url: String): DataFrame = {
    val jsonDF = df.select(struct(df.columns.map(col): _*).alias("input"))
    new PowerBITransformer()
      .setUrl(url)
      .setInputCol("input")
      .setOutputCol("output")
      .transform(jsonDF)
  }

  def stream(df: DataFrame, url: String): DataStreamWriter[Row] = {
      prepareDF(df, url).writeStream.foreach(new StreamMaterializer)
  }

  def write(df: DataFrame, url: String): Unit = {
    prepareDF(df, url).foreachPartition(it => it.foreach(_ => ()))
  }

}
