// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.powerbi

import com.microsoft.azure.synapse.ml.io.http.{CustomOutputParser, HTTPResponseData, SimpleHTTPTransformer}
import com.microsoft.azure.synapse.ml.stages.{DynamicMiniBatchTransformer, FixedMiniBatchTransformer,
  PartitionConsolidator, TimeIntervalMiniBatchTransformer}
import org.apache.http.client.HttpResponseException
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row}

import scala.collection.JavaConverters._

private[ml] class StreamMaterializer extends ForeachWriter[Row] {

  override def open(partitionId: Long, version: Long): Boolean = true

  override def process(value: Row): Unit = ()

  override def close(errorOrNull: Throwable): Unit = ()

}

object PowerBIWriter {

  val Logger: Logger = LogManager.getRootLogger

  private def prepareDF(df: DataFrame,  //scalastyle:ignore method.length
                        url: String,
                        options: Map[String, String] = Map()): DataFrame = {
    val applicableOptions = Set(
      "consolidate", "concurrency", "concurrentTimeout", "minibatcher",
      "maxBatchSize", "batchSize", "buffered", "maxBufferSize", "millisToWait"
    )

    options.keys.foreach(k =>
      assert(applicableOptions(k), s"$k not an applicable option ${applicableOptions.toList}"))

    val consolidate = options.get("consolidate").map(_.toBoolean).getOrElse(false)

    val concurrency = options.get("concurrency").map(_.toInt).getOrElse(1)
    val concurrentTimeout = options.get("concurrentTimeout").map(_.toDouble).getOrElse(30.0)

    val minibatcher = options.getOrElse("minibatcher", "fixed")
    val maxBatchSize = options.get("maxBatchSize").map(_.toInt).getOrElse(Integer.MAX_VALUE)
    val batchSize = options.get("batchSize").map(_.toInt).getOrElse(10)  //scalastyle:ignore magic.number
    val isBuffered = options.get("buffered").map(_.toBoolean).getOrElse(false)
    val maxBufferSize = options.get("maxBufferSize").map(_.toInt).getOrElse(5)  //scalastyle:ignore magic.number
    val millisToWait = options.get("millisToWait").map(_.toInt).getOrElse(1000)  //scalastyle:ignore magic.number

    val mb = minibatcher match {
      case "dynamic" =>
        new DynamicMiniBatchTransformer()
           .setMaxBatchSize(maxBatchSize)
      case "fixed" =>
        new FixedMiniBatchTransformer()
          .setBuffered(isBuffered)
          .setBatchSize(batchSize)
          .setMaxBufferSize(maxBufferSize)
      case "timed" =>
        new TimeIntervalMiniBatchTransformer()
          .setMillisToWait(millisToWait)
          .setMaxBatchSize(maxBatchSize)
    }

    val df2 = if (consolidate){
      new PartitionConsolidator().transform(df)
    }else{
      df
    }

    new SimpleHTTPTransformer()
      .setUrl(url)
      .setMiniBatcher(mb)
      .setFlattenOutputBatches(false)
      .setOutputParser(new CustomOutputParser().setUDF({response: HTTPResponseData =>
        val status = response.statusLine
        val code = status.statusCode
        if (code != 200){
          val content = new String(response.entity.get.content)
          throw new HttpResponseException(code, s"Request failed with \n " +
            s"code: $code, \n" +
            s"reason:${status.reasonPhrase}, \n" +
            s"content: $content")
        }
        response
      }))
      .setConcurrency(concurrency)
      .setConcurrentTimeout(concurrentTimeout)
      .setInputCol("input")
      .setOutputCol("output")
      .transform(df2.select(struct(df2.columns.map(col): _*).alias("input")))
  }

  def stream(df: DataFrame, url: String, options: Map[String, String] = Map()): DataStreamWriter[Row] = {
    prepareDF(df, url, options).writeStream.foreach(new StreamMaterializer)
  }

  def write(df: DataFrame, url: String, options: Map[String, String] = Map()): Unit = {
    prepareDF(df, url, options).foreach(_ => ())
  }

  def stream(df: DataFrame, url: String,
             options: java.util.HashMap[String, String]): DataStreamWriter[Row] = {
    stream(df, url, options.asScala.toMap)
  }

  def write(df: DataFrame, url: String,
            options: java.util.HashMap[String, String]): Unit = {
    write(df, url, options.asScala.toMap)
  }

}
