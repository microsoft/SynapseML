// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.message.BufferedHeader
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row}

import scala.collection.mutable

object PowerBIWriter {

  private def sendJsonStrings(jsonStrings: Seq[String], url: String): Unit = {
    if (jsonStrings.isEmpty) return
    val json = "[" + jsonStrings.mkString(",\n") + "]"
    val post = new HttpPost(url)
    post.setHeader("Content-type", "application/json")
    post.setEntity(new StringEntity(json))

    StreamUtilities.using(HttpClientBuilder.create().build()) { client =>
      val response = client.execute(post)
      if (response.getStatusLine.getStatusCode == 429) {
        val waitTime = response.headerIterator("Retry-After")
          .nextHeader().asInstanceOf[BufferedHeader]
          .getBuffer.toString.split(" ").last.toInt
        Thread.sleep(waitTime.toLong * 500)
        sendJsonStrings(jsonStrings, url)
        return
      }
      assert(response.getStatusLine.getStatusCode == 200, response.toString)
    }.get
  }

  private class PowerBIWriter(val url: String, val batchInterval:Int) extends ForeachWriter[Row] {
    var client: CloseableHttpClient = _
    val queue: mutable.Queue[String] = mutable.Queue()
    var thread: Thread = _

    def process(value: Row): Unit = {
      queue.enqueue(value.getString(0))
    }

    def processBatch(): Unit = {
      try {
        sendJsonStrings(queue.dequeueAll(_ => true), url)
        Thread.sleep(batchInterval.toLong)
      } catch {
        case ex: InterruptedException =>
      }
    }

    def close(errorOrNull: Throwable): Unit = {
      client.close()
      thread.interrupt()
      sendJsonStrings(queue.dequeueAll(_ => true), url)
    }

    def open(partitionId: Long, version: Long): Boolean = {
      client = HttpClientBuilder.create().build()
      thread = new Thread {
        override def run(): Unit =processBatch()
      }
      thread.start()
      true
    }
  }

  def stream(df: DataFrame, url: String, batchInterval:Int = 1000): DataStreamWriter[Row] = {
    df.select(to_json(struct(df.columns.map(col): _*)).alias("json"))
      .writeStream.foreach(new PowerBIWriter(url, batchInterval))
  }

  def write(df: DataFrame, url: String): Unit = {
    df.select(to_json(struct(df.columns.map(col): _*)).alias("json"))
      .foreachPartition { rows =>
        sendJsonStrings(rows.map(_.getString(0)).toSeq,url)
      }
  }

}
