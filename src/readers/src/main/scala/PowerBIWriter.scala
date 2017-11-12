// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.sql.Timestamp

import org.apache.hadoop.conf.{Configuration => HConf}
import org.apache.hadoop.io.{IOUtils => HUtils}
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row}

import scala.util.parsing.json.JSONFormat.{ValueFormatter, quoteString}
import scala.util.parsing.json.{JSONArray, JSONObject}

object PowerBIWriter {

  private val customFormatter: ValueFormatter = {
    case s: String => "\"" + quoteString(s) + "\""
    case jo: JSONObject => jo.toString(customFormatter)
    case ja: JSONArray => ja.toString(customFormatter)
    case t: Timestamp => "\"" + t.toString + "\""
    case other => other.toString
  }

  private def toJson(row: Row) = {
    val nonNullFields = row.schema.fieldNames
      .zipWithIndex
      .filter(p => !row.isNullAt(p._2))
      .map(_._1)
    JSONObject(row.getValuesMap(nonNullFields))
  }

  private class PowerBIWriter(val url: String) extends ForeachWriter[Row] {
    var client: CloseableHttpClient = _

    def process(value: Row): Unit = {
      val json = JSONArray(List(toJson(value))).toString(customFormatter)
      val post = new HttpPost(url)
      post.setHeader("Content-type", "application/json")
      post.setEntity(new StringEntity(json))
      val response = client.execute(post)
      assert(response.getStatusLine.getStatusCode == 200, response.toString)
    }

    def close(errorOrNull: Throwable): Unit = {
      client.close()
    }

    def open(partitionId: Long, version: Long): Boolean = {
      client = HttpClientBuilder.create().build()
      true
    }
  }

  def stream(df: DataFrame, url: String): DataStreamWriter[Row] = {
    df.writeStream.foreach(new PowerBIWriter(url))
  }

  def write(df: DataFrame, url: String): Unit = {
    df.foreachPartition { rows =>
      val json = JSONArray(rows.map(toJson).toList).toString(customFormatter)
      val post = new HttpPost(url)
      post.setHeader("Content-type", "application/json")
      post.setEntity(new StringEntity(json))

      StreamUtilities.using(HttpClientBuilder.create().build()) { client =>
        val response = client.execute(post)
        assert(response.getStatusLine.getStatusCode == 200, response.toString)
      }.get
    }
  }

}
