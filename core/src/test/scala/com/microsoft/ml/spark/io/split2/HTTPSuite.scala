// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.io.split2

import java.io.File

import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.io.http.HTTPSchema.string_to_response
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.spark.sql.execution.streaming.{HTTPSinkProvider, HTTPSourceProvider}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

class HTTPSuite extends TestBase with HTTPTestUtils {

  test("stream from HTTP") {
    val q1 = spark.readStream.format(classOf[HTTPSourceProvider].getName)
      .option("host", host)
      .option("port", port.toString)
      .option("path", apiPath)
      .load()
      .withColumn("contentLength", col("request.entity.contentLength"))
      .withColumn("reply", string_to_response(col("contentLength").cast(StringType)))
      .writeStream
      .format(classOf[HTTPSinkProvider].getName)
      .option("name", "foo")
      .queryName("foo")
      .option("replyCol", "reply")
      .option("checkpointLocation", new File(tmpDir.toFile, "checkpoints").toString)
      .start()

    Thread.sleep(5000)
    val client = HttpClientBuilder.create().build()
    val p1 = sendJsonRequest(client, Map("foo" -> 1, "bar" -> "here"), url)
    val p2 = sendJsonRequest(client, Map("foo" -> 1, "bar" -> "heree"), url)
    val p3 = sendJsonRequest(client, Map("foo" -> 1, "bar" -> "hereee"), url)
    val p4 = sendJsonRequest(client, Map("foo" -> 1, "bar" -> "hereeee"), url)
    val posts = List(p1, p2, p3, p4)
    val correctResponses = List(27, 28, 29, 30)

    posts.zip(correctResponses).foreach { p =>
      assert(p._1 === p._2.toString)
    }
    q1.stop()
    client.close()
  }

}
