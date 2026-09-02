// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.split1

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.HandlingUtils
import org.apache.http.HttpVersion
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.entity.BasicHttpEntity
import org.apache.http.message.{BasicHttpResponse, BasicStatusLine}

import java.io.{IOException, InputStream}
import scala.io.Source

class VerifyResponseBodyInspection extends TestBase {

  test("response inspection replays bytes after an input failure") {
    val content = """{"error":{"code":"RateLimitExceeded"}}""".getBytes("UTF-8")
    val input = new InputStream {
      private var index = 0
      private var failed = false

      override def read(): Int = {
        if (index < content.length) {
          val value = content(index) & 0xff
          index += 1
          value
        } else {
          failOrFinish()
        }
      }

      override def read(buffer: Array[Byte], offset: Int, length: Int): Int = {
        if (index < content.length) {
          val count = math.min(length, content.length - index)
          System.arraycopy(content, index, buffer, offset, count)
          index += count
          count
        } else {
          failOrFinish()
        }
      }

      private def failOrFinish(): Int = {
        if (!failed) {
          failed = true
          throw new IOException("simulated response read failure")
        }
        -1
      }
    }
    val entity = new BasicHttpEntity()
    entity.setContent(input)
    entity.setContentLength(-1)
    val response = new BasicHttpResponse(
      new BasicStatusLine(HttpVersion.HTTP_1_1, 429, "Too Many Requests"))
      with CloseableHttpResponse {
      override def close(): Unit = ()
    }
    response.setEntity(entity)

    assert(HandlingUtils.responseBodyForInspection(response).isEmpty)
    val replayed = Source.fromInputStream(response.getEntity.getContent, "UTF-8")
    val replayedBody = try {
      replayed.mkString
    } finally {
      replayed.close()
    }

    assert(replayedBody === new String(content, "UTF-8"))
  }
}
