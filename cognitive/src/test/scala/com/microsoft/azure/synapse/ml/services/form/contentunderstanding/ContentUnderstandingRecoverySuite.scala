// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.form.contentunderstanding

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.services.contentunderstanding.{
  ContentUnderstanding, ContentUnderstandingException}
import org.apache.commons.io.FileUtils
import org.apache.http.HttpStatus
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import spray.json._

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.collection.JavaConverters._

class ContentUnderstandingRecoverySuite extends TestBase {
  import ContentUnderstandingFixtures.{
    Accepted => accepted, LocationPath => locationPath, Succeeded => succeeded, TooManyRequests}
  import ContentUnderstandingStub.withReplies

  private def withJournal(test: String => Unit): Unit = {
    val directory = Files.createTempDirectory("cu-recovery-")
    try {
      test(directory.resolve("journal").toString)
    } finally {
      FileUtils.deleteDirectory(directory.toFile)
    }
  }

  private def stage(server: ContentUnderstandingStub): ContentUnderstanding =
    new ContentUnderstanding().setEndpoint(server.endpoint).setOutputCol("result").setErrorCol("error")
      .setPollingDelay(0).setMaxPollAttempts(3)

  private def input: DataFrame =
    spark.createDataFrame(Seq(Row("doc")).asJava, new StructType().add("id", StringType)).coalesce(1)

  test("large chunked and compressed responses preserve the size-limit error and saved handle") {
    val bytes = new Array[Byte](65536)
    new scala.util.Random(1234).nextBytes(bytes)
    val body = JsObject(succeeded.parseJson.asJsObject.fields +
      ("padding" -> JsString(java.util.Base64.getEncoder.encodeToString(bytes)))).compactPrint
    Seq(false, true).foreach { gzip =>
      withReplies(Seq(accepted, ContentUnderstandingStubReply(HttpStatus.SC_OK, body,
        chunked = true, gzip = gzip))) { server =>
        withJournal { path =>
          val transformer = stage(server).setDocumentBytes(Array[Byte](1))
            .setMaxResponseBytes(1024).setMaxPollAttempts(2)
          val failure = intercept[ContentUnderstandingException] {
            transformer.writeToPath(input, "id", path, "parquet")
          }
          assert(failure.response.error.exists(_.contains("ResponseTooLarge")))
          assert(server.requests.map(_.method) == Seq("POST", "GET"))
          assert(transformer.readPath(spark, path, "parquet").head().getAs[String]("status") == "Running")
          val resumed = transformer.setMaxResponseBytes(body.getBytes(StandardCharsets.UTF_8).length)
            .writeToPath(input, "id", path, "parquet").head()
          assert(resumed.getAs[String]("status") == "Succeeded")
          assert(resumed.getAs[String]("rawResponse") == body)
          assert(server.requests.map(_.method) == Seq("POST", "GET", "GET"))
        }
      }
    }
  }

  test("writer polling leaves oversized results resumable after the response cap is raised") {
    withReplies(Seq(accepted, ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded))) { server =>
      withJournal { path =>
        val transformer = stage(server).setDocumentBytes(Array[Byte](1)).setMaxResponseBytes(128)
          .setMaxPollAttempts(1)
        val failure = intercept[ContentUnderstandingException] {
          transformer.writeToPath(input, "id", path, "parquet")
        }
        assert(failure.response.error.exists(_.contains("ResponseTooLarge")))
        assert(failure.response.operationLocation.contains(server.endpoint + locationPath))
        val pending = transformer.readPath(spark, path, "parquet").collect().head
        assert(pending.getAs[String]("status") == "Running")
        assert(pending.getAs[String]("operationLocation") == server.endpoint + locationPath)
        val resumed = transformer.setMaxResponseBytes(4096).writeToPath(input, "id", path, "parquet").collect().head
        assert(resumed.getAs[String]("status") == "Succeeded")
        assert(server.requests.count(_.method == "POST") == 1)
        assert(server.requests.count(_.method == "GET") == 2)
        assert(server.requests.filter(_.method == "GET").map(_.path).distinct.size == 1)
      }
    }
  }

  test("an accepted handle survives an oversized or malformed submission response") {
    val complete = """{"id":"op-1","status":"Succeeded","result":{"contents":[]}}"""
    Seq("x" * 129, "not-json").foreach { body =>
      withReplies(Seq(accepted.copy(body = body),
        ContentUnderstandingStubReply(HttpStatus.SC_OK, complete))) { server =>
        withJournal { path =>
          val transformer = stage(server).setDocumentBytes(Array[Byte](1)).setMaxResponseBytes(128)
          val result = transformer.writeToPath(input, "id", path, "parquet").head()
          assert(result.getAs[String]("status") == "Succeeded")
          assert(result.getAs[String]("operationLocation") == server.endpoint + locationPath)
          assert(server.requests.map(_.method) == Seq("POST", "GET"))
          val history = spark.read.parquet(path).orderBy("sequence").select("status").collect()
          assert(history.map(_.getString(0)).toSeq == Seq("Unknown", "Succeeded"))
        }
      }
    }
  }

  test("an unknown submission is journaled and is not resubmitted without an operation handle") {
    val replies = Seq(
      ContentUnderstandingStubReply(0, "", disconnect = true) -> "TransportError",
      ContentUnderstandingStubReply(HttpStatus.SC_OK, "not-json") -> "InvalidResponse",
      accepted.copy(headers = Map.empty) -> "MissingOperationLocation",
      ContentUnderstandingStubReply(HttpStatus.SC_INTERNAL_SERVER_ERROR, "{}",
        Map("Operation-Location" -> "https://example.invalid/unsafe")) -> "InvalidOperationLocation")
    replies.foreach { case (reply, errorCode) =>
      withReplies(Seq(reply)) { server =>
        withJournal { path =>
          val transformer = stage(server).setDocumentBytes(Array[Byte](1))
          val first = intercept[ContentUnderstandingException] {
            transformer.writeToPath(input, "id", path, "parquet")
          }
          assert(first.response.status == "Unknown")
          val recorded = transformer.readPath(spark, path, "parquet").head()
          assert(recorded.getAs[String]("status") == "Unknown")
          assert(recorded.getAs[String]("error").contains(errorCode))
          val retry = intercept[IllegalArgumentException] {
            transformer.writeToPath(input, "id", path, "parquet")
          }
          assert(retry.getMessage.contains("unknown"))
          assert(server.requests.map(_.method) == Seq("POST"))
        }
      }
    }
  }

  test("exhausted transient polling errors leave the committed handle available for retry") {
    val throttled = ContentUnderstandingStubReply(TooManyRequests,
      """{"error":{"code":"TooManyRequests"}}""", Map("Retry-After" -> "0"))
    withReplies(Seq(accepted, throttled)) { server =>
      withJournal { path =>
        val transformer = stage(server).setDocumentBytes(Array[Byte](1)).setMaxPollAttempts(1)
        val failure = intercept[ContentUnderstandingException] {
          transformer.writeToPath(input, "id", path, "parquet")
        }
        assert(failure.response.httpStatus == TooManyRequests)
        val pending = transformer.readPath(spark, path, "parquet").head()
        assert(pending.getAs[String]("status") == "Running")
        assert(pending.getAs[String]("operationLocation") == server.endpoint + locationPath)
        assert(server.requests.map(_.method) == Seq("POST", "GET"))
      }
    }
  }

  test("polling configuration and credential errors do not terminalize an accepted operation") {
    Seq(HttpStatus.SC_BAD_REQUEST, HttpStatus.SC_UNAUTHORIZED, HttpStatus.SC_FORBIDDEN).foreach { code =>
      val rejected = ContentUnderstandingStubReply(code, """{"error":{"code":"PollingRejected"}}""")
      withReplies(Seq(accepted, rejected, ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded))) { server =>
        withJournal { path =>
          val transformer = stage(server).setDocumentBytes(Array[Byte](1)).setAADToken("expired-test-token")
          val failure = intercept[ContentUnderstandingException] {
            transformer.writeToPath(input, "id", path, "parquet")
          }
          assert(failure.response.httpStatus == code)
          assert(transformer.readPath(spark, path, "parquet").head().getAs[String]("status") == "Running")
          val resumed = transformer.setAADToken("refreshed-test-token").writeToPath(input, "id", path, "parquet")
          assert(resumed.head().getAs[String]("status") == "Succeeded")
          assert(server.requests.count(_.method == "POST") == 1)
          assert(server.requests.last.headers("authorization") == "Bearer refreshed-test-token")
        }
      }
    }
  }
}
