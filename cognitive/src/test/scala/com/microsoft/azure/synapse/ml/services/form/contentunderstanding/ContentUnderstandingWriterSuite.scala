// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.form.contentunderstanding

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.services.contentunderstanding.{
  ContentUnderstanding, ContentUnderstandingException, ContentUnderstandingWriter}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import spray.json._

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

class ContentUnderstandingWriterSuite extends TestBase {
  import spark.implicits._

  private class Service extends AutoCloseable {
    private val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    private val operations = new ConcurrentHashMap[String, String]()
    private val submissions = new ConcurrentHashMap[String, AtomicInteger]()
    val malformed = new AtomicReference[String]("")
    val pending = new AtomicReference[String]("")
    val failed = new AtomicReference[String]("")

    def endpoint: String = s"http://127.0.0.1:${server.getAddress.getPort}"
    def submitted(name: String): Int = Option(submissions.get(name)).map(_.get()).getOrElse(0)
    def totalSubmissions: Int = {
      import scala.collection.JavaConverters._
      submissions.values().asScala.map(_.get()).sum
    }

    private def send(exchange: HttpExchange, code: Int, body: String): Unit = {
      val bytes = body.getBytes(StandardCharsets.UTF_8)
      exchange.getResponseHeaders.add("Content-Type", "application/json")
      exchange.sendResponseHeaders(code, bytes.length)
      exchange.getResponseBody.write(bytes)
    }

    server.createContext("/contentunderstanding", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        try {
          if (exchange.getRequestMethod == "POST") {
            val json = IOUtils.toString(exchange.getRequestBody, StandardCharsets.UTF_8).parseJson.asJsObject
            val input = json.fields("inputs").asInstanceOf[JsArray].elements.head.asJsObject
            val name = input.fields("name").asInstanceOf[JsString].value
            submissions.computeIfAbsent(name, _ => new AtomicInteger()).incrementAndGet()
            val id = UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8)).toString
            operations.put(id, name)
            val location = s"$endpoint/contentunderstanding/analyzerResults/$id?api-version=2025-11-01"
            exchange.getResponseHeaders.add("Operation-Location", location)
            send(exchange, 202, s"""{"id":"$id","status":"Running","result":{"contents":[]}}""")
          } else {
            val id = exchange.getRequestURI.getPath.split("/").last
            val name = operations.get(id)
            if (name == malformed.get()) {
              send(exchange, 200, "{")
            } else if (name == pending.get()) {
              send(exchange, 200, s"""{"id":"$id","status":"Running","result":{"contents":[]}}""")
            } else if (name == failed.get()) {
              send(exchange, 200,
                s"""{"id":"$id","status":"Failed","error":{"code":"InvalidRequest"},"result":{"contents":[]}}""")
            } else {
              send(exchange, 200,
                s"""{"id":"$id","status":"Succeeded","usage":{"documentPagesBasic":1},
                   |"result":{"contents":[{"kind":"document","markdown":"synthetic $name",
                   |"fields":{"Optional":{"type":"string","confidence":0.9}}}]},
                   |"futureProperty":{"preserve":true}}""".stripMargin)
            }
          }
        } finally {
          exchange.close()
        }
      }
    })
    server.start()

    override def close(): Unit = server.stop(0)
  }

  private def withService(testCode: Service => Unit): Unit = {
    val service = new Service
    try {
      testCode(service)
    } finally {
      service.close()
    }
  }

  private def withOutput(testCode: String => Unit): Unit = {
    val directory = Files.createTempDirectory("cu-writer-")
    try {
      testCode(directory.resolve("journal").toString)
    } finally {
      FileUtils.deleteDirectory(directory.toFile)
    }
  }

  private def analyzer(service: Service): ContentUnderstanding =
    new ContentUnderstanding()
      .setEndpoint(service.endpoint)
      .setSubscriptionKey("synthetic-test-key")
      .setDocumentUrlCol("source")
      .setDocumentNameCol("docId")
      .setMaxPollAttempts(1)
      .setPollingDelay(0)

  private def documents: DataFrame =
    Seq(("a", "https://example.test/a.pdf"), ("b", "https://example.test/b.pdf")).toDF("docId", "source")

  test("committed results and operation handles survive a later polling failure and resume without POSTs") {
    withService { service =>
      withOutput { path =>
        val stage = analyzer(service)
        service.malformed.set("b")
        intercept[Exception] {
          ContentUnderstandingWriter.writeToPath(documents, stage, "docId", path, "parquet")
        }
        val partial = ContentUnderstandingWriter.readPath(spark, path, "parquet")
        val states = partial.select("documentId", "status").collect().map(r => r.getString(0) -> r.getString(1)).toMap
        assert(states == Map("a" -> "Succeeded", "b" -> "Running"))
        assert(partial.filter(col("documentId") === "b").select("operationLocation").head().getString(0).nonEmpty)
        assert(service.submitted("a") == 1)
        assert(service.submitted("b") == 1)

        service.malformed.set("")
        val resumed = ContentUnderstandingWriter.writeToPath(documents, stage, "docId", path, "parquet")
        assert(resumed.filter(col("status") === "Succeeded").count() == 2)
        assert(service.totalSubmissions == 2)
        val raw = resumed.select("rawResponse").head().getString(0).parseJson.asJsObject
        assert(raw.fields.contains("usage"))
        assert(raw.fields.contains("futureProperty"))
        val journal = spark.read.parquet(path)
        assert(journal.count() == 4)
      }
    }
  }

  test("poll budget exhaustion remains resumable and does not submit the document again") {
    withService { service =>
      withOutput { path =>
        val stage = analyzer(service)
        val input = documents.filter(col("docId") === "a")
        service.pending.set("a")
        val pending = ContentUnderstandingWriter.writeToPath(input, stage, "docId", path, "parquet")
        assert(pending.select("status").head().getString(0) == "Running")
        service.pending.set("")
        val done = ContentUnderstandingWriter.writeToPath(input, stage, "docId", path, "parquet")
        assert(done.select("status").head().getString(0) == "Succeeded")
        assert(service.submitted("a") == 1)
      }
    }
  }

  test("retains service failures as terminal records without repeating invalid requests") {
    withService { service =>
      withOutput { path =>
        service.failed.set("a")
        val stage = analyzer(service)
        val first = ContentUnderstandingWriter.writeToPath(documents, stage, "docId", path, "parquet", 2)
        assert(first.filter(col("status") === "Failed").count() == 1)
        assert(first.filter(col("documentId") === "a").select("error").head().getString(0).contains("InvalidRequest"))
        ContentUnderstandingWriter.writeToPath(documents, stage, "docId", path, "parquet", 2)
        assert(service.totalSubmissions == 2)
      }
    }
  }

  test("rejects an ID reused with changed content or analysis configuration") {
    withService { service =>
      withOutput { path =>
        val stage = analyzer(service)
        val input = documents.filter(col("docId") === "a")
        ContentUnderstandingWriter.writeToPath(input, stage, "docId", path, "parquet")
        intercept[IllegalArgumentException] {
          ContentUnderstandingWriter.writeToPath(
            Seq(("a", "https://example.test/changed.pdf")).toDF("docId", "source"),
            stage, "docId", path, "parquet")
        }
        intercept[IllegalArgumentException] {
          ContentUnderstandingWriter.writeToPath(
            input, analyzer(service).setRange("1-2"), "docId", path, "parquet")
        }
        assert(service.totalSubmissions == 1)
      }
    }
  }

  test("validates IDs and the destination before making any service request") {
    withService { service =>
      withOutput { path =>
        val stage = analyzer(service)
        val invalidInputs = Seq(
          Seq(("a", "https://example.test/a"), ("a", "https://example.test/b")).toDF("docId", "source"),
          Seq((Option.empty[String].orNull, "https://example.test/a")).toDF("docId", "source"),
          Seq((" ", "https://example.test/a")).toDF("docId", "source"),
          Seq(("\t\n", "https://example.test/a")).toDF("docId", "source")
        )
        invalidInputs.foreach { input =>
          intercept[IllegalArgumentException] {
            ContentUnderstandingWriter.writeToPath(input, stage, "docId", path, "parquet")
          }
        }
        Seq("unrelated").toDF("value").write.parquet(path)
        intercept[IllegalArgumentException] {
          ContentUnderstandingWriter.writeToPath(documents, stage, "docId", path, "parquet")
        }
        assert(service.totalSubmissions == 0)
      }
    }
  }

  test("writes and resumes a catalog table through the same public journal API") {
    withService { service =>
      val tableName = "cu_writer_" + UUID.randomUUID().toString.replace("-", "")
      try {
        val stage = analyzer(service)
        val result = ContentUnderstandingWriter.writeToTable(documents, stage, "docId", tableName, "parquet", 2)
        assert(result.count() == 2)
        assert(ContentUnderstandingWriter.readTable(spark, tableName).count() == 2)
        ContentUnderstandingWriter.writeToTable(documents, stage, "docId", tableName, "parquet", 1)
        assert(service.totalSubmissions == 2)
      } finally {
        spark.sql(s"DROP TABLE IF EXISTS `$tableName`")
      }
    }
  }

  test("empty input creates a readable empty journal without calling the service") {
    withService { service =>
      withOutput { path =>
        val result = ContentUnderstandingWriter.writeToPath(
          documents.limit(0), analyzer(service), "docId", path, "parquet")
        assert(result.count() == 0)
        assert(service.totalSubmissions == 0)
      }
    }
  }

  test("treats punctuation in input column names literally and allows credential rotation on resume") {
    withService { service =>
      withOutput { path =>
        val input = Seq(("a", "https://example.test/a.pdf")).toDF("doc.`id", "file.uri")
        val stage = analyzer(service).setDocumentUrlCol("file.uri").setDocumentNameCol("doc.`id")
        val first = stage.writeToPath(input, "doc.`id", path, "parquet")
        assert(first.select("documentId").head().getString(0) == "a")
        stage.setSubscriptionKey("synthetic-rotated-key")
        stage.writeToPath(input, "doc.`id", path, "parquet")
        assert(stage.readPath(spark, path, "parquet").count() == 1)
        assert(service.totalSubmissions == 1)
      }
    }
  }

  test("rejects invalid batch sizes and poll-only configuration before submission") {
    withService { service =>
      withOutput { path =>
        val stage = analyzer(service)
        intercept[IllegalArgumentException] {
          ContentUnderstandingWriter.writeToPath(documents, stage, "docId", path, "parquet", 0)
        }
        intercept[IllegalArgumentException] {
          ContentUnderstandingWriter.writeToPath(
            documents, stage.setOperationMode("poll"), "docId", path, "parquet")
        }
        assert(service.totalSubmissions == 0)
      }
    }
  }

  private val accepted = ContentUnderstandingStubReply(202, """{"id":"op","status":"Running"}""",
    Map("Operation-Location" -> "$ROOT/contentunderstanding/analyzerResults/op?api-version=2025-11-01"))
  private val completed = ContentUnderstandingStubReply(200,
    """{"id":"op","status":"Succeeded","result":{"contents":[]}}""")

  private def stubAnalyzer(server: ContentUnderstandingStub): ContentUnderstanding =
    new ContentUnderstanding().setEndpoint(server.endpoint).setDocumentBytes(Array[Byte](1))
      .setDocumentNameCol("docId").setPollingDelay(0).setMaxPollAttempts(1)

  test("writer recovery retries definite admission rejections without poisoning completed IDs") {
    Seq(401, 403, 429).foreach { code =>
      val rejected = ContentUnderstandingStubReply(code, """{"error":{"code":"Rejected"}}""")
      ContentUnderstandingStub.withReplies(Seq(accepted, completed, rejected, accepted, completed)) { server =>
        withOutput { path =>
          val stage = stubAnalyzer(server)
          val failure = intercept[ContentUnderstandingException] {
            stage.writeToPath(documents, "docId", path, "parquet")
          }
          assert(failure.response.status == "Rejected")
          assert(failure.response.httpStatus == code)
          val partial = stage.readPath(spark, path, "parquet").collect()
          assert(partial.length == 1)
          assert(partial.head.getAs[String]("documentId") == "a")
          assert(partial.head.getAs[String]("status") == "Succeeded")
          val resumed = stage.setAADToken("refreshed-test-token")
            .writeToPath(documents, "docId", path, "parquet")
          assert(resumed.filter(col("status") === "Succeeded").count() == 2)
          assert(server.requests.count(_.method == "POST") == 3)
        }
      }
    }
  }

  test("writer recovery records indeterminate server errors without resubmitting them") {
    Seq(408, 500, 503).foreach { code =>
      ContentUnderstandingStub.withReplies(Seq(
        ContentUnderstandingStubReply(code, """{"error":{"code":"Indeterminate"}}"""))) { server =>
        withOutput { path =>
          val stage = stubAnalyzer(server)
          val failure = intercept[ContentUnderstandingException] {
            stage.writeToPath(documents, "docId", path, "parquet")
          }
          assert(failure.response.status == "Unknown")
          assert(stage.readPath(spark, path, "parquet").head().getAs[String]("status") == "Unknown")
          intercept[IllegalArgumentException] {
            stage.writeToPath(documents, "docId", path, "parquet")
          }
          assert(server.requests.count(_.method == "POST") == 1)
        }
      }
    }
  }

  test("writer recovery records unavailable results and continues with later documents") {
    Seq(404, 410).foreach { code =>
      val unavailable = ContentUnderstandingStubReply(code, """{"error":{"code":"ResultNotFound"}}""")
      ContentUnderstandingStub.withReplies(Seq(accepted, accepted, unavailable, accepted, completed)) { server =>
        withOutput { path =>
          val stage = stubAnalyzer(server)
          val first = stage.writeToPath(documents.filter(col("docId") === "a"), "docId", path, "parquet")
          assert(first.head().getAs[String]("status") == "Running")
          val resumed = stage.writeToPath(documents, "docId", path, "parquet")
          val missing = resumed.filter(col("documentId") === "a").head()
          assert(missing.getAs[String]("status") == "ResultUnavailable")
          assert(missing.getAs[Int]("httpStatus") == code)
          assert(missing.getAs[String]("error").contains("ResultNotFound"))
          assert(resumed.filter(col("documentId") === "b").head().getAs[String]("status") == "Succeeded")
          stage.writeToPath(documents, "docId", path, "parquet")
          assert(server.requests.map(_.method) == Seq("POST", "GET", "GET", "POST", "GET"))
        }
      }
    }
  }
}
