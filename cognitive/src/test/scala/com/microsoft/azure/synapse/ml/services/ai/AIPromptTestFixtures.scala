// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.core.test.fuzzing.TransformerFuzzing
import com.microsoft.azure.synapse.ml.param.RPrinter
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import spray.json._

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.{ConcurrentLinkedQueue, Executors}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._
import scala.util.Using

private[ai] final class AIRecordingServer(reply: (Int, String) => (Int, String),
                                         retryAfter: String = "0",
                                         responseHeaders: Map[String, String] = Map.empty,
                                         encodeResponse: String => Array[Byte] =
                                           _.getBytes(StandardCharsets.UTF_8)) extends AutoCloseable {
  private val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
  private val pool = Executors.newCachedThreadPool()
  val requests = new ConcurrentLinkedQueue[(String, String)]()
  val headers = new ConcurrentLinkedQueue[Map[String, String]]()
  server.setExecutor(pool)
  server.createContext("/", (exchange: HttpExchange) => {
    val body = Using.resource(exchange.getRequestBody) { in =>
      new String(IOUtils.toByteArray(in), StandardCharsets.UTF_8)
    }
    requests.add(exchange.getRequestURI.toString -> body)
    headers.add(exchange.getRequestHeaders.asScala.map { case (name, values) =>
      name.toLowerCase(java.util.Locale.ROOT) -> values.asScala.mkString(",")
    }.toMap)
    val (status, response) = reply(requests.size(), body)
    val bytes = encodeResponse(response)
    exchange.getResponseHeaders.add("Content-Type", "application/json")
    responseHeaders.foreach { case (name, value) => exchange.getResponseHeaders.add(name, value) }
    if (status == 429) exchange.getResponseHeaders.add("Retry-After", retryAfter)
    exchange.sendResponseHeaders(status, bytes.length)
    Using.resource(exchange.getResponseBody)(_.write(bytes))
    exchange.close()
  })
  server.start()
  def url: String = s"http://127.0.0.1:${server.getAddress.getPort}"
  override def close(): Unit = {
    server.stop(0)
    pool.shutdownNow()
  }
}

private[ai] class AIPromptRecordingServer extends AutoCloseable {
  val requests: AtomicInteger = new AtomicInteger()
  private val server = new AIRecordingServer((_, body) => {
    requests.incrementAndGet()
    if (body.contains("ROW_ERROR")) {
      400 -> """{"error":{"code":"invalid_request","message":"synthetic row error"}}"""
    } else {
      200 -> """{"id":"local-response","model":"local-model","choices":[{"index":0,
        |"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}],
        |"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}""".stripMargin
    }
  })
  val url: String = server.url
  override def close(): Unit = server.close()
}

private[ai] class TestableAIFabricAuthPrompt(override val uid: String) extends AIPrompt(uid) {
  override private[ai] def fabricEndpoint: Option[String] =
    Some("https://fabric.example.invalid/cognitive/openai")

  override private[ai] def fabricAuthHeader: String = "MwcToken synthetic"

  override private[ai] def assertFabricModelStatus(model: String): Unit =
    require(model != "blocked-model", "Synthetic Fabric model is unavailable")
}

private[ai] class TestableAIThrowingFabricAuthPrompt(override val uid: String)
  extends TestableAIFabricAuthPrompt(uid) {
  override private[ai] def fabricAuthHeader: String =
    throw new IllegalStateException("The unused Fabric fallback must not be evaluated")
}

private[ai] object AIChatContentFixtures {
  def mixed(spark: org.apache.spark.sql.SparkSession): DataFrame = {
    val content = ArrayType(MapType(StringType, StringType))
    val schema = new StructType().add("messages",
      ArrayType(new StructType().add("role", StringType).add("content", content)))
    val parts = Seq(Seq.empty[Map[String, String]], Seq(Map("type" -> "text", "text" -> "valid")),
      Seq(Map("type" -> "audio", "data" -> "AQID")), Seq(Map("type" -> "unknown", "text" -> "not supported")),
      Seq(Map("type" -> "text")), Seq(Map("type" -> "text", "text" -> "hello", "extra" -> "lost")))
    spark.createDataFrame(parts.map(content => Row(Seq(Row("user", content)))).asJava, schema)
  }
}

private[services] object AILiveTestEnvironment {
  def get(name: String): Option[String] = sys.env.get(name).map(_.trim).filter(_.nonEmpty)
}

private[services] object NativeRequestFixtures {
  val SystemQuestions: String =
    """{"repeat_contact":{"type":"noul","instructions":"Has the customer contacted support before?"},
      |"team":{"type":"choice","instructions":"Which team should handle this request?",
      |"criteria":{"billing":"Charges, invoices and refunds","engineering":"Bugs, errors and outages"}},
      |"severity":{"type":"score","instructions":"How severe is this issue?",
      |"criteria":["Cosmetic","Major","Critical"]}}""".stripMargin
  val DecisionQuestions: String =
    """[{"type":"predicate","name":"repeat_contact","instructions":"Has the customer contacted support before?"},
      |{"type":"choice","name":"team","instructions":"Which team should handle this request?",
      |"choices":[{"value":"billing","description":"Charges, invoices and refunds"},
      |{"value":"engineering","description":"Bugs, errors and outages"}]},
      |{"type":"score","name":"severity","instructions":"How severe is this issue?",
      |"levels":[{"label":"Cosmetic","description":"Appearance only"},
      |{"label":"Major","description":"A task fails but a workaround exists"},
      |{"label":"Critical","description":"A task is fully blocked"}]}]""".stripMargin
  val SystemResponse: String =
    """{"model":"microsoft-decision-1","answers":{"repeat_contact":true,
      |"team":{"type":"choice","choice":"engineering","additional":[1,false,null]},
      |"severity":1.7},"usage":{"input_tokens":20,"output_tokens":0,"total_tokens":20},"future":true}""".stripMargin
  val DecisionResponse: String =
    """{"model":"gpt-6-luna","answers":[{"type":"predicate","name":"repeat_contact","probability":0.98},
      |{"type":"choice","name":"team","choice":"engineering","confidence":0.9,
      |"probabilities":[{"value":"billing","probability":0.05},{"value":"engineering","probability":0.95}]},
      |{"type":"score","name":"severity","score":1.7,"confidence":0.8,"probabilities":[
      |{"value":0,"label":"Cosmetic","probability":0.1},{"value":1,"label":"Major","probability":0.1},
      |{"value":2,"label":"Critical","probability":0.8}]}],
      |"usage":{"input_tokens":20,"output_tokens":0,"total_tokens":20}}""".stripMargin
}

private[services] trait AIPromptFuzzing[S <: AIPrompt] extends TransformerFuzzing[S] {
  override def rTestInstantiateModel(stage: S, num: Int): String = {
    // Reuse the UID so native-incompatible defaults need not become explicit constructor arguments.
    val lines = super.rTestInstantiateModel(stage, num).split("\n")
    val uidLines = lines.indices.filter(index => lines(index).trim.startsWith(",uid = "))
    require(uidLines.length == 1, "Expected exactly one generated R constructor UID")
    lines.updated(uidLines.head, s",uid = ${RPrinter(JsString(stage.uid))})").mkString("\n")
  }

  override def pyTestInstantiateModel(stage: S, num: Int): String = {
    // Keep UID-derived defaults implicit when the generator falls back to explicit Params.
    val columns = JsObject("messagesCol" -> JsString(stage.getMessagesCol),
      "responseIdCol" -> JsString(stage.getResponseIdCol)).compactPrint
    super.pyTestInstantiateModel(stage, num) + s"model._setDefault(**$columns)\n"
  }
}
