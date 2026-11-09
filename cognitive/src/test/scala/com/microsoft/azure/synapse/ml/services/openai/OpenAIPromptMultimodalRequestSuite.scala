// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.contracts.HasOutputCol
import com.microsoft.azure.synapse.ml.io.http.{ErrorUtils, HasErrorCol}
import com.microsoft.azure.synapse.ml.services.HasCognitiveServiceInput
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.TaskKilledException
import org.apache.spark.injections.TaskContextUtils
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{array, col, lit, struct, to_json}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Seconds, Span}
import spray.json._

import java.io.{File, InterruptedIOException}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.{Callable, ConcurrentLinkedQueue, ExecutionException, Executors, TimeUnit}
import java.util.{Locale, UUID}
import scala.collection.JavaConverters._
import scala.util.Using

class OpenAIPromptMultimodalRequestSuite extends OpenAIPromptHttpTestBase {

  import spark.implicits._

  private def withAttachmentFailureFileSystem(testCode: AttachmentReadFailureFileSystem => Unit): Unit = {
    val conf = new Configuration()
    conf.setClass(s"fs.${AttachmentReadFailures.Location.getScheme}.impl",
      classOf[AttachmentReadFailureFileSystem], classOf[FileSystem])
    // Production uses a fresh Configuration, so inject the fixture through Hadoop's shared filesystem cache.
    Using.resource(FileSystem.get(AttachmentReadFailures.Location, conf)) {
      case fs: AttachmentReadFailureFileSystem =>
        testCode(fs)
        assert(fs.unexpected.isEmpty, s"Unknown attachment failure scenarios: ${fs.unexpected}")
      case _ => fail("Unexpected attachment failure filesystem")
    }
  }

  test("Responses prewarm keeps usage and response ID without a false content-filter error") {
    val previousAnsi = spark.conf.get("spark.sql.ansi.enabled")
    try {
      Seq(false, true).foreach { ansi =>
        spark.conf.set("spark.sql.ansi.enabled", ansi)
        Seq("completed", "incomplete").foreach { status =>
          val response = s"""{"id":"resp_prewarm","status":"$status","output":[],
            |"incomplete_details":{"reason":"content_filter"},
            |"usage":{"input_tokens":12000,"output_tokens":0,"total_tokens":12000,
            |"input_tokens_details":{"cached_tokens":4000,"cache_write_tokens":7000,
            |"cache_write_tokens_details":{"ttl_30m_tokens":3000,"ttl_12h_tokens":4000}},
            |"output_tokens_details":{"reasoning_tokens":0}}}""".stripMargin
          withEchoServer({ (baseUrl, bodies, _) =>
            val stage = prompt(baseUrl, "responses").setColumnTypes(Map.empty[String, String])
              .setStore(true).setResponseIdCol("response_id").setUsageCol("usage")
              .setPromptCacheKey("warmup").setPromptCacheOptions(Map("prewarm" -> true, "ttl" -> "12h"))
              .setReasoning(Map("context" -> "all_turns", "effort" -> "max"))
              .setTruncation("disabled").setTopLogProbs(2)
            val input = Seq("Reusable prefix").toDF("prompt")
            val expectedSchema = stage.transformSchema(input.schema)
            val result = stage.transform(input)
            assert(result.schema == expectedSchema)
            val row = result.head()
            assert(row.isNullAt(row.fieldIndex("output")))
            assert(row.getAs[String]("response_id") == "resp_prewarm")
            val details = row.getAs[Row]("usage").getAs[Map[String, Long]]("input_token_details")
            assert(details("cache_write_tokens") == 7000L)
            assert(details("cache_write_tokens_details.ttl_12h_tokens") == 4000L)
            if (status == "completed") {
              assert(row.isNullAt(row.fieldIndex("error")))
            } else {
              assert(row.getAs[Row]("error").getAs[String]("response") == "content_filter")
            }
            val body = bodies.poll().parseJson.asJsObject.fields
            assert(body("prompt_cache_key") == JsString("warmup"))
            assert(body("prompt_cache_options").asJsObject.fields("prewarm") == JsTrue)
            assert(body("reasoning").asJsObject.fields("context") == JsString("all_turns"))
            assert(body("include") == JsArray(JsString("output_text.logprobs")))
          }, responseBody = _ => response)
        }
      }
    } finally {
      spark.conf.set("spark.sql.ansi.enabled", previousAnsi)
    }
  }

  test("Chat storage and per-row storage controls are forwarded with response IDs") {
    withEchoServer { (baseUrl, bodies) =>
      Seq("chat_completions", "responses").foreach { api =>
        val stage = prompt(baseUrl, api).setColumnTypes(Map.empty[String, String])
          .setStoreCol("persist").setResponseIdCol("response_id")
        val input = Seq(("hello", true), ("hello", false)).toDF("prompt", "persist").coalesce(1)
        val result = stage.transform(input)
        assert(result.schema == stage.transformSchema(input.schema))
        val rows = result.collect()
        assert(rows.forall(row => row.isNullAt(row.fieldIndex("error"))))
        assert(rows.find(_.getAs[Boolean]("persist")).get.getAs[String]("response_id").nonEmpty)
        assert(rows.find(!_.getAs[Boolean]("persist")).get.isNullAt(result.schema.fieldIndex("response_id")))
        val stored = Seq(bodies.poll(), bodies.poll()).map(_.parseJson.asJsObject.fields("store")).toSet
        assert(stored == Set(JsBoolean(true), JsBoolean(false)))
      }
    }
  }

  test("OpenAIPrompt sends scalar and per-row cache options through both APIs") {
    withEchoServer { (baseUrl, bodies) =>
      Seq("chat_completions", "responses").foreach { apiType =>
        val transformer = prompt(baseUrl, apiType).setColumnTypes(Map.empty[String, String])
          .setPromptCacheOptions(Map("mode" -> "implicit", "ttl" -> "30m"))
        val scalar = transformer.transform(Seq("hello").toDF("prompt")).head()
        assert(scalar.isNullAt(scalar.fieldIndex("error")))
        assert(bodies.poll().parseJson.asJsObject.fields("prompt_cache_options") ==
          JsObject("mode" -> JsString("implicit"), "ttl" -> JsString("30m")))

        val rows = Seq(("hello", Map("mode" -> "explicit", "ttl" -> "12h"))).toDF("prompt", "cache_options")
        val column = transformer.setPromptCacheOptionsCol("cache_options").transform(rows).head()
        assert(column.isNullAt(column.fieldIndex("error")))
        assert(bodies.poll().parseJson.asJsObject.fields("prompt_cache_options") ==
          JsObject("mode" -> JsString("explicit"), "ttl" -> JsString("12h")))
      }
    }
  }

  test("provider cache errors retain their response rather than being rejected by client policy") {
    val providerError =
      """{"error":{"type":"invalid_request_error",
        |"message":"Unsupported cache lifetime","param":"prompt_cache_options.ttl"}}""".stripMargin
    withEchoServer(
      (baseUrl, bodies, _) => {
        Seq("chat_completions", "responses").foreach { apiType =>
          val result = prompt(baseUrl, apiType).setColumnTypes(Map.empty[String, String])
            .setPromptCacheOptions(Map("ttl" -> "not-a-cache-lifetime"))
            .transform(Seq("hello").toDF("prompt")).head()
          assert(bodies.poll().parseJson.asJsObject.fields("prompt_cache_options") ==
            JsObject("ttl" -> JsString("not-a-cache-lifetime")))
          val error = result.getAs[Row]("error")
          assert(Option(error).isDefined)
          assert(error.getAs[String]("response").contains("invalid_request_error"))
          assert(error.getAs[String]("response").contains("Unsupported cache lifetime"))
        }
      },
      statusCode = 400,
      responseBody = _ => providerError
    )
  }

  test("struct cache options preserve strings and booleans through Chat Responses and OpenAIPrompt") {
    withEchoServer { (baseUrl, bodies) =>
      val data = Seq(("hello", true), ("hello", false)).toDF("prompt", "prewarm")
        .withColumn("cache_options", struct(lit("30m").as("ttl"), col("prewarm")))
      type CacheTransformer = Transformer with HasOpenAITextParams with HasMessagesInput
        with HasCognitiveServiceInput with HasOutputCol with HasErrorCol
      val stages: Seq[CacheTransformer] = Seq(
        new OpenAIChatCompletion(),
        new OpenAIResponses(),
        new OpenAIPrompt().setApiType("chat_completions").setPromptTemplate("{prompt}"),
        new OpenAIPrompt().setApiType("responses").setPromptTemplate("{prompt}")
      )
      stages.foreach { stage =>
        val input = stage match {
          case _: OpenAIPrompt => data
          case _ => data.withColumn("messages", array(struct(lit("user").as("role"), col("prompt").as("content"))))
        }
        val rows = stage.setUrl(baseUrl).setSubscriptionKey("unused").setDeploymentName("gpt-6-luna")
          .setMessagesCol("messages").setPromptCacheOptionsCol("cache_options")
          .setOutputCol("output").setErrorCol("error").transform(input).collect()
        assert(rows.length == 2 && rows.forall(row => row.isNullAt(row.fieldIndex("error"))))
        assert(bodies.size() == 2)
        val options = Seq.fill(2)(bodies.poll().parseJson.asJsObject.fields("prompt_cache_options")).toSet
        assert(options == Set(
          JsObject("ttl" -> JsString("30m"), "prewarm" -> JsBoolean(true)),
          JsObject("ttl" -> JsString("30m"), "prewarm" -> JsBoolean(false))
        ))
      }
    }
  }

  test("Chat and Responses transforms retain input breakpoints only on marked content") {
    withEchoServer { (baseUrl, bodies) =>
      val stages: Seq[(String, OpenAIServicesBase with HasOpenAITextParams with HasMessagesInput)] = Seq(
        "text" -> new OpenAIChatCompletion(),
        "input_text" -> new OpenAIResponses()
      )
      stages.foreach { case (contentType, stage) =>
        val json =
          s"""{"messages":[
             |{"role":"developer","content":[{"type":"$contentType","text":"Reusable reference",
             |"prompt_cache_breakpoint":{"mode":"explicit"}}]},
             |{"role":"user","content":[{"type":"$contentType","text":"A changing question"}]}]}""".stripMargin
        val input = spark.read.json(Seq(json).toDS())
        val result = stage.setUrl(baseUrl).setSubscriptionKey("unused").setDeploymentName("gpt-6-luna")
          .setMessagesCol("messages").setOutputCol("output").setErrorCol("error")
          .setPromptCacheOptions(Map("mode" -> "explicit", "ttl" -> "30m"))
          .transform(input).head()
        assert(result.isNullAt(result.fieldIndex("error")))
        val field = if (contentType == "text") "messages" else "input"
        val messages = bodies.poll().parseJson.asJsObject.fields(field).asInstanceOf[JsArray].elements
        val prefix = messages.head.asJsObject.fields("content").asInstanceOf[JsArray].elements.head.asJsObject
        val question = messages.last.asJsObject.fields("content").asInstanceOf[JsArray].elements.head.asJsObject
        assert(prefix.fields("prompt_cache_breakpoint") == JsObject("mode" -> JsString("explicit")))
        assert(!question.fields.contains("prompt_cache_breakpoint"))
      }
    }
  }

  private def requestPayload(
      imageForBaseUrl: String => String,
      apiType: String
  ): (Row, JsObject) = {
    var result: Row = null // scalastyle:ignore null
    var payload: JsObject = null // scalastyle:ignore null
    withEchoServer { (baseUrl, bodies) =>
      result = prompt(baseUrl, apiType)
        .transform(Seq(("Describe this image.", imageForBaseUrl(baseUrl))).toDF("prompt", "image"))
        .head()
      val rowError = Option(result.getAs[Row]("error")).map(_.getAs[String]("response")).getOrElse("")
      assert(bodies.size() == 1, rowError)
      payload = bodies.asScala.head.parseJson.asJsObject
    }
    result -> payload
  }

  test("Responses OpenAIPrompt downloads URL images and sends input_image") {
    val (result, payload) = requestPayload(
      _.replace("/openai/v1", "/image.png"),
      "responses"
    )

    assert(Option(result.getAs[Row]("error")).isEmpty)
    val JsArray(messages) = payload.fields("input")
    val JsArray(parts) = messages(1).asJsObject.fields("content")
    assert(parts.map(_.asJsObject.fields("type")) == Seq(JsString("input_text"), JsString("input_image")))
    val JsString(imageUrl) = parts(1).asJsObject.fields("image_url")
    assert(imageUrl == dataImage)
  }

  test("Responses OpenAIPrompt preserves AI Functions row JSON and URL attachments") {
    withEchoServer { (baseUrl, bodies, attachmentRequests) =>
      val imageUrl = baseUrl.replace("/openai/v1", "/image")
      val input = Seq((imageUrl, "Ace")).toDF("image_path", "master")
      val rowJsonCol = "ai_functions_row_json"
      val prepared = input.withColumn(rowJsonCol, to_json(struct(input.columns.map(col): _*)))

      val result = new OpenAIPrompt()
        .setUrl(baseUrl)
        .setApiType("responses")
        .setDeploymentName("gpt-6-luna")
        .setSubscriptionKey("unused")
        .setSystemPrompt("User input text is encoded in JSON\nIdentify the Pokemon and its master.")
        .setPromptTemplate(s"{$rowJsonCol}")
        .setColumnTypes(Map("image_path" -> "path"))
        .setOutputCol("output")
        .setErrorCol("error")
        .setConcurrency(1)
        .transform(prepared)
        .head()

      val rowError = Option(result.getAs[Row]("error")).map(_.getAs[String]("response"))
      assert(rowError.isEmpty, rowError.getOrElse(""))
      assert(bodies.size() == 1)
      assert(attachmentRequests.asScala.toSeq == Seq("/image"))

      val payload = bodies.asScala.head.parseJson.asJsObject
      val JsArray(messages) = payload.fields("input")
      val JsArray(parts) = messages(1).asJsObject.fields("content")
      val JsString(text) = parts.head.asJsObject.fields("text")
      assert(text.contains("\"master\":\"Ace\""))
      assert(parts(1).asJsObject.fields("type") == JsString("input_image"))
      val JsString(encodedImage) = parts(1).asJsObject.fields("image_url")
      assert(encodedImage == dataImage)
    }
  }

  test("Responses OpenAIPrompt downloads extensionless JSON and XML URLs as text") {
    withEchoServer { (baseUrl, bodies, attachmentRequests) =>
      val cases = Seq(
        "/document-json" -> "{}",
        "/document-xml" -> "<?xml version=\"1.0\"?><root/>"
      )

      cases.foreach { case (path, expectedText) =>
        val attachmentUrl = baseUrl.replace("/openai/v1", path)
        val result = prompt(baseUrl, "responses")
          .transform(Seq(("Read the attachment.", attachmentUrl)).toDF("prompt", "image"))
          .head()

        val rowError = Option(result.getAs[Row]("error")).map(_.getAs[String]("response"))
        assert(rowError.isEmpty, rowError.getOrElse(""))

        val payload = bodies.asScala.last.parseJson.asJsObject
        val JsArray(messages) = payload.fields("input")
        val JsArray(parts) = messages(1).asJsObject.fields("content")
        assert(parts(1).asJsObject == JsObject(
          "type" -> JsString("input_text"),
          "text" -> JsString(expectedText)
        ))
      }

      assert(attachmentRequests.asScala.toSeq == cases.map(_._1))
    }
  }

  test("structured MIME fallback recognizes XML declarations") {
    val xml = "<?xml version=\"1.0\"?><root/>".getBytes(StandardCharsets.UTF_8)
    assert(OpenAIAttachmentUtils.inferStructuredTextMimeType(xml).contains("application/xml"))
  }

  test("Responses OpenAIPrompt accepts data image URLs") {
    val (result, payload) = requestPayload(_ => unpaddedDataImage, "responses")

    assert(Option(result.getAs[Row]("error")).isEmpty)
    val JsArray(messages) = payload.fields("input")
    val JsArray(parts) = messages(1).asJsObject.fields("content")
    assert(parts(1).asJsObject.fields("image_url") == JsString(unpaddedDataImage))
  }

  test("Responses OpenAIPrompt sends data files as input_file parts") {
    val (result, payload) = requestPayload(_ => dataPdf, "responses")

    assert(Option(result.getAs[Row]("error")).isEmpty)
    val JsArray(messages) = payload.fields("input")
    val JsArray(parts) = messages(1).asJsObject.fields("content")
    assert(parts(1).asJsObject == JsObject(
      "type" -> JsString("input_file"),
      "filename" -> JsString("attachment.pdf"),
      "file_data" -> JsString(dataPdf)
    ))
  }

  test("attachment filenames and extensions are locale independent") {
    val originalLocale = Locale.getDefault
    val tempFile = Files.createTempFile("synapseml-openai", ".GIF")
    try {
      Files.write(tempFile, imageBytes)
      Locale.setDefault(Locale.forLanguageTag("tr-TR"))

      assert(OpenAIAttachmentUtils.attachmentFilename("data:IMAGE/GIF;base64,") == "attachment.gif")
      val (_, _, fileType, mimeType) = OpenAIAttachmentUtils.prepareFile(
        tempFile.toString,
        None,
        imageExtensions = Set("gif"),
        audioExtensions = Set.empty,
        textExtensions = Set.empty)
      assert(fileType == "image")
      assert(mimeType == "image/png")
    } finally {
      Locale.setDefault(originalLocale)
      Files.deleteIfExists(tempFile)
    }
  }

  test("extensionless images use their inferred MIME type") {
    val tempFile = Files.createTempFile("synapseml-openai-extensionless", "")
    try {
      Files.write(tempFile, imageBytes)
      val (_, _, fileType, mimeType) = OpenAIAttachmentUtils.prepareFile(
        tempFile.toString,
        None,
        imageExtensions = Set("png"),
        audioExtensions = Set.empty,
        textExtensions = Set.empty)
      assert(fileType == "image")
      assert(mimeType == "image/png")
    } finally {
      Files.deleteIfExists(tempFile)
    }
  }

  test("unsupported Chat attachments report semantic and MIME types") {
    val error = intercept[IllegalArgumentException] {
      new OpenAIPrompt()
        .setApiType("chat_completions")
        .createMessagesForRow("Read the attachment.", Map("file" -> dataPdf), Seq("file"))
    }

    assert(error.getMessage ==
      "File type 'file' with MIME type 'application/pdf' is not supported for Chat Completions. " +
        "Only text and image attachments are supported; use apiType='responses' for other file inputs.")
  }

  test("Chat Completions OpenAIPrompt sends image_url content parts") {
    val (result, payload) = requestPayload(
      _ => unpaddedDataImage,
      "chat_completions"
    )

    assert(Option(result.getAs[Row]("error")).isEmpty)
    val JsArray(messages) = payload.fields("messages")
    val JsArray(parts) = messages(1).asJsObject.fields("content")
    assert(parts.map(_.asJsObject.fields("type")) == Seq(JsString("text"), JsString("image_url")))
    val JsString(imageUrl) = parts(1).asJsObject.fields("image_url").asJsObject.fields("url")
    assert(imageUrl == unpaddedDataImage)
  }

  test("base64 JSON data URLs are decoded as text attachments") {
    val messages = new OpenAIPrompt()
      .setApiType("responses")
      .createMessagesForRow("Read the attachment.", Map("file" -> dataJson), Seq("file"))

    assert(messages(1).content.map(_("type")) == Seq("input_text", "input_text"))
    assert(messages(1).content(1)("text") == "{}")
  }

  test("invalid and null data image URLs stay row-local and skip HTTP") {
    withEchoServer { (baseUrl, bodies) =>
      val rows = prompt(baseUrl, "responses")
        .transform(Seq(
          ("missing-base64-marker", Some("data:image/png,not-base64")),
          ("invalid-base64", Some("data:image/png;base64,%%%")),
          ("null", Option.empty[String])
        ).toDF("prompt", "image"))
        .collect()
        .map(row => row.getAs[String]("prompt") -> row)
        .toMap

      assert(bodies.isEmpty)
      assert(Option(rows("missing-base64-marker").getAs[Row]("output")).isEmpty)
      assert(rows("missing-base64-marker").getAs[Row]("error").getAs[String]("response") ==
        "Only base64-encoded data URLs are supported for path inputs")
      assert(Option(rows("invalid-base64").getAs[Row]("output")).isEmpty)
      assert(rows("invalid-base64").getAs[Row]("error").getAs[String]("response") ==
        "Data URL contains invalid base64 content")
      assert(Option(rows("null").getAs[Row]("output")).isEmpty)
      assert(Option(rows("null").getAs[Row]("error")).isEmpty)
    }
  }

  test("Java column type setters use the same validation as Scala") {
    val valid = new java.util.HashMap[String, String]()
    valid.put("image", "path")
    assert(new OpenAIPrompt().setColumnTypes(valid).getColumnTypes == Map("image" -> "path"))

    val invalid = new java.util.HashMap[String, String]()
    invalid.put("image", "binary")
    val error = intercept[IllegalArgumentException] {
      new OpenAIPrompt().setColumnTypes(invalid)
    }
    assert(error.getMessage ==
      "requirement failed: Unsupported column type: binary. Supported types are 'text' and 'path'.")
  }

  test("attachment read errors are sanitized and stay row-local") {
    withEchoServer { (baseUrl, bodies) =>
      val missingUrl = baseUrl.replace("/openai/v1", "/missing.png?sig=very-secret")
      val result = prompt(baseUrl, "responses")
        .transform(Seq(("Describe.", missingUrl)).toDF("prompt", "image"))
        .head()

      assert(bodies.isEmpty)
      assert(Option(result.getAs[Row]("output")).isEmpty)
      val error = result.getAs[Row]("error").getAs[String]("response")
      assert(error == "Unable to read attachment 'missing.png'")
      assert(!error.contains("very-secret"))
      assert(!error.contains(missingUrl))
    }
  }
  test("attachment file reads propagate cancellation without converting ordinary IO failures") {
    withAttachmentFailureFileSystem { fs =>
      def read(name: String): Unit = {
        OpenAIAttachmentUtils.prepareFile(AttachmentReadFailures.Location.resolve(name).toString,
          None, Set("png"), Set.empty, Set.empty)
      }
      assert(!Thread.currentThread().isInterrupted)
      val interruptedNames = AttachmentReadFailures.InterruptedNames
      interruptedNames.foreach { name =>
        try {
          val error = intercept[InterruptedException](read(name))
          assert(Thread.currentThread().isInterrupted)
          assert(error.getMessage == "Attachment read interrupted" && error.getCause == null)
        } finally Thread.interrupted()
      }
      val ordinaryNames = "ordinary.png" +: AttachmentReadFailures.TimeoutNames
      ordinaryNames.foreach { name =>
        try {
          val error = intercept[OpenAIAttachmentUtils.AttachmentReadException](read(name))
          assert(!error.isInstanceOf[OpenAIAttachmentUtils.CancelledAttachmentReadException])
          assert(error.getMessage == s"Unable to read attachment '$name'" && error.getCause == null)
          assert(!Thread.currentThread().isInterrupted)
        } finally Thread.interrupted()
      }
      try {
        val cancelled = intercept[OpenAIAttachmentUtils.CancelledAttachmentReadException](read("cancelled.png"))
        assert(cancelled.getMessage == "Unable to read attachment 'cancelled.png'" && cancelled.getCause == null)
        assert(!Thread.currentThread().isInterrupted)
        assert(fs.opened.asScala.toSet == (interruptedNames ++ ordinaryNames :+ "cancelled.png").toSet)
      } finally Thread.interrupted()
    }
  }
  Seq(false, true).foreach { includeCauses =>
    test(s"attachment reads detect interruption in cyclic exception graphs (causes=$includeCauses)") {
      val (root, last) = AttachmentReadFailures.cyclicExceptionGraph(includeCauses)
      try {
        val ordinary = intercept[OpenAIAttachmentUtils.AttachmentReadException] {
          OpenAIAttachmentUtils.readAttachment("image.png")(throw root)
        }
        assert(ordinary.getMessage == "Unable to read attachment 'image.png'" && ordinary.getCause == null)
        assert(!Thread.currentThread().isInterrupted)
        val interruption = AttachmentReadFailures.cyclicInterruption()
        if (includeCauses) last.initCause(interruption) else last.addSuppressed(interruption)
        val error = intercept[InterruptedException] {
          OpenAIAttachmentUtils.readAttachment("image.png")(throw root)
        }
        assert(Thread.currentThread().isInterrupted)
        assert(error.getMessage == "Attachment read interrupted" && error.getCause == null)
      } finally Thread.interrupted()
    }
  }

  test("fatal attachment failures are not replaced by nested interruption") {
    val fatal = new LinkageError("Synthetic fatal failure")
    fatal.addSuppressed(new InterruptedIOException("Synthetic private interruption"))
    fatal.initCause(new InterruptedException("Synthetic private cause"))
    try {
      assert(intercept[LinkageError] {
        OpenAIAttachmentUtils.readAttachment("image.png")(throw fatal)
      } eq fatal)
      assert(!Thread.currentThread().isInterrupted)
    } finally Thread.interrupted()
  }

  test("attachment reads check interruption before IO and after successful IO") {
    Seq(false, true).foreach { interruptBefore =>
      var read = false
      assert(!Thread.currentThread().isInterrupted)
      try {
        if (interruptBefore) Thread.currentThread().interrupt()
        val error = intercept[InterruptedException] {
          OpenAIAttachmentUtils.readAttachment("image.png") {
            read = true
            Thread.currentThread().interrupt()
            imageBytes
          }
        }
        assert(read != interruptBefore)
        assert(Thread.currentThread().isInterrupted)
        assert(error.getMessage == "Attachment read interrupted" && error.getCause == null)
      } finally Thread.interrupted()
    }
  }

  for {
    api <- Seq("chat_completions", "responses")
    (interrupt, successfulRead, beforeRead) <- Seq(
      (false, false, false), (false, true, false), (true, false, false), (false, false, true))
  } {
    val timing = if (beforeRead) " before IO" else ""
    test(s"legacy $api attachment task cancellation escapes the UDF " +
      s"(interrupt=$interrupt, read=$successfulRead)$timing") {
      val context = spark.sparkContext
      val group = UUID.randomUUID().toString
      val gate = new AttachmentReadFailures.Gate(if (successfulRead) Some(imageBytes) else None)
      AttachmentReadFailures.Active.put(group, gate)
      try withAttachmentFailureFileSystem { fs =>
        withEchoServer { (baseUrl, bodies) =>
          val data = if (beforeRead) spark.range(0L, 1L, 1L, 1).mapPartitions { _ =>
            AttachmentReadFailures.pathAfterCancellation(group)
          }.toDF("prompt", "image") else spark.range(1).select(lit("Describe this image.").as("prompt"),
            lit(AttachmentReadFailures.Location.resolve(group).toString).as("image"))
          val result = prompt(baseUrl, api).transform(data)
          val executor = Executors.newSingleThreadExecutor()
          Using.resource(new AutoCloseable {
            override def close(): Unit = {
              gate.release.countDown()
              try context.cancelJobGroup(group) finally {
                executor.shutdownNow()
                assert(executor.awaitTermination(15, TimeUnit.SECONDS), "Attachment job did not terminate")
              }
            }
          }) { _ =>
            val job = executor.submit(new Callable[Unit] {
              override def call(): Unit = {
                context.setJobGroup(group, "Legacy attachment task cancellation", interruptOnCancel = interrupt)
                try result.collect() finally context.clearJobGroup()
              }
            })
            assert(gate.entered.await(30, TimeUnit.SECONDS))
            context.cancelJobGroup(group)
            eventually(timeout(Span(10, Seconds))) {
              assert(gate.context.get().isInterrupted())
            }
            if (!interrupt) gate.release.countDown()
            assert(gate.completed.await(10, TimeUnit.SECONDS))
            intercept[ExecutionException](job.get(10, TimeUnit.SECONDS))
            val causes = Iterator.iterate(gate.taskFailure.get())(_.getCause).takeWhile(_ != null).toVector
            if (interrupt) {
              assert(causes.exists {
                case error: InterruptedException => error.getMessage == "Attachment read interrupted"
                case _ => false
              })
            } else {
              assert(!gate.completionInterrupted.get())
              val reason = TaskContextUtils.getKillReason(gate.context.get())
              assert(reason.nonEmpty && causes.exists {
                case error: TaskKilledException => reason.contains(error.reason)
                case _ => false
              })
            }
            if (beforeRead) assert(fs.opened.isEmpty, "Pre-cancelled task entered attachment IO")
            assert(bodies.isEmpty)
          }
        }
      } finally AttachmentReadFailures.Active.remove(group)
    }
  }

  for {
    api <- Seq("chat_completions", "responses")
    name <- "cancelled.png" +: AttachmentReadFailures.TimeoutNames
  } {
    test(s"legacy $api storage failure $name without task cancellation stays sanitized and row-local") {
      val location = AttachmentReadFailures.Location
      withAttachmentFailureFileSystem { fs =>
        withEchoServer { (baseUrl, bodies) =>
          val result = prompt(baseUrl, api)
            .transform(Seq(("Describe.", location.resolve(name).toString)).toDF("prompt", "image"))
            .head()
          assert(bodies.isEmpty)
          assert(Option(result.getAs[Row]("output")).isEmpty)
          assert(result.getAs[Row]("error").getAs[String]("response") == s"Unable to read attachment '$name'")
          assert(fs.opened.asScala.toVector == Vector(name))
        }
      }
    }
  }

  for {
    api <- Seq("chat_completions", "responses")
    name <- AttachmentReadFailures.ClearedInterruptNames
  } {
    test(s"legacy $api propagates interrupted IO from $name when storage clears the thread flag") {
      withAttachmentFailureFileSystem { fs =>
        assert(!Thread.currentThread().isInterrupted)
        try {
          val error = intercept[InterruptedException] {
            new OpenAIPrompt().setApiType(api).createMessagesForRow("Describe.",
              Map("file" -> AttachmentReadFailures.Location.resolve(name).toString), Seq("file"))
          }
          assert(Thread.currentThread().isInterrupted)
          assert(error.getMessage == "Attachment read interrupted" && error.getCause == null)
          assert(fs.opened.asScala.toVector == Vector(name))
        } finally Thread.interrupted()
      }
    }
  }

  test("OpenAIPrompt preserves existing row errors over attachment failures") {
    assert(!spark.conf.get("spark.sql.caseSensitive").toBoolean)
    val existingError = Row("upstream error", null) // scalastyle:ignore null
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("Describe.", "data:image/png,not-base64", existingError)
      ), 1),
      StructType(Seq(
        StructField("prompt", StringType, nullable = false),
        StructField("image", StringType, nullable = true),
        StructField("UPSTREAM_ERROR", ErrorUtils.ErrorSchema, nullable = true)
      ))
    )

    withEchoServer { (baseUrl, bodies) =>
      val result = prompt(baseUrl, "responses")
        .setErrorCol("upstream_error")
        .transform(input)
        .head()

      assert(bodies.isEmpty)
      assert(Option(result.getAs[Row]("output")).isEmpty)
      assert(result.getAs[Row]("upstream_error").getAs[String]("response") == "upstream error")
    }
  }

  test("OpenAIPrompt replaces non-error upstream columns with attachment errors") {
    assert(!spark.conf.get("spark.sql.caseSensitive").toBoolean)
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("Describe.", "data:image/png,not-base64", "not an error struct")
      ), 1),
      StructType(Seq(
        StructField("prompt", StringType, nullable = false),
        StructField("image", StringType, nullable = true),
        StructField("UPSTREAM_ERROR", StringType, nullable = true)
      ))
    )

    withEchoServer { (baseUrl, bodies) =>
      val result = prompt(baseUrl, "responses")
        .setErrorCol("upstream_error")
        .transform(input)
        .head()

      assert(bodies.isEmpty)
      assert(result.schema("upstream_error").dataType == ErrorUtils.ErrorSchema)
      assert(result.getAs[Row]("upstream_error").getAs[String]("response") ==
        "Only base64-encoded data URLs are supported for path inputs")
      assert(Option(result.getAs[Row]("output")).isEmpty)
    }
  }

  test("data image URLs honor the configured file size limit without leaking content") {
    withEchoServer { (baseUrl, bodies) =>
      val result = prompt(baseUrl, "responses")
        .setFileSizeLimitMB(0.000001)
        .transform(Seq(("Describe.", dataImage)).toDF("prompt", "image"))
        .head()

      assert(bodies.isEmpty)
      assert(Option(result.getAs[Row]("output")).isEmpty)
      val error = result.getAs[Row]("error").getAs[String]("response")
      assert(error.startsWith("Data URL attachment size "))
      assert(!error.contains("base64"))
    }
  }

  test("OpenAIPrompt multimodal transformSchema matches runtime output") {
    val input = Seq(("Describe this image.", dataImage)).toDF("prompt", "image")

    withEchoServer { (baseUrl, _) =>
      val droppedMessages = prompt(baseUrl, "responses")
      val droppedOutput = droppedMessages.transform(input)
      assert(droppedMessages.transformSchema(input.schema) == droppedOutput.schema)
      assert(!droppedOutput.columns.contains(droppedMessages.getMessagesCol))

      val retainedMessages = prompt(baseUrl, "chat_completions").setDropPrompt(false)
      val retainedOutput = retainedMessages.transform(input)
      assert(retainedMessages.transformSchema(input.schema) == retainedOutput.schema)
      val ArrayType(messageType: StructType, _) =
        retainedOutput.schema(retainedMessages.getMessagesCol).dataType
      assert(messageType.fieldNames.sameElements(Array("role", "content", "name")))
    }
  }

  test("OpenAIPrompt transformSchema handles a resolver-matched messages column") {
    assert(!spark.conf.get("spark.sql.caseSensitive").toBoolean)
    val input = Seq(("Describe this image.", dataImage, "stale"))
      .toDF("prompt", "image", "MESSAGES")

    withEchoServer { (baseUrl, _) =>
      Seq("chat_completions", "responses").foreach { apiType =>
        Seq(true, false).foreach { dropPrompt =>
          val transformer = prompt(baseUrl, apiType)
            .setMessagesCol("messages")
            .setDropPrompt(dropPrompt)
          val output = transformer.transform(input)
          val outputSchema = transformer.transformSchema(input.schema)
          val messageFields = outputSchema.fields.filter(
            _.name.equalsIgnoreCase(transformer.getMessagesCol))
          val runtimeMessageFields = output.schema.fields.filter(
            _.name.equalsIgnoreCase(transformer.getMessagesCol))
          val expectedMessageFields = if (dropPrompt) 0 else 1

          assert(outputSchema == output.schema)
          assert(messageFields.length == expectedMessageFields)
          assert(runtimeMessageFields.length == expectedMessageFields)
        }
      }
    }
  }

  test("OpenAIPrompt scratch columns preserve resolver-colliding input columns") {
    assert(!spark.conf.get("spark.sql.caseSensitive").toBoolean)

    withEchoServer { (baseUrl, _) =>
      val transformer = prompt(baseUrl, "responses")
      val fileResultPrefix = s"${transformer.uid}_file_result"
      val fileResultCol = fileResultPrefix.toUpperCase(Locale.ROOT)
      val fileResultSuffixCol = s"${fileResultPrefix}_1".toUpperCase(Locale.ROOT)
      val input = Seq((
        "Describe this image.",
        dataImage,
        "keep filename",
        "keep filename suffix",
        "keep result",
        "keep result suffix"
      )).toDF(
        "prompt",
        "image",
        "IMAGE_FILENAME",
        "IMAGE_FILENAME_1",
        fileResultCol,
        fileResultSuffixCol
      )

      val result = transformer.transform(input).head()

      assert(result.getAs[String]("IMAGE_FILENAME") == "keep filename")
      assert(result.getAs[String]("IMAGE_FILENAME_1") == "keep filename suffix")
      assert(result.getAs[String](fileResultCol) == "keep result")
      assert(result.getAs[String](fileResultSuffixCol) == "keep result suffix")
    }
  }

  test("OpenAIPrompt public column collisions are rejected") {
    assert(!spark.conf.get("spark.sql.caseSensitive").toBoolean)
    val input = Seq(("Describe this image.", dataImage)).toDF("prompt", "image")
    Seq(
      ("messagesCol 'collision' must be different from outputCol 'COLLISION'",
        prompt("https://example.services.ai.azure.com/openai/v1", "responses")
          .setMessagesCol("collision")
          .setOutputCol("COLLISION")),
      ("messagesCol 'collision' must be different from errorCol 'CoLlIsIoN'",
        prompt("https://example.services.ai.azure.com/openai/v1", "responses")
          .setMessagesCol("collision")
          .setErrorCol("CoLlIsIoN")),
      ("outputCol 'result' must be different from errorCol 'RESULT'",
        prompt("https://example.services.ai.azure.com/openai/v1", "responses")
          .setOutputCol("result")
          .setErrorCol("RESULT"))
    ).foreach { case (expectedMessage, transformer) =>
      val schemaError = intercept[IllegalArgumentException] {
        transformer.transformSchema(input.schema)
      }
      assert(schemaError.getMessage.contains(expectedMessage))

      val transformError = intercept[IllegalArgumentException] {
        transformer.transform(input)
      }
      assert(transformError.getMessage.contains(expectedMessage))
    }
  }

  test("multimodal prompt configuration survives copy and persistence") {
    val original = new OpenAIPrompt()
      .setApiType("responses")
      .setPromptTemplate("{prompt}")
      .setColumnTypes(Map("image" -> "path"))
      .setFileSizeLimitMB(5.0)

    val copied = original.copy(ParamMap.empty).asInstanceOf[OpenAIPrompt]
    assert(copied.getApiType == "responses")
    assert(copied.getColumnTypes == Map("image" -> "path"))
    assert(copied.getFileSizeLimitMB == 5.0)

    val persistenceDir = new File("cognitive/target/openai-prompt-multimodal-persistence")
    FileUtils.deleteDirectory(persistenceDir)
    try {
      original.write.session(spark).save(persistenceDir.getAbsolutePath)
      val loaded = OpenAIPrompt.read.session(spark).load(persistenceDir.getAbsolutePath)
      assert(loaded.getApiType == "responses")
      assert(loaded.getColumnTypes == Map("image" -> "path"))
      assert(loaded.getFileSizeLimitMB == 5.0)
    } finally {
      FileUtils.deleteDirectory(persistenceDir)
    }
  }
}
