// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.ErrorUtils
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import spray.json._

import java.io.File
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.Locale
import scala.collection.JavaConverters._

class OpenAIPromptMultimodalRequestSuite extends TestBase {

  import spark.implicits._

  private val dataImage =
    "data:image/png;base64," +
      "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y9Zl1sAAAAASUVORK5CYII="
  private val dataJson = "data:application/json;base64,e30="
  private val dataPdf = "data:application/pdf;base64,JVBERi0xLjQK"
  private val imageBytes = java.util.Base64.getDecoder.decode(dataImage.split(",", 2)(1))

  private def responseFor(path: String): String = {
    if (path.endsWith("/responses")) {
      JsObject(
        "id" -> JsString("resp_test"),
        "object" -> JsString("response"),
        "created_at" -> JsString("1"),
        "model" -> JsString("gpt-5.1"),
        "output" -> JsArray(JsObject(
          "content" -> JsArray(JsObject(
            "type" -> JsString("output_text"),
            "text" -> JsString("ok")
          )),
          "status" -> JsString("completed")
        )),
        "system_fingerprint" -> JsNull,
        "usage" -> JsNull
      ).compactPrint
    } else {
      JsObject(
        "id" -> JsString("chatcmpl_test"),
        "object" -> JsString("chat.completion"),
        "created" -> JsString("1"),
        "model" -> JsString("gpt-5.1"),
        "choices" -> JsArray(JsObject(
          "message" -> JsObject(
            "role" -> JsString("assistant"),
            "content" -> JsString("ok"),
            "name" -> JsNull
          ),
          "index" -> JsNumber(0),
          "finish_reason" -> JsString("stop")
        )),
        "system_fingerprint" -> JsNull,
        "usage" -> JsNull
      ).compactPrint
    }
  }

  private def withEchoServer(
      testCode: (String, ConcurrentLinkedQueue[String], ConcurrentLinkedQueue[String]) => Unit
  ): Unit = {
    val bodies = new ConcurrentLinkedQueue[String]()
    val attachmentRequests = new ConcurrentLinkedQueue[String]()
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/openai/v1", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        val body = new String(IOUtils.toByteArray(exchange.getRequestBody), StandardCharsets.UTF_8)
        bodies.add(body)
        val response = responseFor(exchange.getRequestURI.getPath).getBytes(StandardCharsets.UTF_8)
        exchange.getResponseHeaders.add("Content-Type", "application/json")
        exchange.sendResponseHeaders(200, response.length)
        exchange.getResponseBody.write(response)
        exchange.close()
      }
    })
    def addAttachmentContext(path: String, contentType: String, content: Array[Byte]): Unit = {
      server.createContext(path, new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          attachmentRequests.add(exchange.getRequestURI.getPath)
          exchange.getResponseHeaders.add("Content-Type", contentType)
          exchange.sendResponseHeaders(200, content.length)
          exchange.getResponseBody.write(content)
          exchange.close()
        }
      })
    }
    addAttachmentContext("/image.png", "image/png", imageBytes)
    addAttachmentContext("/image", "image/png", imageBytes)
    addAttachmentContext("/document-json", "application/json", "{}".getBytes(StandardCharsets.UTF_8))
    addAttachmentContext("/document-xml", "application/xml", "<root/>".getBytes(StandardCharsets.UTF_8))
    server.start()
    try {
      testCode(
        s"http://127.0.0.1:${server.getAddress.getPort}/openai/v1",
        bodies,
        attachmentRequests
      )
    } finally {
      server.stop(0)
    }
  }

  private def withEchoServer(testCode: (String, ConcurrentLinkedQueue[String]) => Unit): Unit = {
    withEchoServer { (baseUrl, bodies, _) =>
      testCode(baseUrl, bodies)
    }
  }

  private def prompt(baseUrl: String, apiType: String): OpenAIPrompt = {
    new OpenAIPrompt()
      .setUrl(baseUrl)
      .setApiType(apiType)
      .setDeploymentName("gpt-5.1")
      .setSubscriptionKey("unused")
      .setPromptTemplate("{prompt}")
      .setColumnTypes(Map("image" -> "path"))
      .setOutputCol("output")
      .setErrorCol("error")
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
        .setDeploymentName("gpt-5.1")
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
        "/document-xml" -> "<root/>"
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

  test("Responses OpenAIPrompt accepts data image URLs") {
    val (result, payload) = requestPayload(_ => dataImage, "responses")

    assert(Option(result.getAs[Row]("error")).isEmpty)
    val JsArray(messages) = payload.fields("input")
    val JsArray(parts) = messages(1).asJsObject.fields("content")
    assert(parts(1).asJsObject.fields("image_url") == JsString(dataImage))
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
      _.replace("/openai/v1", "/image.png"),
      "chat_completions"
    )

    assert(Option(result.getAs[Row]("error")).isEmpty)
    val JsArray(messages) = payload.fields("messages")
    val JsArray(parts) = messages(1).asJsObject.fields("content")
    assert(parts.map(_.asJsObject.fields("type")) == Seq(JsString("text"), JsString("image_url")))
    val JsString(imageUrl) = parts(1).asJsObject.fields("image_url").asJsObject.fields("url")
    assert(imageUrl == dataImage)
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
