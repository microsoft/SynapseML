// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.HTTPRequestData
import com.microsoft.azure.synapse.ml.services.HasCognitiveServiceInput
import com.microsoft.azure.synapse.ml.services.aifoundry.AIFoundryChatCompletion
import org.apache.http.entity.AbstractHttpEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructField, StructType}
import org.apache.spark.util.LongAccumulator
import spray.json._

class OpenAIV1EndpointSuite extends TestBase {

  import spark.implicits._

  private val prepareUrl = classOf[HasCognitiveServiceInput].getDeclaredMethod("prepareUrl")
  prepareUrl.setAccessible(true)

  private val prepareEntity = classOf[HasCognitiveServiceInput].getDeclaredMethod("prepareEntity")
  prepareEntity.setAccessible(true)

  private def requestUrl(transformer: HasCognitiveServiceInput, row: Row): String =
    prepareUrl.invoke(transformer).asInstanceOf[Row => String].apply(row)

  private def requestPayload(transformer: HasCognitiveServiceInput, row: Row): JsObject = {
    val entityBuilder = prepareEntity.invoke(transformer).asInstanceOf[Row => Option[AbstractHttpEntity]]
    EntityUtils.toString(entityBuilder.apply(row).get).parseJson.asJsObject
  }

  private val stringMessageSchema = StructType(Seq(
    StructField("role", StringType, nullable = false),
    StructField("content", StringType, nullable = true),
    StructField("name", StringType, nullable = true)
  ))

  private val nullableStringRoleMessageSchema = StructType(Seq(
    StructField("role", StringType, nullable = true),
    StructField("content", StringType, nullable = true),
    StructField("name", StringType, nullable = true)
  ))

  private val integerRoleMessageSchema = StructType(Seq(
    StructField("role", IntegerType, nullable = false),
    StructField("content", StringType, nullable = true),
    StructField("name", StringType, nullable = true)
  ))

  private val missingRoleMessageSchema = StructType(Seq(
    StructField("content", StringType, nullable = true),
    StructField("name", StringType, nullable = true)
  ))

  private val compositeMessageSchema = StructType(Seq(
    StructField("role", StringType, nullable = false),
    StructField(
      "content",
      ArrayType(
        MapType(StringType, StringType, valueContainsNull = true),
        containsNull = false
      ),
      nullable = true
    ),
    StructField("name", StringType, nullable = true)
  ))

  private val imageUrlSchema = StructType(Seq(
    StructField("url", StringType, nullable = true),
    StructField("detail", StringType, nullable = true)
  ))

  private val structuredContentPartSchema = StructType(Seq(
    StructField("type", StringType, nullable = false),
    StructField("text", StringType, nullable = true),
    StructField("image_url", imageUrlSchema, nullable = true)
  ))

  private val structuredMessageSchema = StructType(Seq(
    StructField("role", StringType, nullable = false),
    StructField(
      "content",
      ArrayType(structuredContentPartSchema, containsNull = false),
      nullable = false
    ),
    StructField("name", StringType, nullable = true)
  ))

  private val nullableStructuredRoleMessageSchema = StructType(Seq(
    StructField("role", StringType, nullable = true),
    StructField(
      "content",
      ArrayType(structuredContentPartSchema, containsNull = false),
      nullable = false
    ),
    StructField("name", StringType, nullable = true)
  ))

  private val primitiveArrayMessageSchema = StructType(Seq(
    StructField("role", StringType, nullable = false),
    StructField("content", ArrayType(StringType, containsNull = false), nullable = false)
  ))

  private def messagesRequestSchema(messageSchema: StructType): StructType = StructType(Seq(
    StructField("messages", ArrayType(messageSchema, containsNull = true), nullable = true)
  ))

  private def messageRow(role: String, content: String): Row =
    new GenericRowWithSchema(Array[Any](role, content, null), stringMessageSchema) // scalastyle:ignore null

  private def messageRowWithSchema(role: Any, content: String, schema: StructType): Row =
    new GenericRowWithSchema(Array[Any](role, content, null), schema) // scalastyle:ignore null

  private def compositeMessageRow(role: String, parts: Seq[Map[String, String]]): Row =
    new GenericRowWithSchema(Array[Any](role, parts, null), compositeMessageSchema) // scalastyle:ignore null

  private def imageUrlRow(url: Option[String], detail: Option[String] = None): Row =
    new GenericRowWithSchema(Array[Any](url.orNull, detail.orNull), imageUrlSchema)

  private def structuredContentPartRow(
    partType: String,
    text: Option[String] = None,
    imageUrl: Option[Row] = None
  ): Row = {
    new GenericRowWithSchema(
      Array[Any](partType, text.orNull, imageUrl.orNull),
      structuredContentPartSchema
    )
  }

  private def structuredMessageRow(role: String, parts: Seq[Row]): Row =
    new GenericRowWithSchema(Array[Any](role, parts, null), structuredMessageSchema) // scalastyle:ignore null

  private def structuredMessageRowWithSchema(role: Any, parts: Seq[Row], schema: StructType): Row =
    new GenericRowWithSchema(Array[Any](role, parts, null), schema) // scalastyle:ignore null

  private def primitiveArrayMessageRow(role: String, parts: Seq[String]): Row =
    new GenericRowWithSchema(Array[Any](role, parts), primitiveArrayMessageSchema)

  private def requestRow(messages: Seq[Row], messageSchema: StructType): Row =
    new GenericRowWithSchema(Array[Any](messages), messagesRequestSchema(messageSchema))

  private def messagesRow: Row = requestRow(Seq(messageRow("user", "hello")), stringMessageSchema)

  private def chatTransformer(): OpenAIChatCompletion = new OpenAIChatCompletion()
    .setUrl("https://example.services.ai.azure.com/openai/v1")
    .setDeploymentName("gpt-5.1")
    .setMessagesCol("messages")
    .setApiVersion("2025-04-01-preview")

  private def chatPayload(messages: Seq[Row], messageSchema: StructType): JsObject =
    requestPayload(chatTransformer(), requestRow(messages, messageSchema))

  private def chatSerializationError(messages: Seq[Row], messageSchema: StructType): IllegalArgumentException = {
    intercept[IllegalArgumentException] {
      chatPayload(messages, messageSchema)
    }
  }

  private def invalidRoleError(messageIndex: Int): String =
    s"messages[$messageIndex].role must be a non-empty string"

  private def transformWithThrowingHandler(
      rows: Seq[Row],
      messageSchema: StructType,
      outputCol: String = "output",
      errorCol: String = "error"
  ): (Array[Row], LongAccumulator) = {
    val handlerInvocations = spark.sparkContext.longAccumulator
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(rows, 1),
      messagesRequestSchema(messageSchema)
    )

    val chat = chatTransformer()
      .setSubscriptionKey("unused")
      .setOutputCol(outputCol)
      .setErrorCol(errorCol)
      .setHandler { (_: CloseableHttpClient, _: HTTPRequestData) =>
        handlerInvocations.add(1L)
        throw new AssertionError("HTTP handler should not be invoked for invalid message content")
      }

    val output = chat.transform(input).select("messages", outputCol, errorCol).collect()
    (output, handlerInvocations)
  }

  test("OpenAI URLs preserve configured base URL strings") {
    val root = new OpenAIChatCompletion().setUrl("https://example.openai.azure.com")
    assert(root.getUrl == "https://example.openai.azure.com")

    val v1 = new OpenAIChatCompletion().setUrl("https://example.openai.azure.com/openai/v1")
    assert(v1.getUrl == "https://example.openai.azure.com/openai/v1")

    val prompt = new OpenAIPrompt().setUrl("https://example.services.ai.azure.com")
    assert(prompt.getUrl == "https://example.services.ai.azure.com")

    val versionedPath = "https://synapseml-openai-3.openai.azure.com/openai/v2"
    OpenAIDefaults.setURL(versionedPath)
    try {
      assert(OpenAIDefaults.getURL.contains(versionedPath))
    } finally {
      OpenAIDefaults.resetURL()
    }

    OpenAIDefaults.setURL("https://example.services.ai.azure.com/openai/v1")
    try {
      val transformer = new OpenAIChatCompletion()
      transformer.transferGlobalParamsToParamMap()
      assert(transformer.getUrl == "https://example.services.ai.azure.com/openai/v1")
    } finally {
      OpenAIDefaults.resetURL()
    }
  }

  test("non-v1 versioned paths remain literal non-v1 base URLs") {
    val versionedPath = "https://synapseml-openai-3.openai.azure.com/openai/v2"
    OpenAIDefaults.setURL(versionedPath)
    try {
      val transformer = new OpenAIChatCompletion()
        .setDeploymentName("gpt-5.1")
        .setMessagesCol("messages")
      transformer.transferGlobalParamsToParamMap()

      assert(OpenAIDefaults.getURL.contains(versionedPath))
      assert(transformer.getUrl == versionedPath)
      assert(requestUrl(transformer, messagesRow) ==
        versionedPath + "/openai/deployments/gpt-5.1/chat/completions?api-version=2025-04-01-preview")
    } finally {
      OpenAIDefaults.resetURL()
    }
  }

  test("chat completions uses OpenAI v1 base URL without api-version and sends model") {
    val transformer = new OpenAIChatCompletion()
      .setUrl("https://example.services.ai.azure.com/openai/v1")
      .setDeploymentName("gpt-5.1")
      .setMessagesCol("messages")
      .setApiVersion("2025-04-01-preview")

    val row = messagesRow
    assert(requestUrl(transformer, row) == "https://example.services.ai.azure.com/openai/v1/chat/completions")

    val payload = requestPayload(transformer, row)
    assert(payload.fields.get("model").contains(JsString("gpt-5.1")))
    assert(payload.fields.contains("messages"))
  }

  test("chat completions accepts OpenAI-compatible v1 base URLs with and without trailing slash") {
    Seq(
      "https://example.openai.azure.com/openai/v1" ->
        "https://example.openai.azure.com/openai/v1/chat/completions",
      "https://example.openai.azure.com/openai/v1/" ->
        "https://example.openai.azure.com/openai/v1/chat/completions",
      "https://api.openai.com/v1" ->
        "https://api.openai.com/v1/chat/completions",
      "http://localhost:8000/v1/" ->
        "http://localhost:8000/v1/chat/completions"
    ).foreach { case (baseUrl, expectedUrl) =>
        val transformer = new OpenAIChatCompletion()
          .setUrl(baseUrl)
          .setDeploymentName("gpt-5.1")
          .setMessagesCol("messages")
          .setApiVersion("2025-04-01-preview")

        assert(requestUrl(transformer, messagesRow) == expectedUrl)
      }
  }

  test("chat completions keeps legacy Azure deployment URL and api-version with and without trailing slash") {
    Seq("https://example.openai.azure.com", "https://example.openai.azure.com/").foreach { baseUrl =>
      val transformer = new OpenAIChatCompletion()
        .setUrl(baseUrl)
        .setDeploymentName("gpt-5.1")
        .setMessagesCol("messages")
        .setApiVersion("2025-04-01-preview")

      val row = messagesRow
      assert(requestUrl(transformer, row) ==
        "https://example.openai.azure.com/openai/deployments/gpt-5.1/chat/completions" +
          "?api-version=2025-04-01-preview")
      assert(!requestPayload(transformer, row).fields.contains("model"))
    }
  }

  test("chat completions accepts services.ai.azure.com resource root with and without trailing slash") {
    Seq("https://example.services.ai.azure.com", "https://example.services.ai.azure.com/").foreach { baseUrl =>
      val transformer = new OpenAIChatCompletion()
        .setUrl(baseUrl)
        .setDeploymentName("gpt-5.1")
        .setMessagesCol("messages")
        .setApiVersion("2025-04-01-preview")

      assert(requestUrl(transformer, messagesRow) ==
        "https://example.services.ai.azure.com/openai/deployments/gpt-5.1/chat/completions" +
          "?api-version=2025-04-01-preview")
    }
  }

  test("AI Foundry chat accepts services.ai.azure.com resource root with and without trailing slash") {
    Seq("https://example.services.ai.azure.com", "https://example.services.ai.azure.com/").foreach { baseUrl =>
      val transformer = new AIFoundryChatCompletion()
        .setUrl(baseUrl)
        .setModel("gpt-5.1")
        .setMessagesCol("messages")
        .setApiVersion("2025-04-01-preview")

      assert(requestUrl(transformer, messagesRow) ==
        "https://example.services.ai.azure.com/models/chat/completions?api-version=2025-04-01-preview")
    }
  }

  test("non-v1 URL paths remain permissive and use legacy request construction") {
    val transformer = new OpenAIChatCompletion()
      .setUrl("https://example.openai.azure.com/openai")
      .setDeploymentName("gpt-5.1")
      .setMessagesCol("messages")
      .setApiVersion("2025-04-01-preview")

    assert(requestUrl(transformer, messagesRow) ==
      "https://example.openai.azure.com/openai/openai/deployments/gpt-5.1/chat/completions" +
        "?api-version=2025-04-01-preview")
  }

  test("custom non-Azure URL strings remain permissive") {
    val transformer = new OpenAIChatCompletion()
      .setUrl("https://proxy.contoso.com/openai")
      .setDeploymentName("gpt-5.1")
      .setMessagesCol("messages")
      .setApiVersion("2025-04-01-preview")

    assert(requestUrl(transformer, messagesRow) ==
      "https://proxy.contoso.com/openai/openai/deployments/gpt-5.1/chat/completions" +
        "?api-version=2025-04-01-preview")
  }

  test("OpenAI defaults allow non-v1 URL paths") {
    OpenAIDefaults.setURL("https://example.openai.azure.com/openai")
    try {
      val transformer = new OpenAIChatCompletion()
        .setDeploymentName("gpt-5.1")
        .setMessagesCol("messages")
      transformer.transferGlobalParamsToParamMap()

      assert(requestUrl(transformer, messagesRow) ==
        "https://example.openai.azure.com/openai/openai/deployments/gpt-5.1/chat/completions" +
          "?api-version=2025-04-01-preview")
    } finally {
      OpenAIDefaults.resetURL()
    }
  }

  test("OpenAI defaults allow arbitrary URL strings") {
    OpenAIDefaults.setURL("not-a-url")
    try {
      val transformer = new OpenAIChatCompletion()
      transformer.transferGlobalParamsToParamMap()
      assert(transformer.getUrl == "not-a-url")
    } finally {
      OpenAIDefaults.resetURL()
    }
  }

  test("OpenAI defaults accept v1 URL and omit global api-version") {
    OpenAIDefaults.setURL("https://example.openai.azure.com/openai/v1")
    OpenAIDefaults.setApiVersion("2025-04-01-preview")
    try {
      val transformer = new OpenAIChatCompletion()
        .setDeploymentName("gpt-5.1")
        .setMessagesCol("messages")
      transformer.transferGlobalParamsToParamMap()

      assert(requestUrl(transformer, messagesRow) == "https://example.openai.azure.com/openai/v1/chat/completions")
    } finally {
      OpenAIDefaults.resetURL()
      OpenAIDefaults.resetApiVersion()
    }
  }

  test("embeddings uses OpenAI v1 base URL and sends deployment as model") {
    Seq(
      "https://example.services.ai.azure.com/openai/v1" ->
        "https://example.services.ai.azure.com/openai/v1/embeddings",
      "https://example.services.ai.azure.com/openai/v1/" ->
        "https://example.services.ai.azure.com/openai/v1/embeddings",
      "https://api.openai.com/v1" ->
        "https://api.openai.com/v1/embeddings"
    ).foreach { case (baseUrl, expectedUrl) =>
        val transformer = new OpenAIEmbedding()
          .setUrl(baseUrl)
          .setDeploymentName("text-embedding-3-large")
          .setTextCol("text")
          .setApiVersion("2025-04-01-preview")

        val row = Seq("hello").toDF("text").collect().head
        assert(requestUrl(transformer, row) == expectedUrl)

        val payload = requestPayload(transformer, row)
        assert(payload.fields.get("model").contains(JsString("text-embedding-3-large")))
        assert(payload.fields.get("input").contains(JsString("hello")))
      }
  }

  test("embeddings keeps legacy Azure deployment URL and api-version") {
    val transformer = new OpenAIEmbedding()
      .setUrl("https://example.openai.azure.com/")
      .setDeploymentName("text-embedding-3-large")
      .setTextCol("text")
      .setApiVersion("2025-04-01-preview")

    val row = Seq("hello").toDF("text").collect().head
    assert(requestUrl(transformer, row) ==
      "https://example.openai.azure.com/openai/deployments/text-embedding-3-large/embeddings" +
        "?api-version=2025-04-01-preview")

    val payload = requestPayload(transformer, row)
    assert(!payload.fields.contains("model"))
    assert(payload.fields.get("input").contains(JsString("hello")))
  }

  test("chat completions preserve legacy string content in local v1 request JSON") {
    val payload = chatPayload(Seq(messageRow("user", "Describe the image")), stringMessageSchema)

    val expected =
      """{
        |  "model": "gpt-5.1",
        |  "messages": [{"role": "user", "content": "Describe the image"}]
        |}""".stripMargin.parseJson.asJsObject
    assert(payload == expected)
  }

  test("chat completions collapse legacy map-backed content parts into text") {
    val payload = chatPayload(
      Seq(compositeMessageRow("user", Seq(
        Map("type" -> "text", "text" -> "Line one"),
        Map("type" -> "input_file", "filename" -> "example.txt"),
        Map("type" -> "text", "text" -> "Line two")
      ))),
      compositeMessageSchema
    )

    val expected =
      """{
        |  "model": "gpt-5.1",
        |  "messages": [{"role": "user", "content": "Line one\nLine two"}]
        |}""".stripMargin.parseJson.asJsObject
    assert(payload == expected)
  }

  test("chat completions preserve nested image_url content") {
    val imagePart: Row = structuredContentPartRow(
      "image_url",
      imageUrl = Some(imageUrlRow(Some("https://example.com/triangle.png"), Some("low")))
    )

    val payload = chatPayload(Seq(structuredMessageRow("user", Seq(imagePart))), structuredMessageSchema)
    val JsArray(messages) = payload.fields("messages")
    val JsArray(contentParts) = messages.head.asJsObject.fields("content")
    val imageUrl = contentParts.head.asJsObject.fields("image_url").asJsObject

    assert(payload.fields.get("model").contains(JsString("gpt-5.1")))
    assert(imageUrl.fields.get("url").contains(JsString("https://example.com/triangle.png")))
    assert(imageUrl.fields.get("detail").contains(JsString("low")))
  }

  test("chat completions serialize exact mixed text and image_url request JSON") {
    val textPart: Row = structuredContentPartRow("text", text = Some("What is shown?"))
    val imagePart: Row = structuredContentPartRow(
      "image_url",
      imageUrl = Some(imageUrlRow(Some("data:image/png;base64,AAA")))
    )

    val payload = chatPayload(
      Seq(structuredMessageRow("user", Seq(textPart, imagePart))),
      structuredMessageSchema
    )

    val expected =
      """{
        |  "model": "gpt-5.1",
        |  "messages": [{"role": "user", "content": [
        |    {"type": "text", "text": "What is shown?"},
        |    {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAA"}}
        |  ]}]
        |}""".stripMargin.parseJson.asJsObject
    assert(payload == expected)
  }

  test("chat completions reject invalid roles during request serialization") {
    val textPart: Row = structuredContentPartRow("text", text = Some("Describe the image"))

    val nullRoleError = chatSerializationError(
      Seq(messageRowWithSchema(Option.empty[String].orNull, "hello", nullableStringRoleMessageSchema)),
      nullableStringRoleMessageSchema
    )
    assert(nullRoleError.getMessage == invalidRoleError(0))

    val blankRoleError = chatSerializationError(
      Seq(messageRowWithSchema("   ", "hello", nullableStringRoleMessageSchema)),
      nullableStringRoleMessageSchema
    )
    assert(blankRoleError.getMessage == invalidRoleError(0))

    val wrongSchemaRoleError = chatSerializationError(
      Seq(messageRowWithSchema(Int.box(7), "hello", integerRoleMessageSchema)),
      integerRoleMessageSchema
    )
    assert(wrongSchemaRoleError.getMessage == invalidRoleError(0))

    val wrongRuntimeRoleError = chatSerializationError(
      Seq(messageRowWithSchema(Int.box(7), "hello", stringMessageSchema)),
      stringMessageSchema
    )
    assert(wrongRuntimeRoleError.getMessage == invalidRoleError(0))

    val missingRoleError = chatSerializationError(
      Seq(new GenericRowWithSchema(Array[Any]("hello", Option.empty[String].orNull), missingRoleMessageSchema)),
      missingRoleMessageSchema
    )
    assert(missingRoleError.getMessage == invalidRoleError(0))

    val structuredNullRoleError = chatSerializationError(
      Seq(
        structuredMessageRowWithSchema(
          Option.empty[String].orNull,
          Seq(textPart),
          nullableStructuredRoleMessageSchema
        )
      ),
      nullableStructuredRoleMessageSchema
    )
    assert(structuredNullRoleError.getMessage == invalidRoleError(0))
  }

  test("chat completions reject invalid multimodal content shapes during request serialization") {
    val missingUrl: Row = structuredContentPartRow(
      "image_url",
      imageUrl = Some(imageUrlRow(None))
    )
    val missingUrlError = chatSerializationError(
      Seq(structuredMessageRow("user", Seq(missingUrl))),
      structuredMessageSchema
    )
    assert(missingUrlError.getMessage.contains("requires a non-empty string 'url' field"))

    val unsupportedPart: Row = structuredContentPartRow("input_file", text = Some("not supported"))
    val unsupportedError = chatSerializationError(
      Seq(structuredMessageRow("user", Seq(unsupportedPart))),
      structuredMessageSchema
    )
    assert(unsupportedError.getMessage.contains("unsupported type"))

    val primitiveError = chatSerializationError(
      Seq(primitiveArrayMessageRow("user", Seq("not an object"))),
      primitiveArrayMessageSchema
    )
    assert(primitiveError.getMessage.contains("Unsupported content part type"))

    val nullPart = Option.empty[Row].orNull
    val nullPartError = chatSerializationError(
      Seq(structuredMessageRow("user", Seq(nullPart))),
      structuredMessageSchema
    )
    assert(nullPartError.getMessage == "messages[0].content[0] must be an object")

    val nullMessageError = chatSerializationError(
      Seq[Row](Option.empty[Row].orNull),
      structuredMessageSchema
    )
    assert(nullMessageError.getMessage == "messages[0] must be an object")
  }

  test("chat completions route malformed structured content to errorCol without invoking HTTP") {
    val malformedPart: Row = structuredContentPartRow(
      "text",
      text = Some("Describe the image"),
      imageUrl = Some(imageUrlRow(Some("https://private.example/secret.png"), Some("low")))
    )
    val nullMessage = Option.empty[Row].orNull

    val (rows, handlerInvocations) = transformWithThrowingHandler(Seq(
      Row(Seq(structuredMessageRow("user", Seq(malformedPart)))),
      Row(Seq[Row](nullMessage)),
      Row(null) // scalastyle:ignore null
    ), structuredMessageSchema)

    assert(rows.length == 3)
    assert(handlerInvocations.value == 0L)

    val firstRow = rows(0)
    val firstError = firstRow.getAs[Row]("error")
    assert(Option(firstError).isDefined)
    val firstErrorResponse = firstError.getAs[String]("response")
    assert(firstErrorResponse == "messages[0].content[0] contains unsupported fields")
    assert(!firstErrorResponse.contains("Describe the image"))
    assert(!firstErrorResponse.contains("https://private.example/secret.png"))
    assert(Option(firstError.getAs[Row]("status")).isEmpty)
    assert(Option(firstRow.getAs[Row]("output")).isEmpty)
    val restoredFirstMessages = Option(firstRow.getAs[scala.collection.Seq[Row]]("messages"))
      .getOrElse(scala.collection.Seq.empty[Row])
    assert(restoredFirstMessages.size == 1)
    val restoredFirstParts = restoredFirstMessages.head.getAs[scala.collection.Seq[Row]]("content")
    assert(restoredFirstParts.size == 1)
    assert(restoredFirstParts.head.getAs[String]("text") == "Describe the image")
    assert(Option(restoredFirstParts.head.getAs[Row]("image_url"))
      .exists(_.getAs[String]("url") == "https://private.example/secret.png"))

    val secondRow = rows(1)
    val secondError = secondRow.getAs[Row]("error")
    assert(Option(secondError).isDefined)
    assert(secondError.getAs[String]("response") == "messages[0] must be an object")
    assert(Option(secondError.getAs[Row]("status")).isEmpty)
    assert(Option(secondRow.getAs[Row]("output")).isEmpty)
    val restoredSecondMessages = Option(secondRow.getAs[scala.collection.Seq[Row]]("messages"))
      .getOrElse(scala.collection.Seq.empty[Row])
    assert(restoredSecondMessages.size == 1)
    assert(restoredSecondMessages.headOption.flatMap(message => Option(message)).isEmpty)

    val thirdRow = rows(2)
    assert(Option(thirdRow.getAs[scala.collection.Seq[Row]]("messages")).isEmpty)
    assert(Option(thirdRow.getAs[Row]("error")).isEmpty)
    assert(Option(thirdRow.getAs[Row]("output")).isEmpty)
  }

  test("chat completions route invalid structured roles to errorCol without invoking HTTP") {
    val textPart: Row = structuredContentPartRow("text", text = Some("Describe the image"))

    val (rows, handlerInvocations) = transformWithThrowingHandler(Seq(
      Row(Seq(structuredMessageRowWithSchema("   ", Seq(textPart), nullableStructuredRoleMessageSchema)))
    ), nullableStructuredRoleMessageSchema)

    assert(rows.length == 1)
    assert(handlerInvocations.value == 0L)

    val onlyRow = rows.head
    val error = onlyRow.getAs[Row]("error")
    assert(Option(error).isDefined)
    assert(error.getAs[String]("response") == invalidRoleError(0))
    assert(Option(error.getAs[Row]("status")).isEmpty)
    assert(Option(onlyRow.getAs[Row]("output")).isEmpty)

    val restoredMessages = Option(onlyRow.getAs[scala.collection.Seq[Row]]("messages"))
      .getOrElse(scala.collection.Seq.empty[Row])
    assert(restoredMessages.size == 1)
    assert(restoredMessages.head.getAs[String]("role") == "   ")
    val restoredParts = restoredMessages.head.getAs[scala.collection.Seq[Row]]("content")
    assert(restoredParts.size == 1)
    assert(restoredParts.head.getAs[String]("text") == "Describe the image")
  }

  test("chat completions keep public output when it collides with originalMessages scratch column") {
    val textPart: Row = structuredContentPartRow("text", text = Some("Describe the image"))

    val (rows, handlerInvocations) = transformWithThrowingHandler(
      Seq(Row(Seq(
        structuredMessageRowWithSchema(
          Option.empty[String].orNull,
          Seq(textPart),
          nullableStructuredRoleMessageSchema
        )
      ))),
      nullableStructuredRoleMessageSchema,
      outputCol = "originalMessages"
    )

    assert(rows.length == 1)
    assert(handlerInvocations.value == 0L)

    val onlyRow = rows.head
    assert(onlyRow.schema.fieldNames.contains("originalMessages"))
    assert(Option(onlyRow.getAs[Row]("originalMessages")).isEmpty)
    val error = onlyRow.getAs[Row]("error")
    assert(Option(error).isDefined)
    assert(error.getAs[String]("response") == invalidRoleError(0))
    assert(Option(error.getAs[Row]("status")).isEmpty)

    val restoredMessages = Option(onlyRow.getAs[scala.collection.Seq[Row]]("messages"))
      .getOrElse(scala.collection.Seq.empty[Row])
    assert(restoredMessages.size == 1)
    assert(restoredMessages.head.isNullAt(restoredMessages.head.fieldIndex("role")))
    val restoredParts = restoredMessages.head.getAs[scala.collection.Seq[Row]]("content")
    assert(restoredParts.size == 1)
    assert(restoredParts.head.getAs[String]("text") == "Describe the image")
  }

  test("chat completions route primitive array content to errorCol without invoking HTTP") {
    val (rows, handlerInvocations) = transformWithThrowingHandler(Seq(
      Row(Seq(primitiveArrayMessageRow("user", Seq("not an object"))))
    ), primitiveArrayMessageSchema)

    assert(rows.length == 1)
    assert(handlerInvocations.value == 0L)

    val onlyRow = rows.head
    val error = onlyRow.getAs[Row]("error")
    assert(Option(error).isDefined)
    assert(error.getAs[String]("response") == "Unsupported content part type: string. Expected struct or map")
    assert(Option(error.getAs[Row]("status")).isEmpty)
    assert(Option(onlyRow.getAs[Row]("output")).isEmpty)
    val restoredMessages = Option(onlyRow.getAs[scala.collection.Seq[Row]]("messages"))
      .getOrElse(scala.collection.Seq.empty[Row])
    assert(restoredMessages.size == 1)
    assert(
      restoredMessages.head.getAs[scala.collection.Seq[String]]("content") ==
        scala.collection.Seq("not an object")
    )
  }

  test("responses uses OpenAI v1 base URL without api-version") {
    Seq(
      "https://example.services.ai.azure.com/openai/v1" ->
        "https://example.services.ai.azure.com/openai/v1/responses",
      "https://example.services.ai.azure.com/openai/v1/" ->
        "https://example.services.ai.azure.com/openai/v1/responses",
      "https://api.openai.com/v1" ->
        "https://api.openai.com/v1/responses"
    ).foreach { case (baseUrl, expectedUrl) =>
        val transformer = new OpenAIResponses()
          .setUrl(baseUrl)
          .setDeploymentName("gpt-5-mini")
          .setMessagesCol("messages")
          .setApiVersion("2025-04-01-preview")

        val row = messagesRow
        assert(requestUrl(transformer, row) == expectedUrl)

        val payload = requestPayload(transformer, row)
        assert(payload.fields.get("model").contains(JsString("gpt-5-mini")))
        assert(payload.fields.contains("input"))
      }
  }

  test("responses v1 endpoint requires deployment name as model") {
    val transformer = new OpenAIResponses()
      .setUrl("https://example.services.ai.azure.com/openai/v1")
      .setMessagesCol("messages")

    val err = intercept[IllegalArgumentException] {
      requestPayload(transformer, messagesRow)
    }
    assert(err.getMessage.contains("No deployment/model name provided for OpenAI v1 endpoint"))
  }

  test("responses keeps legacy Azure URL shape when URL is not an OpenAI v1 base") {
    val transformer = new OpenAIResponses()
      .setUrl("https://example.openai.azure.com/")
      .setDeploymentName("gpt-5-mini")
      .setMessagesCol("messages")
      .setApiVersion("2025-04-01-preview")

    assert(requestUrl(transformer, messagesRow) ==
      "https://example.openai.azure.com/openai/responses?api-version=2025-04-01-preview")
  }

  test("OpenAIPrompt treats services.ai.azure.com/openai/v1 as OpenAI v1, not models chat endpoint") {
    val prompt = new OpenAIPrompt()
      .setUrl("https://example.services.ai.azure.com/openai/v1")
      .setModel("gpt-5.1")
      .setMessagesCol("messages")

    val prepareEntity = classOf[OpenAIPrompt].getDeclaredMethod("prepareEntity")
    prepareEntity.setAccessible(true)
    val buildEntity = prepareEntity.invoke(prompt).asInstanceOf[Row => Option[AbstractHttpEntity]]

    val payload = EntityUtils.toString(buildEntity(messagesRow).get).parseJson.asJsObject
    assert(payload.fields.get("model").contains(JsString("gpt-5.1")))
    assert(payload.fields.contains("messages"))
  }
}
