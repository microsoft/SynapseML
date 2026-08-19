// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, MapType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import spray.json._
import spray.json.DefaultJsonProtocol._

class OpenAICoreOfflineSuite extends AnyFunSuite {

  private val stringMessageSchema = StructType(Seq(
    StructField("role", StringType, nullable = false),
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

  private val unsupportedContentSchema = StructType(Seq(
    StructField("role", StringType, nullable = false),
    StructField("content", IntegerType, nullable = false)
  ))

  private def messageRow(role: String, content: String): Row =
    new GenericRowWithSchema(Array[Any](role, content, ""), stringMessageSchema)

  private def compositeMessageRow(role: String, parts: Seq[Map[String, String]]): Row =
    new GenericRowWithSchema(Array[Any](role, parts, ""), compositeMessageSchema)

  private def unsupportedMessageRow(role: String, content: Int): Row =
    new GenericRowWithSchema(Array[Any](role, content), unsupportedContentSchema)

  // Struct-backed (Array(Struct(...))) content, as produced when a user supplies a typed schema.
  private val imagePartSchema = StructType(Seq(
    StructField("type", StringType, nullable = true),
    StructField("text", StringType, nullable = true),
    StructField("image_url", StringType, nullable = true)
  ))

  private val structContentSchema = StructType(Seq(
    StructField("role", StringType, nullable = false),
    StructField("content", ArrayType(imagePartSchema, containsNull = true), nullable = true),
    StructField("name", StringType, nullable = true)
  ))

  private def imagePartRow(partType: String, text: String, imageUrl: String): Row =
    new GenericRowWithSchema(Array[Any](partType, text, imageUrl), imagePartSchema)

  private def structMessageRow(role: String, parts: Seq[Row]): Row =
    new GenericRowWithSchema(Array[Any](role, parts, ""), structContentSchema)

  private def parseEntity(entity: StringEntity): JsObject =
    EntityUtils.toString(entity).parseJson.asJsObject

  test("encodeMessagesToMap supports text and composite message shapes") {
    val chat = new OpenAIChatCompletion()
    val compositeParts = Seq(
      Map("type" -> "text", "text" -> "first"),
      Map("type" -> "input_file", "filename" -> "example.txt")
    )

    val mapped = chat.encodeMessagesToMap(Seq(
      messageRow("user", "hello"),
      compositeMessageRow("assistant", compositeParts)
    ))

    assert(mapped.head("role") == "user")
    assert(mapped.head("content") == "hello")
    val secondContent = mapped(1)("content").asInstanceOf[Seq[Map[String, Any]]]
    assert(secondContent.head("type") == "text")
    assert(secondContent(1)("type") == "input_file")
  }

  test("encodeMessagesToMap rejects unsupported content types") {
    val chat = new OpenAIChatCompletion()
    val ex = intercept[IllegalArgumentException] {
      chat.encodeMessagesToMap(Seq(unsupportedMessageRow("user", 123)))
    }
    assert(ex.getMessage.contains("Unsupported content type"))
  }

  test("OpenAIChatCompletion getStringEntity collapses content parts into text") {
    val chat = new OpenAIChatCompletion()
    val messageParts = Seq(
      Map("type" -> "text", "text" -> "Line one"),
      Map("type" -> "input_file", "filename" -> "example.txt"),
      Map("type" -> "text", "text" -> "Line two")
    )

    val entity = chat.getStringEntity(
      Seq(compositeMessageRow("user", messageParts)),
      Map("temperature" -> 0.0)
    )

    val payload = parseEntity(entity)
    val JsArray(messages) = payload.fields("messages")
    val content = messages.head.asJsObject.fields("content").convertTo[String]

    assert(content == "Line one\nLine two")
  }

  test("OpenAIChatCompletion getStringEntity preserves map-backed image_url parts as nested wire objects") {
    val chat = new OpenAIChatCompletion()
    val messageParts = Seq(
      Map("type" -> "text", "text" -> "What is in this image?"),
      Map("type" -> "image_url", "image_url" -> "https://example.com/cat.png")
    )

    val entity = chat.getStringEntity(
      Seq(compositeMessageRow("user", messageParts)),
      Map("temperature" -> 0.0)
    )

    val payload = parseEntity(entity)
    val JsArray(messages) = payload.fields("messages")
    // Content must stay a JSON array (not collapse to text) so the image is not dropped (issue #2246).
    val JsArray(content) = messages.head.asJsObject.fields("content")
    assert(content.length == 2)

    val textPart = content.head.asJsObject
    assert(textPart.fields("type").convertTo[String] == "text")
    assert(textPart.fields("text").convertTo[String] == "What is in this image?")

    val imagePart = content(1).asJsObject
    assert(imagePart.fields("type").convertTo[String] == "image_url")
    // A flat image_url String is reshaped into the nested Chat wire object {"url": ...}.
    val imageUrl = imagePart.fields("image_url").asJsObject
    assert(imageUrl.fields("url").convertTo[String] == "https://example.com/cat.png")
  }

  test("OpenAIChatCompletion getStringEntity preserves struct-backed image content end to end") {
    val chat = new OpenAIChatCompletion()
    val parts = Seq(
      imagePartRow("text", "Describe this", null), //scalastyle:ignore null
      imagePartRow("image_url", null, "https://example.com/dog.png") //scalastyle:ignore null
    )

    val entity = chat.getStringEntity(
      Seq(structMessageRow("user", parts)),
      Map("temperature" -> 0.0)
    )

    val payload = parseEntity(entity)
    val JsArray(messages) = payload.fields("messages")
    val JsArray(content) = messages.head.asJsObject.fields("content")
    assert(content.length == 2)
    assert(content.head.asJsObject.fields("type").convertTo[String] == "text")

    val imagePart = content(1).asJsObject
    assert(imagePart.fields("type").convertTo[String] == "image_url")
    val imageUrl = imagePart.fields("image_url").asJsObject
    assert(imageUrl.fields("url").convertTo[String] == "https://example.com/dog.png")
  }

  test("encodeMessagesToMap converts struct-backed multimodal content without throwing") {
    val chat = new OpenAIChatCompletion()
    val parts = Seq(
      imagePartRow("text", "hello", null), //scalastyle:ignore null
      imagePartRow("image_url", null, "https://example.com/a.png") //scalastyle:ignore null
    )

    val mapped = chat.encodeMessagesToMap(Seq(structMessageRow("user", parts)))

    val content = mapped.head("content").asInstanceOf[Seq[Map[String, Any]]]
    assert(content.length == 2)
    assert(content.head("type") == "text")
    assert(content.head("text") == "hello")
    assert(content(1)("type") == "image_url")
    assert(content(1)("image_url") == "https://example.com/a.png")
    // Null struct fields are omitted (AnyJsonFormat cannot serialize null values).
    assert(!content.head.contains("image_url"))
    assert(!content(1).contains("text"))
  }

  test("encodeMessagesToMap tolerates short struct rows and null fields without crashing") {
    val chat = new OpenAIChatCompletion()
    val shortRow = new GenericRowWithSchema(
      Array[Any]("text"),
      StructType(Seq(StructField("type", StringType, nullable = true)))
    )
    val nullFieldRow = imagePartRow("text", null, null) //scalastyle:ignore null

    val mapped = chat.encodeMessagesToMap(Seq(structMessageRow("user", Seq(shortRow, nullFieldRow))))
    val content = mapped.head("content").asInstanceOf[Seq[Map[String, Any]]]

    assert(content.length == 2)
    assert(content.head("type") == "text")   // short (arity 1) row handled by the arity guard
    assert(content(1)("type") == "text")      // null fields omitted, no crash
    assert(content(1).keySet == Set("type"))
  }

  test("OpenAIChatCompletion getStringEntity keeps pure String content as a String") {
    val chat = new OpenAIChatCompletion()
    val entity = chat.getStringEntity(
      Seq(messageRow("user", "just text")),
      Map("temperature" -> 0.0)
    )
    val payload = parseEntity(entity)
    val JsArray(messages) = payload.fields("messages")
    assert(messages.head.asJsObject.fields("content").convertTo[String] == "just text")
  }

  test("encodeMessagesToMap tolerates null content arrays and heterogeneous part types") {
    val chat = new OpenAIChatCompletion()

    // A null content array must not throw and must round-trip as null.
    val nullContentRow = new GenericRowWithSchema(
      Array[Any]("user", null, ""), structContentSchema) //scalastyle:ignore null
    assert(chat.encodeMessagesToMap(Seq(nullContentRow)).head("content") == null) //scalastyle:ignore null

    // Heterogeneous parts: a null element, a loose scalar, and a map-backed part all survive intact.
    val heterogeneous: Seq[Any] = Seq(
      null, //scalastyle:ignore null
      "loose-string",
      Map("type" -> "text", "text" -> "hi")
    )
    val heteroRow = new GenericRowWithSchema(Array[Any]("user", heterogeneous, ""), structContentSchema)
    val content = chat.encodeMessagesToMap(Seq(heteroRow)).head("content").asInstanceOf[Seq[Any]]

    assert(content.length == 3)
    assert(content.head == null) //scalastyle:ignore null
    assert(content(1) == "loose-string")
    assert(content(2).asInstanceOf[Map[String, Any]]("type") == "text")
  }

  test("encodeMessagesToMap recurses through nested struct, array, and map fields") {
    val chat = new OpenAIChatCompletion()
    val nestedImageSchema = StructType(Seq(
      StructField("url", StringType, nullable = true),
      StructField("detail", StringType, nullable = true)
    ))
    val nestedPartSchema = StructType(Seq(
      StructField("type", StringType, nullable = true),
      StructField("image_url", nestedImageSchema, nullable = true),
      StructField("tags", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("meta", MapType(StringType, StringType, valueContainsNull = true), nullable = true),
      StructField("missing", StringType, nullable = true)
    ))
    val nestedPart = new GenericRowWithSchema(Array[Any](
      "image_url",
      new GenericRowWithSchema(Array[Any]("http://x/y.png", "high"), nestedImageSchema),
      Seq("a", null, "b"), //scalastyle:ignore null
      Map("k" -> "v"),
      null //scalastyle:ignore null
    ), nestedPartSchema)
    val messageWithNested = new GenericRowWithSchema(
      Array[Any]("user", Seq(nestedPart), ""), structContentSchema)

    val part = chat.encodeMessagesToMap(Seq(messageWithNested))
      .head("content").asInstanceOf[Seq[Map[String, Any]]].head

    assert(part("type") == "image_url")
    val image = part("image_url").asInstanceOf[Map[String, Any]]     // nested Row -> Map
    assert(image("url") == "http://x/y.png")
    assert(image("detail") == "high")
    assert(part("tags").asInstanceOf[Seq[Any]] == Seq("a", null, "b")) //scalastyle:ignore null
    assert(part("meta").asInstanceOf[Map[String, Any]]("k") == "v")   // nested Map
    assert(!part.contains("missing"))                                 // null field omitted
  }

  test("OpenAIChatCompletion getStringEntity carries image detail and preserves nested image_url objects") {
    val chat = new OpenAIChatCompletion()
    val parts: Seq[Any] = Seq(
      Map("type" -> "image_url", "image_url" -> "http://x/a.png", "detail" -> "high"),
      Map("type" -> "image_url", "image_url" -> Map("url" -> "http://x/b.png"))
    )
    val row = new GenericRowWithSchema(Array[Any]("user", parts, ""), structContentSchema)

    val payload = parseEntity(chat.getStringEntity(Seq(row), Map("temperature" -> 0.0)))
    val JsArray(messages) = payload.fields("messages")
    val JsArray(content) = messages.head.asJsObject.fields("content")

    // A flat image_url String plus detail is reshaped into a nested {"url": ..., "detail": ...} object.
    val first = content.head.asJsObject
    val firstImage = first.fields("image_url").asJsObject
    assert(firstImage.fields("url").convertTo[String] == "http://x/a.png")
    assert(firstImage.fields("detail").convertTo[String] == "high")
    assert(!first.fields.contains("detail")) // detail is moved inside image_url, not left at top level

    // An already-nested image_url object is left untouched.
    val second = content(1).asJsObject.fields("image_url").asJsObject
    assert(second.fields("url").convertTo[String] == "http://x/b.png")
  }

  test("OpenAIChatCompletion getStringEntity drops unsupported extra leaf fields instead of aborting") {
    val chat = new OpenAIChatCompletion()
    // A struct image part with an extra Long leaf: master serializes every field via AnyJsonFormat,
    // which cannot serialize a Long and aborts the whole Spark job. The serializer must emit only the
    // canonical {type, image_url:{url}} wire fields so the extra leaf never reaches AnyJsonFormat.
    val extraLeafSchema = StructType(Seq(StructField("type", StringType),
      StructField("image_url", StringType), StructField("count", LongType)))
    val part = new GenericRowWithSchema(Array[Any]("image_url", "http://x/y.png", 3L), extraLeafSchema)
    val payload = parseEntity(chat.getStringEntity(Seq(structMessageRow("user", Seq(part))),
      Map("temperature" -> 0.0)))
    val JsArray(messages) = payload.fields("messages")
    val JsArray(content) = messages.head.asJsObject.fields("content")
    val imagePart = content.head.asJsObject
    assert(imagePart.fields("type").convertTo[String] == "image_url")
    assert(imagePart.fields("image_url").asJsObject.fields("url").convertTo[String] == "http://x/y.png")
    assert(!imagePart.fields.contains("count"))  // the extra Long leaf never reaches AnyJsonFormat
  }

  test("OpenAIChatCompletion getStringEntity is null-safe for map-backed null text") {
    // Defense in depth: even if validation drifts, an explicit map text -> null must never NPE at
    // .toString and abort the job; the null text contributes nothing (treated as absent).
    val chat = new OpenAIChatCompletion()
    def content(parts: Seq[Map[String, String]]): JsValue =
      parseEntity(chat.getStringEntity(Seq(compositeMessageRow("user", parts)), Map("temperature" -> 0.0)))
        .fields("messages").asInstanceOf[JsArray].elements.head.asJsObject.fields("content")
    assert(content(Seq(Map("type" -> "text", "text" -> null))).convertTo[String] == "") //scalastyle:ignore null
    val JsArray(withImage) = content(Seq(Map("type" -> "text", "text" -> null), //scalastyle:ignore null
      Map("type" -> "image_url", "image_url" -> "http://x/y.png")))
    val textPart = withImage.find(_.asJsObject.fields("type").convertTo[String] == "text").get.asJsObject
    assert(!textPart.fields.contains("text"))  // null text dropped (not .toString'd)
    assert(withImage.exists(_.asJsObject.fields("type").convertTo[String] == "image_url"))  // image preserved
  }

  test("OpenAIPrompt chat_completions blank/whitespace systemPrompt is not a validation skip") {
    // Faithful wave-2 regression: OpenAIPrompt.getPromptsForMessage injects a system text part built from
    // systemPrompt (and a user text part). An empty/whitespace systemPrompt -- or empty rendered user text
    // -- must NOT become a row-local validation skip, and must serialize to the legacy empty-string Chat
    // content so the request still reaches HTTP.
    val chat = new OpenAIChatCompletion()
    Seq(("", "Answer this"), ("   ", "Answer this"), ("You are helpful.", "")).foreach {
      case (systemPrompt, userText) =>
        val prompt = new OpenAIPrompt().setSystemPrompt(systemPrompt)
        val messages = prompt.getPromptsForMessage(Right(userText))
        val rows = messages.map(m => compositeMessageRow(m.role, m.content))
        assert(OpenAIChatCompletion.validateMessagesForError(rows).isEmpty)  // no local validation skip
        val payload = parseEntity(chat.getStringEntity(rows, Map("temperature" -> 0.0)))
        val JsArray(wire) = payload.fields("messages")
        assert(wire.head.asJsObject.fields("content").convertTo[String] == systemPrompt)  // system preserved
        assert(wire(1).asJsObject.fields("content").convertTo[String] == userText)        // user preserved
    }
  }

  test("OpenAIChatCompletion.validateMessagesForError classifies malformed and null-safe content") {
    val roleOnlySchema = StructType(Seq(StructField("role", StringType, nullable = false)))

    def firstError(messages: Seq[Row]): Option[String] =
      OpenAIChatCompletion.validateMessagesForError(messages).map(_.getString(0))

    // Plain String content is valid; null content is rejected before serialization and HTTP.
    assert(firstError(Seq(messageRow("user", "plain text"))).isEmpty)
    assert(firstError(Seq(messageRow("user", null))).exists(_.contains("null content"))) //scalastyle:ignore null

    // A null message element inside a non-null array is a row-local error, not a silent pass that
    // later crashes serialization (defect: null message elements).
    assert(firstError(Seq(null)).exists(_.contains("message 0 is null"))) //scalastyle:ignore null

    // A message with no content field is flagged (AC-004).
    assert(firstError(Seq(new GenericRowWithSchema(Array[Any]("user"), roleOnlySchema)))
      .exists(_.contains("missing a content field")))

    // Off-spec top-level scalar content (schema-typed non-string/non-array) is a row-local error, so it
    // never reaches encodeMessagesToMap's throw in the request UDF (defect: off-spec scalar content).
    assert(firstError(Seq(unsupportedMessageRow("user", 123)))
      .exists(_.contains("unsupported content type")))

    // Finding 1: the declared content type is checked (not just the value), so an unsupported content type
    // even with a NULL value is a row-local error instead of reaching encodeMessagesToMap's throw.
    assert(firstError(Seq(new GenericRowWithSchema(Array[Any]("user", null), //scalastyle:ignore null
      unsupportedContentSchema))).exists(_.contains("unsupported content type")))

    // Finding 1: each message must carry a present, String role. A missing/null/non-string role is a
    // row-local error -- it would otherwise throw in encodeMessagesToMap's role extraction and abort.
    val contentOnlySchema = StructType(Seq(StructField("content", StringType, nullable = true)))
    assert(firstError(Seq(new GenericRowWithSchema(Array[Any]("hi"), contentOnlySchema)))
      .exists(_.contains("missing a role field")))
    assert(firstError(Seq(new GenericRowWithSchema(Array[Any](null, "hi", ""), //scalastyle:ignore null
      stringMessageSchema))).exists(_.contains("null role")))
    val intRoleSchema = StructType(Seq(StructField("role", IntegerType), StructField("content", StringType)))
    assert(firstError(Seq(new GenericRowWithSchema(Array[Any](5, "hi"), intRoleSchema)))
      .exists(_.contains("non-string role")))

    // Structured part problems are flagged distinctly.
    assert(firstError(Seq(structMessageRow("user", Seq(imagePartRow(null, "hi", null))))) //scalastyle:ignore null
      .exists(_.contains("missing a type")))
    assert(firstError(Seq(structMessageRow("user", Seq(imagePartRow("   ", "hi", null))))) //scalastyle:ignore null
      .exists(_.contains("missing a type"))) // whitespace-only type is blank
    assert(firstError(Seq(structMessageRow("user", Seq(imagePartRow("banana", "hi", null))))) //scalastyle:ignore null
      .exists(_.contains("unsupported type")))
    // A whitespace-padded canonical type is rejected, not silently trimmed, so validation and the
    // exact-match serializer cannot disagree and drop the image (defect: canonicalization mismatch).
    val padded = imagePartRow(" image_url ", null, null) //scalastyle:ignore null
    assert(firstError(Seq(structMessageRow("user", Seq(padded)))).exists(_.contains("unsupported type")))
    // A struct part carrying an unsupported extra leaf (e.g. a Long) is a row-local error rather than an
    // AnyJsonFormat crash that aborts the whole job (defect: unsupported extra leaf fields).
    val extraLeafSchema = StructType(Seq(StructField("type", StringType),
      StructField("image_url", StringType), StructField("count", LongType)))
    val extraLeaf = new GenericRowWithSchema(Array[Any]("image_url", "http://x/y.png", 3L), extraLeafSchema)
    assert(firstError(Seq(structMessageRow("user", Seq(extraLeaf))))
      .exists(_.contains("unsupported field 'count'")))
    assert(firstError(Seq(structMessageRow("user", Seq(imagePartRow("image_url", null, ""))))) //scalastyle:ignore null
      .exists(_.contains("empty image_url")))

    // Backward-compat (wave-2): a present, String text part -- including empty and whitespace strings -- is
    // NOT flagged. OpenAIPrompt injects a system text part, so an empty/whitespace systemPrompt must keep
    // the legacy empty-string Chat path instead of turning every row into a validation skip; struct- and
    // map-backed blank text both pass.
    def blankTextStruct(t: String): Seq[Row] =
      Seq(structMessageRow("user", Seq(imagePartRow("text", t, null)))) //scalastyle:ignore null
    assert(firstError(blankTextStruct("")).isEmpty)
    assert(firstError(blankTextStruct("   ")).isEmpty)
    assert(firstError(Seq(compositeMessageRow("user", Seq(Map("type" -> "text", "text" -> ""))))).isEmpty)
    assert(firstError(Seq(compositeMessageRow("system", Seq(Map("type" -> "text", "text" -> "   "))))).isEmpty)

    // Finding 2: a "text" part must carry a present String text. Struct- and map-backed absent/null/
    // non-string text are all row-local errors (intentionally consistent semantics), so a bare
    // {"type":"text"} wire part can never be emitted and a present-null text can never NPE at serialization.
    assert(firstError(Seq(structMessageRow("user",
      Seq(imagePartRow("text", null, null))))).exists(_.contains("null text value"))) //scalastyle:ignore null
    assert(firstError(Seq(compositeMessageRow("user",
      Seq(Map("type" -> "text", "text" -> null))))).exists(_.contains("null text value"))) //scalastyle:ignore null
    val textlessPartSchema = StructType(Seq(StructField("type", StringType)))
    assert(firstError(Seq(structMessageRow("user",
      Seq(new GenericRowWithSchema(Array[Any]("text"), textlessPartSchema)))))
      .exists(_.contains("missing a text value")))
    assert(firstError(Seq(compositeMessageRow("user", Seq(Map("type" -> "text")))))
      .exists(_.contains("missing a text value")))
    val intTextSchema = StructType(Seq(StructField("type", StringType), StructField("text", IntegerType)))
    assert(firstError(Seq(structMessageRow("user",
      Seq(new GenericRowWithSchema(Array[Any]("text", 5), intTextSchema)))))
      .exists(_.contains("non-string text value")))
    // An image part does not need text: a present-null (or absent) text on an image_url part is ignored,
    // not flagged, since the serializer emits only the nested image_url wire object.
    assert(firstError(Seq(structMessageRow("user",
      Seq(imagePartRow("image_url", null, "http://x/img.png"))))).isEmpty) //scalastyle:ignore null

    // An image_url part whose nested {url} is empty is flagged (consistency: empty nested image URL).
    val nestedUrlSchema = StructType(Seq(
      StructField("url", StringType, nullable = true),
      StructField("detail", StringType, nullable = true)))
    val nestedImagePartSchema = StructType(Seq(
      StructField("type", StringType, nullable = true),
      StructField("image_url", nestedUrlSchema, nullable = true)))
    val nestedContentSchema = StructType(Seq(
      StructField("role", StringType, nullable = false),
      StructField("content", ArrayType(nestedImagePartSchema, containsNull = true), nullable = true),
      StructField("name", StringType, nullable = true)))
    def nestedImageMsg(url: String): Row = new GenericRowWithSchema(Array[Any]("user", Seq(
      new GenericRowWithSchema(Array[Any](
        "image_url",
        new GenericRowWithSchema(Array[Any](url, null), nestedUrlSchema) //scalastyle:ignore null
      ), nestedImagePartSchema)), ""), nestedContentSchema)
    assert(firstError(Seq(nestedImageMsg(""))).exists(_.contains("empty image_url")))
    assert(firstError(Seq(nestedImageMsg("http://x/y.png"))).isEmpty)

    // A null part element and a loose scalar element are flagged as malformed.
    val nullPartRow = new GenericRowWithSchema(
      Array[Any]("user", Seq(null), ""), structContentSchema) //scalastyle:ignore null
    assert(firstError(Seq(nullPartRow)).exists(_.contains("content part 0 is null")))
    val scalarPartRow = new GenericRowWithSchema(Array[Any]("user", Seq("loose"), ""), structContentSchema)
    assert(firstError(Seq(scalarPartRow)).exists(_.contains("unsupported element type")))

    // A well-formed multimodal message is not flagged.
    assert(firstError(Seq(structMessageRow("user", Seq(
      imagePartRow("text", "hi", null), //scalastyle:ignore null
      imagePartRow("image_url", null, "http://x/img.png") //scalastyle:ignore null
    )))).isEmpty)

    // Defensive guarantee: a degenerate, schema-less Row becomes a row-local error instead of throwing
    // out of the request path (validateMessagesForError must never throw).
    assert(firstError(Seq(Row("user", "hi"))).exists(_.contains("Invalid chat message content")))
  }

  test("OpenAIChatCompletion response_format wraps bare schemas and exposes type") {
    val chat = new OpenAIChatCompletion()
    chat.setResponseFormat(Map(
      "name" -> "answer_schema",
      "strict" -> true,
      "schema" -> Map(
        "type" -> "object",
        "properties" -> Map("answer" -> Map("type" -> "string"))
      )
    ))

    val responseFormat = chat.getResponseFormat
    assert(chat.getResponseFormatType == "json_schema")
    assert(responseFormat("type") == "json_schema")
    val jsonSchema = responseFormat("json_schema").asInstanceOf[Map[String, Any]]
    assert(jsonSchema("name") == "answer_schema")
    assert(jsonSchema.contains("schema"))
  }

  test("OpenAIResponses optional params merge text/reasoning and drop gpt-5 sampling") {
    val responses = new OpenAIResponses()
      .setDeploymentName("gpt-5-mini")
      .setTemperature(0.3)
      .setTopP(0.7)
      .setResponseFormat("json_object")
      .setVerbosity("high")
      .setReasoningEffort("medium")

    val params = responses.getOptionalParams(messageRow("user", "hello"))

    assert(params("model") == "gpt-5-mini")
    assert(!params.contains("temperature"))
    assert(!params.contains("top_p"))
    assert(!params.contains("reasoning_effort"))

    val text = params("text").asInstanceOf[Map[String, Any]]
    val format = text("format").asInstanceOf[Map[String, Any]]
    assert(format("type") == "json_object")
    assert(text("verbosity") == "high")

    val reasoning = params("reasoning").asInstanceOf[Map[String, Any]]
    assert(reasoning("effort") == "medium")
  }

  test("OpenAIResponses keeps sampling params for non-gpt5 deployments") {
    val responses = new OpenAIResponses()
      .setDeploymentName("gpt-4.1-mini")
      .setTemperature(0.2)
      .setTopP(0.6)

    val params = responses.getOptionalParams(messageRow("user", "hello"))

    assert(params("model") == "gpt-4.1-mini")
    assert(params("temperature") == 0.2)
    assert(params("top_p") == 0.6)
  }

  test("OpenAIResponses getStringEntity wraps plain text and preserves composite parts") {
    val responses = new OpenAIResponses()
    val compositeParts = Seq(
      Map("type" -> "input_file", "filename" -> "example.txt", "file_data" -> "AAA")
    )

    val entity = responses.getStringEntity(
      Seq(
        messageRow("user", "plain text"),
        compositeMessageRow("user", compositeParts)
      ),
      Map("model" -> "gpt-4.1-mini")
    )

    val payload = parseEntity(entity)
    val JsArray(inputs) = payload.fields("input")

    val JsArray(firstContent) = inputs.head.asJsObject.fields("content")
    assert(firstContent.head.asJsObject.fields("type").convertTo[String] == "input_text")
    assert(firstContent.head.asJsObject.fields("text").convertTo[String] == "plain text")

    // The map-backed composite part is preserved verbatim (AC-009): type, filename and file_data all
    // survive the shared-encoder change untouched.
    val JsArray(secondContent) = inputs(1).asJsObject.fields("content")
    val filePart = secondContent.head.asJsObject
    assert(filePart.fields("type").convertTo[String] == "input_file")
    assert(filePart.fields("filename").convertTo[String] == "example.txt")
    assert(filePart.fields("file_data").convertTo[String] == "AAA")
  }

  test("OpenAI chat and responses stages expose expected response schemas") {
    assert(new OpenAIChatCompletion().responseDataType == ChatModelResponse.schema)
    assert(new OpenAIResponses().responseDataType == ResponsesModelResponse.schema)
  }
}
