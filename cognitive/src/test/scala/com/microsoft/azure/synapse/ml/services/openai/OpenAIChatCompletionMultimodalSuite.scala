// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.{ErrorUtils, HTTPRequestData, HTTPResponseData, HTTPSchema}
import com.microsoft.azure.synapse.ml.services.aifoundry.AIFoundryChatCompletion
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructField, StructType}
import spray.json._

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

object OpenAIChatCompletionMultimodalTestData extends Serializable {
  def echoRequestBody(
      client: CloseableHttpClient,
      request: HTTPRequestData
  ): HTTPResponseData = {
    val requestBody = new String(request.entity.get.content, StandardCharsets.UTF_8)
    val response = JsObject(
      "id" -> JsString("chatcmpl_test"),
      "object" -> JsString("chat.completion"),
      "created" -> JsString("1"),
      "model" -> JsString("gpt-5.1"),
      "choices" -> JsArray(JsObject(
        "message" -> JsObject(
          "role" -> JsString("assistant"),
          "content" -> JsString(requestBody),
          "name" -> JsNull
        ),
        "index" -> JsNumber(0),
        "finish_reason" -> JsString("stop")
      )),
      "system_fingerprint" -> JsNull,
      "usage" -> JsNull
    )
    HTTPSchema.stringToResponse(response.compactPrint, 200, "OK")
  }
}

class OpenAIChatCompletionMultimodalSuite extends TestBase {

  private val imageUrlSchema = StructType(Seq(
    StructField("url", StringType, nullable = true),
    StructField("detail", StringType, nullable = true)
  ))

  private val contentPartSchema = StructType(Seq(
    StructField("type", StringType, nullable = false),
    StructField("text", StringType, nullable = true),
    StructField("image_url", imageUrlSchema, nullable = true)
  ))

  private val messageSchema = StructType(Seq(
    StructField("role", StringType, nullable = false),
    StructField(
      "content",
      ArrayType(contentPartSchema, containsNull = true),
      nullable = true
    ),
    StructField("name", StringType, nullable = true)
  ))

  private val messagesType = ArrayType(messageSchema, containsNull = true)

  private def requestSchema(includeError: Boolean = false): StructType = {
    val fields = Seq(
      StructField("id", StringType, nullable = false),
      StructField("messages", messagesType, nullable = true)
    )
    StructType(if (includeError) fields :+ StructField("error", ErrorUtils.ErrorSchema, nullable = true) else fields)
  }

  private def imageUrl(url: Option[String], detail: Option[String] = None): Row =
    new GenericRowWithSchema(Array[Any](url.orNull, detail.orNull), imageUrlSchema)

  private def contentPart(
      partType: String,
      text: Option[String] = None,
      image: Option[Row] = None
  ): Row =
    new GenericRowWithSchema(Array[Any](partType, text.orNull, image.orNull), contentPartSchema)

  private def message(role: String, content: Any): Row =
    new GenericRowWithSchema(Array[Any](role, content, null), messageSchema) // scalastyle:ignore null

  private def chat(): OpenAIChatCompletion = new OpenAIChatCompletion()
    .setUrl("https://example.services.ai.azure.com/openai/v1")
    .setDeploymentName("gpt-5.1")
    .setMessagesCol("messages")
    .setSubscriptionKey("unused")
    .setOutputCol("output")
    .setErrorCol("error")

  test("multimodal content survives the transformer and request path") {
    val requestBodies = spark.sparkContext.collectionAccumulator[String]
    val systemMessage = message("system", Seq(contentPart("text", text = Some("Describe images precisely."))))
    val userMessage = message("user", Seq(
      contentPart("text", text = Some("What is shown?")),
      contentPart(
        "image_url",
        image = Some(imageUrl(Some("data:image/png;base64,AAA"), Some("low")))
      ),
      contentPart(
        "image_url",
        image = Some(imageUrl(Some("https://example.com/image.png")))
      )
    ))
    val malformedMessage = message("user", Seq(
      contentPart("image_url", image = Some(imageUrl(None)))
    ))
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("valid", Seq(systemMessage, userMessage)),
        Row("invalid", Seq(malformedMessage))
      ), 1),
      requestSchema()
    )

    val transformer = chat().setHandler { (client: CloseableHttpClient, request: HTTPRequestData) =>
      requestBodies.add(new String(request.entity.get.content, StandardCharsets.UTF_8))
      OpenAIChatCompletionMultimodalTestData.echoRequestBody(client, request)
    }
    val transformed = transformer.transform(input)
    val rows = transformed.orderBy("id").collect().map(row => row.getAs[String]("id") -> row).toMap

    assert(transformed.schema("messages").dataType == input.schema("messages").dataType)
    assert(transformer.transformSchema(input.schema)("messages").dataType == input.schema("messages").dataType)

    val valid = rows("valid")
    assert(Option(valid.getAs[Row]("error")).isEmpty)
    val response = ChatModelResponse.makeFromRowConverter(valid.getAs[Row]("output"))
    val payload = response.choices.head.message.content.parseJson.asJsObject
    assert(payload.fields.get("model").contains(JsString("gpt-5.1")))
    assert(requestBodies.value.asScala.nonEmpty)
    assert(requestBodies.value.asScala.forall(_.parseJson == payload))

    val JsArray(messages) = payload.fields("messages")
    assert(messages.size == 2)
    assert(messages.head.asJsObject.fields("content") ==
      JsArray(JsObject("type" -> JsString("text"), "text" -> JsString("Describe images precisely."))))
    val JsArray(userParts) = messages(1).asJsObject.fields("content")
    assert(userParts.head.asJsObject ==
      JsObject("type" -> JsString("text"), "text" -> JsString("What is shown?")))
    assert(userParts(1).asJsObject == JsObject(
      "type" -> JsString("image_url"),
      "image_url" -> JsObject(
        "url" -> JsString("data:image/png;base64,AAA"),
        "detail" -> JsString("low")
      )
    ))
    assert(userParts(2).asJsObject == JsObject(
      "type" -> JsString("image_url"),
      "image_url" -> JsObject(
        "url" -> JsString("https://example.com/image.png")
      )
    ))

    val restored = valid.getAs[scala.collection.Seq[Row]]("messages")
    assert(restored(1).getAs[scala.collection.Seq[Row]]("content")(1)
      .getAs[Row]("image_url").getAs[String]("url") == "data:image/png;base64,AAA")

    val invalid = rows("invalid")
    assert(Option(invalid.getAs[Row]("output")).isEmpty)
    assert(invalid.getAs[Row]("error").getAs[String]("response") ==
      "messages[0].content[0].image_url requires a non-empty string 'url' field")
    assert(invalid.getAs[scala.collection.Seq[Row]]("messages").nonEmpty)
  }

  test("transform resolves messagesCol with Spark case-insensitive semantics") {
    assert(!spark.conf.get("spark.sql.caseSensitive").toBoolean)
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("valid", Seq(message("user", Seq(contentPart("text", text = Some("Describe."))))))
      ), 1),
      requestSchema()
    ).withColumnRenamed("messages", "MESSAGES")
    val requestBodies = spark.sparkContext.collectionAccumulator[String]

    val result = chat()
      .setHandler { (client: CloseableHttpClient, request: HTTPRequestData) =>
        requestBodies.add(new String(request.entity.get.content, StandardCharsets.UTF_8))
        OpenAIChatCompletionMultimodalTestData.echoRequestBody(client, request)
      }
      .transform(input)
      .head()

    assert(Option(result.getAs[Row]("error")).isEmpty)
    assert(Option(result.getAs[Row]("output")).isDefined)
    assert(requestBodies.value.asScala.size == 1)
  }

  test("AIFoundryChatCompletion inherits multimodal validation and serialization") {
    val validMessage = message("user", Seq(
      contentPart("text", text = Some("What is shown?")),
      contentPart("image_url", image = Some(imageUrl(Some("data:image/png;base64,AAA"))))
    ))
    val invalidMessage = message("user", Seq(
      contentPart("image_url", image = Some(imageUrl(None)))
    ))
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("valid", Seq(validMessage)),
        Row("invalid", Seq(invalidMessage))
      ), 1),
      requestSchema()
    )
    val transformer = new AIFoundryChatCompletion()
      .setUrl("https://example.services.ai.azure.com")
      .setModel("gpt-5.1")
      .setMessagesCol("messages")
      .setSubscriptionKey("unused")
      .setOutputCol("output")
      .setErrorCol("error")
      .setHandler(OpenAIChatCompletionMultimodalTestData.echoRequestBody _)

    val rows = transformer.transform(input).collect().map(row => row.getAs[String]("id") -> row).toMap
    val response = ChatModelResponse.makeFromRowConverter(rows("valid").getAs[Row]("output"))
    val payload = response.choices.head.message.content.parseJson.asJsObject
    val JsArray(messages) = payload.fields("messages")
    val JsArray(parts) = messages.head.asJsObject.fields("content")

    assert(parts(1).asJsObject.fields("image_url").asJsObject.fields("url") ==
      JsString("data:image/png;base64,AAA"))
    assert(Option(rows("valid").getAs[Row]("error")).isEmpty)
    assert(Option(rows("invalid").getAs[Row]("output")).isEmpty)
    assert(rows("invalid").getAs[Row]("error").getAs[String]("response") ==
      "messages[0].content[0].image_url requires a non-empty string 'url' field")
  }

  test("null and empty content produce row errors without HTTP") {
    val requestCount = spark.sparkContext.longAccumulator
    val existingError = Row("upstream error", null) // scalastyle:ignore null
    val rows = Seq(
      Row("empty-content", Seq(message("user", Seq.empty[Row])), null), // scalastyle:ignore null
      Row("empty-messages", Seq.empty[Row], null), // scalastyle:ignore null
      Row("existing-error", Seq(message("user", Seq(contentPart("image_url", image = Some(imageUrl(None)))))),
        existingError), // scalastyle:ignore null
      Row("blank-type", Seq(message("user", Seq(contentPart("   ")))), null), // scalastyle:ignore null
      Row("null-content", Seq(message("user", null)), null), // scalastyle:ignore null
      Row("null-messages", null, null), // scalastyle:ignore null
      Row("null-part", Seq(message("user", Seq[Row](null))), null) // scalastyle:ignore null
    )
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(rows, 1),
      requestSchema(includeError = true)
    )
    val transformer = chat().setHandler { (_: CloseableHttpClient, _: HTTPRequestData) =>
      requestCount.add(1L)
      throw new AssertionError("HTTP must not run for null or malformed messages")
    }

    val output = transformer.transform(input).collect().map(row => row.getAs[String]("id") -> row).toMap

    assert(requestCount.value == 0L)
    assert(output("empty-content").getAs[Row]("error").getAs[String]("response") ==
      "messages[0].content must not be empty")
    assert(output("null-content").getAs[Row]("error").getAs[String]("response") ==
      "messages[0].content must be an array of content part objects")
    assert(output("null-part").getAs[Row]("error").getAs[String]("response") ==
      "messages[0].content[0] must be an object")
    assert(output("blank-type").getAs[Row]("error").getAs[String]("response") ==
      "messages[0].content[0] requires a non-empty string 'type' field")
    assert(output("existing-error").getAs[Row]("error").getAs[String]("response") == "upstream error")
    assert(Option(output("empty-messages").getAs[Row]("error")).isEmpty)
    assert(Option(output("null-messages").getAs[Row]("error")).isEmpty)
    output.values.foreach(row => assert(Option(row.getAs[Row]("output")).isEmpty))
  }

  test("case-insensitive error column matching preserves upstream errors") {
    assert(!spark.conf.get("spark.sql.caseSensitive").toBoolean)
    val existingError = Row("upstream error", null) // scalastyle:ignore null
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(
          "invalid",
          Seq(message("user", Seq(contentPart("image_url", image = Some(imageUrl(None)))))),
          existingError
        )
      ), 1),
      StructType(requestSchema().fields :+
        StructField("UPSTREAM_ERROR", ErrorUtils.ErrorSchema, nullable = true))
    )
    val transformer = chat()
      .setErrorCol("upstream_error")
      .setHandler { (_: CloseableHttpClient, _: HTTPRequestData) =>
        throw new AssertionError("HTTP must not run for malformed messages")
      }

    val result = transformer.transform(input).head()

    assert(result.getAs[Row]("upstream_error").getAs[String]("response") == "upstream error")
    assert(Option(result.getAs[Row]("output")).isEmpty)
  }

  test("Chat scratch columns preserve resolver-colliding input columns") {
    assert(!spark.conf.get("spark.sql.caseSensitive").toBoolean)
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(
          "valid",
          Seq(message("user", Seq(contentPart("text", text = Some("hello"))))),
          "keep original",
          "keep original suffix",
          "keep validation",
          "keep validation suffix"
        )
      ), 1),
      StructType(requestSchema().fields ++ Seq(
        StructField("ORIGINALMESSAGES", StringType, nullable = false),
        StructField("ORIGINALMESSAGES_1", StringType, nullable = false),
        StructField("STRUCTUREDMESSAGEVALIDATIONERROR", StringType, nullable = false),
        StructField("STRUCTUREDMESSAGEVALIDATIONERROR_1", StringType, nullable = false)
      ))
    )
    val result = chat()
      .setHandler(OpenAIChatCompletionMultimodalTestData.echoRequestBody _)
      .transform(input)
      .head()

    assert(result.getAs[String]("ORIGINALMESSAGES") == "keep original")
    assert(result.getAs[String]("ORIGINALMESSAGES_1") == "keep original suffix")
    assert(result.getAs[String]("STRUCTUREDMESSAGEVALIDATIONERROR") == "keep validation")
    assert(result.getAs[String]("STRUCTUREDMESSAGEVALIDATIONERROR_1") == "keep validation suffix")
  }

  test("non-error upstream columns are replaced by Chat validation errors") {
    assert(!spark.conf.get("spark.sql.caseSensitive").toBoolean)
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(
          "invalid",
          Seq(message("user", Seq(contentPart("image_url", image = Some(imageUrl(None)))))),
          "not an error struct"
        )
      ), 1),
      StructType(requestSchema().fields :+
        StructField("UPSTREAM_ERROR", StringType, nullable = true))
    )
    val transformer = chat()
      .setErrorCol("upstream_error")
      .setHandler { (_: CloseableHttpClient, _: HTTPRequestData) =>
        throw new AssertionError("HTTP must not run for malformed messages")
      }

    val result = transformer.transform(input).head()

    assert(result.schema("upstream_error").dataType == ErrorUtils.ErrorSchema)
    assert(result.getAs[Row]("upstream_error").getAs[String]("response") ==
      "messages[0].content[0].image_url requires a non-empty string 'url' field")
    assert(Option(result.getAs[Row]("output")).isEmpty)
  }

  test("legacy map-backed text ignores null values") {
    val mapMessageSchema = StructType(Seq(
      StructField("role", StringType, nullable = false),
      StructField(
        "content",
        ArrayType(MapType(StringType, StringType, valueContainsNull = true), containsNull = false),
        nullable = true
      ),
      StructField("name", StringType, nullable = true)
    ))
    val legacyMessage = new GenericRowWithSchema(
      Array[Any](
        "user",
        Seq(
          Map[String, String]("type" -> "text", "text" -> null), // scalastyle:ignore null
          Map("type" -> "input_file", "filename" -> "example.txt"),
          Map("type" -> "text", "text" -> "kept")
        ),
        null // scalastyle:ignore null
      ),
      mapMessageSchema
    )

    val payload = EntityUtils.toString(
      new OpenAIChatCompletion().getStringEntity(Seq(legacyMessage), Map.empty)
    ).parseJson.asJsObject
    val JsArray(messages) = payload.fields("messages")

    assert(messages.head.asJsObject.fields("content") == JsString("kept"))
  }

  test("map-backed image parts preserve nested wire shape and reject extra fields") {
    val mapMessageSchema = StructType(Seq(
      StructField("role", StringType, nullable = false),
      StructField(
        "content",
        ArrayType(MapType(StringType, StringType, valueContainsNull = true), containsNull = false),
        nullable = true
      ),
      StructField("name", StringType, nullable = true)
    ))
    def mapMessage(parts: Seq[Map[String, String]]): Row =
      new GenericRowWithSchema(
        Array[Any]("user", parts, null), // scalastyle:ignore null
        mapMessageSchema
      )

    val payload = EntityUtils.toString(
      new OpenAIChatCompletion().getStringEntity(Seq(mapMessage(Seq(
        Map("type" -> "text", "text" -> "Describe."),
        Map(
          "type" -> "image_url",
          "image_url" -> "data:image/png;base64,AAA",
          "detail" -> "low"
        )
      ))), Map.empty)
    ).parseJson.asJsObject
    val JsArray(messages) = payload.fields("messages")
    val JsArray(parts) = messages.head.asJsObject.fields("content")

    assert(parts.head.asJsObject ==
      JsObject("type" -> JsString("text"), "text" -> JsString("Describe.")))
    assert(parts(1).asJsObject == JsObject(
      "type" -> JsString("image_url"),
      "image_url" -> JsObject(
        "url" -> JsString("data:image/png;base64,AAA"),
        "detail" -> JsString("low")
      )
    ))

    val error = intercept[IllegalArgumentException] {
      new OpenAIChatCompletion().getStringEntity(Seq(mapMessage(Seq(Map(
        "type" -> "image_url",
        "image_url" -> "data:image/png;base64,AAA",
        "unsupported" -> "value"
      )))), Map.empty)
    }
    assert(error.getMessage == "messages[0].content[0] contains unsupported fields")

    val unsupportedTypeError = intercept[IllegalArgumentException] {
      new OpenAIChatCompletion().getStringEntity(Seq(mapMessage(Seq(
        Map(
          "type" -> "image_url",
          "image_url" -> "data:image/png;base64,AAA"
        ),
        Map("type" -> "input_audio")
      ))), Map.empty)
    }
    assert(unsupportedTypeError.getMessage ==
      "messages[0].content[1] has an unsupported type; supported types are 'text' and 'image_url'")
  }

  test("short content rows fail with a structural validation error") {
    val shortPart = new GenericRowWithSchema(Array[Any]("text"), contentPartSchema)

    val error = intercept[IllegalArgumentException] {
      new OpenAIChatCompletion().getStringEntity(Seq(message("user", Seq(shortPart))), Map.empty)
    }

    assert(error.getMessage ==
      "messages[0].content[0] is invalid: Struct content part does not match its declared schema")
  }

  test("short message rows fail with stable structural errors") {
    val missingRole = new GenericRowWithSchema(Array.empty[Any], messageSchema)
    val missingContent = new GenericRowWithSchema(Array[Any]("user"), messageSchema)

    val roleError = intercept[IllegalArgumentException] {
      new OpenAIChatCompletion().getStringEntity(Seq(missingRole), Map.empty)
    }
    assert(roleError.getMessage == "messages[0].role must be a non-empty string")

    val contentError = intercept[IllegalArgumentException] {
      new OpenAIChatCompletion().getStringEntity(Seq(missingContent), Map.empty)
    }
    assert(contentError.getMessage ==
      "messages[0].content must be a string or an array of content part objects")
  }

  test("string content runtime type mismatches fail with a stable error") {
    val stringMessageSchema = StructType(Seq(
      StructField("role", StringType, nullable = false),
      StructField("content", StringType, nullable = true),
      StructField("name", StringType, nullable = true)
    ))
    val invalidContent = new GenericRowWithSchema(
      Array[Any]("user", 1, null), // scalastyle:ignore null
      stringMessageSchema
    )

    val error = intercept[IllegalArgumentException] {
      new OpenAIChatCompletion().getStringEntity(Seq(invalidContent), Map.empty)
    }

    assert(error.getMessage == "messages[0].content must be a string")
  }

  test("missing content fields produce row errors without HTTP") {
    val requestCount = spark.sparkContext.longAccumulator
    val missingContentSchema = StructType(Seq(
      StructField("role", StringType, nullable = false),
      StructField("name", StringType, nullable = true)
    ))
    val missingContentMessage = new GenericRowWithSchema(
      Array[Any]("user", null), // scalastyle:ignore null
      missingContentSchema
    )
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("missing-content", Seq(missingContentMessage))), 1),
      StructType(Seq(
        StructField("id", StringType, nullable = false),
        StructField("messages", ArrayType(missingContentSchema, containsNull = false), nullable = true)
      ))
    )
    val transformer = chat().setHandler { (_: CloseableHttpClient, _: HTTPRequestData) =>
      requestCount.add(1L)
      throw new AssertionError("HTTP must not run for malformed messages")
    }

    val result = transformer.transform(input).head()

    assert(requestCount.value == 0L)
    assert(result.getAs[Row]("error").getAs[String]("response") ==
      "messages[0].content must be a string or an array of content part objects")
    assert(Option(result.getAs[Row]("output")).isEmpty)
  }

  test("public column collisions honor Spark's resolver") {
    assert(!spark.conf.get("spark.sql.caseSensitive").toBoolean)
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("valid", Seq(message("user", Seq(contentPart("text", text = Some("hello"))))))
      ), 1),
      requestSchema()
    )

    Seq(
      ("messagesCol 'messages' must be different from outputCol 'MESSAGES'",
        chat().setOutputCol("MESSAGES")),
      ("messagesCol 'messages' must be different from errorCol 'MeSsAgEs'",
        chat().setErrorCol("MeSsAgEs")),
      ("outputCol 'result' must be different from errorCol 'RESULT'",
        chat().setOutputCol("result").setErrorCol("RESULT"))
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
}
