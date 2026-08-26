// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.{ErrorUtils, HTTPRequestData, HTTPResponseData, HTTPSchema}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructField, StructType}
import spray.json._

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

object OpenAIResponsesMultimodalTestData extends Serializable {
  def echoRequestBody(
      client: CloseableHttpClient,
      request: HTTPRequestData
  ): HTTPResponseData = {
    val requestBody = new String(request.entity.get.content, StandardCharsets.UTF_8)
    val response = JsObject(
      "id" -> JsString("resp_test"),
      "object" -> JsString("response"),
      "created_at" -> JsString("1"),
      "model" -> JsString("gpt-5.1"),
      "output" -> JsArray(JsObject(
        "content" -> JsArray(JsObject(
          "type" -> JsString("output_text"),
          "text" -> JsString(requestBody)
        )),
        "status" -> JsString("completed")
      )),
      "system_fingerprint" -> JsNull,
      "usage" -> JsNull
    )
    HTTPSchema.stringToResponse(response.compactPrint, 200, "OK")
  }
}

class OpenAIResponsesMultimodalSuite extends TestBase {

  private val contentPartSchema = StructType(Seq(
    StructField("type", StringType, nullable = false),
    StructField("text", StringType, nullable = true),
    StructField("image_url", StringType, nullable = true),
    StructField("detail", StringType, nullable = true),
    StructField("file_id", StringType, nullable = true),
    StructField("file_data", StringType, nullable = true),
    StructField("filename", StringType, nullable = true)
  ))

  private val structuredMessageSchema = StructType(Seq(
    StructField("role", StringType, nullable = false),
    StructField(
      "content",
      ArrayType(contentPartSchema, containsNull = true),
      nullable = true
    ),
    StructField("name", StringType, nullable = true)
  ))

  private val mapMessageSchema = StructType(Seq(
    StructField("role", StringType, nullable = false),
    StructField(
      "content",
      ArrayType(MapType(StringType, StringType, valueContainsNull = true), containsNull = true),
      nullable = true
    ),
    StructField("name", StringType, nullable = true)
  ))

  private val stringMessageSchema = StructType(Seq(
    StructField("role", StringType, nullable = false),
    StructField("content", StringType, nullable = true),
    StructField("name", StringType, nullable = true)
  ))

  private def requestSchema(messageSchema: StructType, includeError: Boolean = false): StructType = {
    val fields = Seq(
      StructField("id", StringType, nullable = false),
      StructField("messages", ArrayType(messageSchema, containsNull = true), nullable = true)
    )
    StructType(if (includeError) fields :+ StructField("error", ErrorUtils.ErrorSchema, nullable = true) else fields)
  }

  private def contentPart(
      partType: String,
      text: Option[String] = None,
      imageUrl: Option[String] = None,
      detail: Option[String] = None,
      fileId: Option[String] = None,
      fileData: Option[String] = None,
      filename: Option[String] = None
  ): Row =
    new GenericRowWithSchema(
      Array[Any](
        partType,
        text.orNull,
        imageUrl.orNull,
        detail.orNull,
        fileId.orNull,
        fileData.orNull,
        filename.orNull
      ),
      contentPartSchema
    )

  private def structuredMessage(role: String, content: Any): Row =
    new GenericRowWithSchema(
      Array[Any](role, content, null), // scalastyle:ignore null
      structuredMessageSchema
    )

  private def mapMessage(role: String, content: Seq[Map[String, String]]): Row =
    new GenericRowWithSchema(
      Array[Any](role, content, null), // scalastyle:ignore null
      mapMessageSchema
    )

  private def responses(): OpenAIResponses = new OpenAIResponses()
    .setUrl("https://example.services.ai.azure.com/openai/v1")
    .setDeploymentName("gpt-5.1")
    .setMessagesCol("messages")
    .setSubscriptionKey("unused")
    .setOutputCol("output")
    .setErrorCol("error")

  test("getStringEntity preserves struct-backed text, image, and file parts") {
    val entity = new OpenAIResponses().getStringEntity(
      Seq(structuredMessage("user", Seq(
        contentPart("input_text", text = Some("What is shown?")),
        contentPart(
          "input_image",
          imageUrl = Some("data:image/png;base64,AAA"),
          detail = Some("low")
        ),
        contentPart("input_image", fileId = Some("file-image")),
        contentPart(
          "input_file",
          fileData = Some("data:application/pdf;base64,AAA"),
          filename = Some("example.pdf")
        ),
        contentPart("input_file", fileId = Some("file-document"))
      ))),
      Map("model" -> "gpt-5.1")
    )

    val payload = EntityUtils.toString(entity).parseJson.asJsObject
    val JsArray(inputs) = payload.fields("input")
    val JsArray(content) = inputs.head.asJsObject.fields("content")

    assert(content.head.asJsObject ==
      JsObject("type" -> JsString("input_text"), "text" -> JsString("What is shown?")))
    assert(content(1).asJsObject == JsObject(
      "type" -> JsString("input_image"),
      "image_url" -> JsString("data:image/png;base64,AAA"),
      "detail" -> JsString("low")
    ))
    assert(content(2).asJsObject == JsObject(
      "type" -> JsString("input_image"),
      "file_id" -> JsString("file-image")
    ))
    assert(content(3).asJsObject == JsObject(
      "type" -> JsString("input_file"),
      "file_data" -> JsString("data:application/pdf;base64,AAA"),
      "filename" -> JsString("example.pdf")
    ))
    assert(content(4).asJsObject == JsObject(
      "type" -> JsString("input_file"),
      "file_id" -> JsString("file-document")
    ))
  }

  test("struct-backed Responses content survives the transformer and request path") {
    val requestBodies = spark.sparkContext.collectionAccumulator[String]
    val validMessage = structuredMessage("user", Seq(
      contentPart("input_text", text = Some("What is shown?")),
      contentPart(
        "input_image",
        imageUrl = Some("data:image/png;base64,AAA"),
        detail = Some("low")
      ),
      contentPart("input_image", imageUrl = Some("https://example.com/image.png"))
    ))
    val invalidMessage = structuredMessage("user", Seq(contentPart("input_image")))
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("valid", Seq(validMessage)),
        Row("invalid", Seq(invalidMessage))
      ), 1),
      requestSchema(structuredMessageSchema)
    )

    val transformer = responses().setHandler { (client: CloseableHttpClient, request: HTTPRequestData) =>
      requestBodies.add(new String(request.entity.get.content, StandardCharsets.UTF_8))
      OpenAIResponsesMultimodalTestData.echoRequestBody(client, request)
    }

    val transformed = transformer.transform(input)
    val rows = transformed.collect().map(row => row.getAs[String]("id") -> row).toMap

    assert(transformed.schema("messages").dataType == input.schema("messages").dataType)
    assert(transformer.transformSchema(input.schema)("messages").dataType == input.schema("messages").dataType)

    val valid = rows("valid")
    assert(Option(valid.getAs[Row]("error")).isEmpty)
    val response = ResponsesModelResponse.makeFromRowConverter(valid.getAs[Row]("output"))
    val payload = response.output.last.content.head.text.parseJson.asJsObject
    assert(requestBodies.value.asScala.size == 1)
    assert(requestBodies.value.asScala.head.parseJson == payload)
    assert(payload.fields.get("model").contains(JsString("gpt-5.1")))

    val JsArray(messages) = payload.fields("input")
    val JsArray(parts) = messages.head.asJsObject.fields("content")
    assert(parts.head.asJsObject ==
      JsObject("type" -> JsString("input_text"), "text" -> JsString("What is shown?")))
    assert(parts(1).asJsObject == JsObject(
      "type" -> JsString("input_image"),
      "image_url" -> JsString("data:image/png;base64,AAA"),
      "detail" -> JsString("low")
    ))
    assert(parts(2).asJsObject == JsObject(
      "type" -> JsString("input_image"),
      "image_url" -> JsString("https://example.com/image.png")
    ))

    val restoredParts = valid.getAs[scala.collection.Seq[Row]]("messages").head
      .getAs[scala.collection.Seq[Row]]("content")
    assert(restoredParts(1).getAs[String]("image_url") == "data:image/png;base64,AAA")

    val invalid = rows("invalid")
    assert(Option(invalid.getAs[Row]("output")).isEmpty)
    assert(invalid.getAs[Row]("error").getAs[String]("response") ==
      "messages[0].content[0] requires a non-empty string 'image_url' or 'file_id' field")
    assert(invalid.getAs[scala.collection.Seq[Row]]("messages").nonEmpty)
  }

  test("Responses resolves messagesCol with Spark case-insensitive semantics") {
    assert(!spark.conf.get("spark.sql.caseSensitive").toBoolean)
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("valid", Seq(structuredMessage("user", Seq(
          contentPart("input_text", text = Some("Describe."))
        ))))
      ), 1),
      requestSchema(structuredMessageSchema)
    ).withColumnRenamed("messages", "MESSAGES")
    val requestBodies = spark.sparkContext.collectionAccumulator[String]

    val result = responses()
      .setHandler { (client: CloseableHttpClient, request: HTTPRequestData) =>
        requestBodies.add(new String(request.entity.get.content, StandardCharsets.UTF_8))
        OpenAIResponsesMultimodalTestData.echoRequestBody(client, request)
      }
      .transform(input)
      .head()

    assert(Option(result.getAs[Row]("error")).isEmpty)
    assert(Option(result.getAs[Row]("output")).isDefined)
    assert(requestBodies.value.asScala.size == 1)
  }

  test("map-backed OpenAIPrompt Responses attachments keep their request shape") {
    val requestBodies = spark.sparkContext.collectionAccumulator[String]
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("valid", Seq(mapMessage("user", Seq(
        Map("type" -> "input_text", "text" -> "Summarize"),
        Map(
          "type" -> "input_file",
          "filename" -> "example.pdf",
          "file_data" -> "data:application/pdf;base64,AAA"
        )
      ))))), 1),
      requestSchema(mapMessageSchema)
    )

    val transformer = responses().setHandler { (client: CloseableHttpClient, request: HTTPRequestData) =>
      requestBodies.add(new String(request.entity.get.content, StandardCharsets.UTF_8))
      OpenAIResponsesMultimodalTestData.echoRequestBody(client, request)
    }
    val result = transformer.transform(input).head()

    assert(Option(result.getAs[Row]("error")).isEmpty)
    assert(requestBodies.value.asScala.size == 1)
    val payload = requestBodies.value.asScala.head.parseJson.asJsObject
    val JsArray(messages) = payload.fields("input")
    val JsArray(parts) = messages.head.asJsObject.fields("content")
    assert(parts.head.asJsObject ==
      JsObject("type" -> JsString("input_text"), "text" -> JsString("Summarize")))
    assert(parts(1).asJsObject == JsObject(
      "type" -> JsString("input_file"),
      "filename" -> JsString("example.pdf"),
      "file_data" -> JsString("data:application/pdf;base64,AAA")
    ))
  }

  test("invalid Responses content produces row errors without HTTP") {
    val requestCount = spark.sparkContext.longAccumulator
    val existingError = Row("upstream error", null) // scalastyle:ignore null
    val rows = Seq(
      Row("empty-content", Seq(structuredMessage("user", Seq.empty[Row])), null), // scalastyle:ignore null
      Row("empty-messages", Seq.empty[Row], null), // scalastyle:ignore null
      Row("existing-error", Seq(structuredMessage("user", Seq(contentPart("input_image")))), existingError),
      Row("null-content", Seq(structuredMessage("user", null)), null), // scalastyle:ignore null
      Row("null-messages", null, null), // scalastyle:ignore null
      Row("null-part", Seq(structuredMessage("user", Seq[Row](null))), null), // scalastyle:ignore null
      Row(
        "unknown-type",
        Seq(structuredMessage("user", Seq(contentPart("input_audio")))),
        null // scalastyle:ignore null
      ),
      Row("second-invalid-role", Seq(
        structuredMessage("system", Seq(contentPart("input_text", text = Some("hello")))),
        structuredMessage("", Seq(contentPart("input_text", text = Some("world"))))
      ), null) // scalastyle:ignore null
    )
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(rows, 1),
      requestSchema(structuredMessageSchema, includeError = true)
    )
    val transformer = responses().setHandler { (_: CloseableHttpClient, _: HTTPRequestData) =>
      requestCount.add(1L)
      throw new AssertionError("HTTP must not run for invalid Responses messages")
    }

    val output = transformer.transform(input).collect().map(row => row.getAs[String]("id") -> row).toMap

    assert(requestCount.value == 0L)
    assert(output("empty-content").getAs[Row]("error").getAs[String]("response") ==
      "messages[0].content must not be empty")
    assert(output("null-content").getAs[Row]("error").getAs[String]("response") ==
      "messages[0].content must be an array of content part objects")
    assert(output("null-part").getAs[Row]("error").getAs[String]("response") ==
      "messages[0].content[0] must be an object")
    assert(output("unknown-type").getAs[Row]("error").getAs[String]("response") ==
      "messages[0].content[0] has an unsupported type; supported types are " +
        "'input_text', 'input_image', and 'input_file'")
    assert(output("second-invalid-role").getAs[Row]("error").getAs[String]("response") ==
      "messages[1].role must be a non-empty string")
    assert(output("existing-error").getAs[Row]("error").getAs[String]("response") == "upstream error")
    assert(Option(output("empty-messages").getAs[Row]("error")).isEmpty)
    assert(Option(output("null-messages").getAs[Row]("error")).isEmpty)
    output.values.foreach(row => assert(Option(row.getAs[Row]("output")).isEmpty))
  }

  test("invalid Responses role schema produces a row error without HTTP") {
    val requestCount = spark.sparkContext.longAccumulator
    val invalidMessageSchema = StructType(Seq(
      StructField("role", IntegerType, nullable = false),
      StructField("content", ArrayType(contentPartSchema, containsNull = true), nullable = true)
    ))
    val invalidMessage = new GenericRowWithSchema(
      Array[Any](1, Seq(contentPart("input_text", text = Some("hello")))),
      invalidMessageSchema
    )
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("invalid-role", Seq(invalidMessage))), 1),
      requestSchema(invalidMessageSchema)
    )
    val transformer = responses().setHandler { (_: CloseableHttpClient, _: HTTPRequestData) =>
      requestCount.add(1L)
      throw new AssertionError("HTTP must not run for invalid Responses messages")
    }

    val result = transformer.transform(input).head()

    assert(requestCount.value == 0L)
    assert(result.getAs[Row]("error").getAs[String]("response") ==
      "messages[0].role must be a non-empty string")
    assert(Option(result.getAs[Row]("output")).isEmpty)
  }

  test("null string Responses content produces a generic row error without HTTP") {
    val requestCount = spark.sparkContext.longAccumulator
    val nullContentMessage = new GenericRowWithSchema(
      Array[Any]("user", null, null), // scalastyle:ignore null
      stringMessageSchema
    )
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("null-content", Seq(nullContentMessage))), 1),
      requestSchema(stringMessageSchema)
    )
    val transformer = responses().setHandler { (_: CloseableHttpClient, _: HTTPRequestData) =>
      requestCount.add(1L)
      throw new AssertionError("HTTP must not run for invalid Responses messages")
    }

    val result = transformer.transform(input).head()

    assert(requestCount.value == 0L)
    assert(result.getAs[Row]("error").getAs[String]("response") ==
      "messages[0].content must be a string or an array of content part objects")
    assert(Option(result.getAs[Row]("output")).isEmpty)
  }

  test("Responses preserves case-insensitive upstream error columns") {
    assert(!spark.conf.get("spark.sql.caseSensitive").toBoolean)
    val existingError = Row("upstream error", null) // scalastyle:ignore null
    val invalidMessage = structuredMessage("user", Seq(
      contentPart("input_image", imageUrl = Some(""))
    ))
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("invalid", Seq(invalidMessage), existingError)
      ), 1),
      StructType(requestSchema(structuredMessageSchema).fields :+
        StructField("UPSTREAM_ERROR", ErrorUtils.ErrorSchema, nullable = true))
    )
    val transformer = responses()
      .setErrorCol("upstream_error")
      .setHandler { (_: CloseableHttpClient, _: HTTPRequestData) =>
        throw new AssertionError("HTTP must not run for malformed messages")
      }

    val result = transformer.transform(input).head()

    assert(result.getAs[Row]("upstream_error").getAs[String]("response") == "upstream error")
    assert(Option(result.getAs[Row]("output")).isEmpty)
  }

  test("Responses scratch columns preserve resolver-colliding input columns") {
    assert(!spark.conf.get("spark.sql.caseSensitive").toBoolean)
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(
          "valid",
          Seq(structuredMessage("user", Seq(contentPart("input_text", text = Some("hello"))))),
          "keep original",
          "keep original suffix",
          "keep validation",
          "keep validation suffix"
        )
      ), 1),
      StructType(requestSchema(structuredMessageSchema).fields ++ Seq(
        StructField("ORIGINALMESSAGES", StringType, nullable = false),
        StructField("ORIGINALMESSAGES_1", StringType, nullable = false),
        StructField("RESPONSESMESSAGEVALIDATIONERROR", StringType, nullable = false),
        StructField("RESPONSESMESSAGEVALIDATIONERROR_1", StringType, nullable = false)
      ))
    )
    val result = responses()
      .setHandler(OpenAIResponsesMultimodalTestData.echoRequestBody _)
      .transform(input)
      .head()

    assert(result.getAs[String]("ORIGINALMESSAGES") == "keep original")
    assert(result.getAs[String]("ORIGINALMESSAGES_1") == "keep original suffix")
    assert(result.getAs[String]("RESPONSESMESSAGEVALIDATIONERROR") == "keep validation")
    assert(result.getAs[String]("RESPONSESMESSAGEVALIDATIONERROR_1") == "keep validation suffix")
  }

  test("Responses replaces non-error upstream columns with validation errors") {
    assert(!spark.conf.get("spark.sql.caseSensitive").toBoolean)
    val invalidMessage = structuredMessage("user", Seq(
      contentPart("input_image", imageUrl = Some(""))
    ))
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("invalid", Seq(invalidMessage), "not an error struct")
      ), 1),
      StructType(requestSchema(structuredMessageSchema).fields :+
        StructField("UPSTREAM_ERROR", StringType, nullable = true))
    )
    val transformer = responses()
      .setErrorCol("upstream_error")
      .setHandler { (_: CloseableHttpClient, _: HTTPRequestData) =>
        throw new AssertionError("HTTP must not run for malformed messages")
      }

    val result = transformer.transform(input).head()

    assert(result.schema("upstream_error").dataType == ErrorUtils.ErrorSchema)
    assert(result.getAs[Row]("upstream_error").getAs[String]("response") ==
      "messages[0].content[0] requires a non-empty string 'image_url' or 'file_id' field")
    assert(Option(result.getAs[Row]("output")).isEmpty)
  }

  test("Responses public column collisions are rejected") {
    assert(!spark.conf.get("spark.sql.caseSensitive").toBoolean)
    val input = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("valid", Seq(structuredMessage("user", Seq(
          contentPart("input_text", text = Some("hello"))
        ))))
      ), 1),
      requestSchema(structuredMessageSchema)
    )

    Seq(
      ("messagesCol 'messages' must be different from outputCol 'MESSAGES'",
        responses().setOutputCol("MESSAGES")),
      ("messagesCol 'messages' must be different from errorCol 'MeSsAgEs'",
        responses().setErrorCol("MeSsAgEs")),
      ("outputCol 'result' must be different from errorCol 'RESULT'",
        responses().setOutputCol("result").setErrorCol("RESULT"))
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
