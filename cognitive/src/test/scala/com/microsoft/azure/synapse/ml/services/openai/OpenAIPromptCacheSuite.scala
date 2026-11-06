// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.HTTPRequestData
import com.microsoft.azure.synapse.ml.param.AnyJsonFormat.anyFormat
import com.microsoft.azure.synapse.ml.services.HasCognitiveServiceInput
import com.microsoft.azure.synapse.ml.services.aifoundry.AIFoundryChatCompletion
import org.apache.http.entity.AbstractHttpEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, BooleanType, MapType, StringType, StructType}
import spray.json.DefaultJsonProtocol._
import spray.json._

class OpenAIPromptCacheSuite extends TestBase {

  private type CacheStage = HasOpenAITextParams with HasCognitiveServiceInput

  private val messageSchema = new StructType().add("role", StringType).add("content", StringType)
  private val requestSchema = new StructType()
    .add("messages", ArrayType(messageSchema))
    .add("cache_options", MapType(StringType, StringType))

  private val prepareEntity = classOf[HasCognitiveServiceInput].getDeclaredMethod("prepareEntity")
  prepareEntity.setAccessible(true)

  private def stages: Seq[CacheStage] = Seq(
    new OpenAIChatCompletion().setMessagesCol("messages"),
    new OpenAIResponses().setMessagesCol("messages"),
    new AIFoundryChatCompletion().setModel("gpt-6-luna").setMessagesCol("messages"),
    new OpenAIPrompt().setModel("gpt-6-luna")
      .setUrl("https://example.services.ai.azure.com").setMessagesCol("messages"),
    new OpenAIPrompt().setMessagesCol("messages").setApiType("chat_completions"),
    new OpenAIPrompt().setMessagesCol("messages").setApiType("responses")
  ).map(_.setDeploymentName("gpt-6-luna"))

  private def requestRow(options: Option[Map[String, Any]] = None): Row = {
    val message = new GenericRowWithSchema(Array[Any]("user", "hello"), messageSchema)
    new GenericRowWithSchema(Array[Any](Seq(message), options.orNull), requestSchema)
  }

  private def payload(stage: CacheStage, row: Row): JsObject = {
    val build = prepareEntity.invoke(stage).asInstanceOf[Row => Option[AbstractHttpEntity]]
    EntityUtils.toString(build(row).get).parseJson.asJsObject
  }

  test("unset options preserve the request and scalar options are forwarded without policy validation") {
    val options = Seq(
      Map.empty[String, Any],
      Map[String, Any]("mode" -> "explicit", "ttl" -> "30m"),
      Map[String, Any]("ttl" -> "12h", "prewarm" -> true),
      Map[String, Any]("mode" -> "provider-decides", "ttl" -> "not-a-cache-lifetime")
    )
    stages.foreach { stage =>
      val original = payload(stage, requestRow()).fields
      assert(!original.contains("prompt_cache_options"))
      options.foreach { value =>
        stage.setPromptCacheOptions(value)
        assert(payload(stage, requestRow()).fields == original.updated("prompt_cache_options", value.toJson))
      }
    }
  }

  test("column options replace scalar binding and omit null rows without validating values") {
    stages.foreach { stage =>
      stage.setPromptCacheOptions(Map("mode" -> "implicit")).setPromptCacheOptionsCol("cache_options")
      assert(!payload(stage, requestRow()).fields.contains("prompt_cache_options"))
      Seq(
        Map[String, Any]("mode" -> "explicit", "ttl" -> "30m"),
        Map[String, Any]("ttl" -> "not-a-cache-lifetime")
      ).foreach { options =>
        assert(payload(stage, requestRow(Some(options))).fields("prompt_cache_options") == options.toJson)
      }
    }
  }

  test("Responses text image and file inputs preserve explicit breakpoint objects") {
    val partSchema = new StructType()
      .add("type", StringType).add("text", StringType).add("image_url", StringType)
      .add("file_id", StringType).add("prompt_cache_breakpoint", MapType(StringType, StringType))
    val messagesSchema = new StructType().add("role", StringType).add("content", ArrayType(partSchema))
    val breakpoint = Map("mode" -> "explicit")
    val parts = Seq(
      Array[Any]("input_text", "Reusable instructions", null, null, breakpoint), // scalastyle:ignore null
      Array[Any]("input_image", null, "https://example.org/image.png", null, breakpoint), // scalastyle:ignore null
      Array[Any]("input_file", null, null, "file-example", breakpoint) // scalastyle:ignore null
    ).map(values => new GenericRowWithSchema(values, partSchema))
    val message = new GenericRowWithSchema(Array[Any]("developer", parts), messagesSchema)
    val response = new OpenAIResponses()
    val body = EntityUtils.toString(response.getStringEntity(Seq(message), Map.empty)).parseJson.asJsObject
    val encodedParts = body.fields("input").asInstanceOf[JsArray].elements.head.asJsObject
      .fields("content").asInstanceOf[JsArray].elements
    assert(encodedParts.length == parts.length)
    encodedParts.foreach { part =>
      assert(part.asJsObject.fields("prompt_cache_breakpoint") == breakpoint.toJson)
    }
  }

  test("SQL decimal options retain exact JSON numbers on all request routes") {
    val decimal = "12345678901234567890.123456789012345678"
    val options = spark.sql(
      s"SELECT named_struct('future_number', CAST('$decimal' AS DECIMAL(38,18))) AS options")
      .head().getStruct(0)
    val schema = new StructType().add("messages", ArrayType(messageSchema)).add("cache_options", options.schema)
    val message = new GenericRowWithSchema(Array[Any]("user", "hello"), messageSchema)
    val row = new GenericRowWithSchema(Array[Any](Seq(message), options), schema)
    val expected = JsObject("future_number" -> JsNumber(BigDecimal(decimal)))
    stages.foreach { stage =>
      assert(payload(stage.setPromptCacheOptionsCol("cache_options"), row).fields("prompt_cache_options") == expected)
    }
  }

  test("scalar decimal cache options retain precision after save and load") {
    Seq("12345678901234567890.123456789012345678", "9223372036854775808", "18446744073709551616")
      .map(BigDecimal(_)).zipWithIndex.foreach { case (decimal, decimalIndex) =>
        val expected = JsObject("future_number" -> JsNumber(decimal))
        Seq[Any](decimal, decimal.bigDecimal).zipWithIndex.foreach { case (value, typeIndex) =>
          val prompt = new OpenAIPrompt().setMessagesCol("messages").setApiType("responses")
            .setPromptCacheOptions(Map("future_number" -> value))
          assert(payload(prompt, requestRow()).fields("prompt_cache_options") == expected)
          val path = tmpDir.resolve(s"decimal-cache-options-$decimalIndex-$typeIndex").toString
          prompt.write.save(path)
          val restored = OpenAIPrompt.load(path)
          assert(payload(restored, requestRow()).fields("prompt_cache_options") == expected)
          val resavedPath = tmpDir.resolve(s"resaved-decimal-cache-options-$decimalIndex-$typeIndex").toString
          restored.write.save(resavedPath)
          assert(payload(OpenAIPrompt.load(resavedPath), requestRow()).fields("prompt_cache_options") == expected)
        }
      }
  }

  test("struct options retain nested JSON values without adding cache-policy validation") {
    val nestedSchema = new StructType().add("enabled", BooleanType)
    val optionsSchema = new StructType().add("ttl", StringType).add("prewarm", BooleanType)
      .add("future", ArrayType(MapType(StringType, nestedSchema)))
    val schema = new StructType().add("messages", ArrayType(messageSchema)).add("cache_options", optionsSchema)
    val nested = new GenericRowWithSchema(Array[Any](false), nestedSchema)
    val options = new GenericRowWithSchema(
      Array[Any]("provider-decides", true, Seq(Map("entry" -> nested))), optionsSchema)
    val message = new GenericRowWithSchema(Array[Any]("user", "hello"), messageSchema)
    val row = new GenericRowWithSchema(Array[Any](Seq(message), options), schema)
    val expected =
      """{"ttl":"provider-decides","prewarm":true,"future":[{"entry":{"enabled":false}}]}""".parseJson
    stages.foreach { stage =>
      assert(payload(stage.setPromptCacheOptionsCol("cache_options"), row).fields("prompt_cache_options") == expected)
    }
  }

  test("Chat text and image inputs preserve breakpoint values for service validation") {
    val imageSchema = new StructType().add("url", StringType)
    val partSchema = new StructType().add("type", StringType).add("text", StringType)
      .add("image_url", imageSchema).add("prompt_cache_breakpoint", MapType(StringType, StringType))
    val messagesSchema = new StructType().add("role", StringType).add("content", ArrayType(partSchema))
    val breakpoint = Map("mode" -> "provider-decides", "ttl" -> "not-a-cache-lifetime")
    val image = new GenericRowWithSchema(Array[Any]("https://example.org/image.png"), imageSchema)
    val parts = Seq(
      Array[Any]("text", "Reusable instructions", null, breakpoint), // scalastyle:ignore null
      Array[Any]("image_url", null, image, breakpoint) // scalastyle:ignore null
    ).map(values => new GenericRowWithSchema(values, partSchema))
    val message = new GenericRowWithSchema(Array[Any]("developer", parts), messagesSchema)
    val body = EntityUtils.toString(new OpenAIChatCompletion()
      .getStringEntity(Seq(message), Map.empty)).parseJson.asJsObject
    val encodedParts = body.fields("messages").asInstanceOf[JsArray].elements.head.asJsObject
      .fields("content").asInstanceOf[JsArray].elements
    assert(encodedParts.length == parts.length)
    encodedParts.foreach { part =>
      assert(part.asJsObject.fields("prompt_cache_breakpoint") == breakpoint.toJson)
    }
  }

  for ((partType, valueField, value) <- Seq(
    ("input_text", "text", "hello"),
    ("input_image", "image_url", "https://example.org/image.png"),
    ("input_file", "file_id", "file-example"),
    ("text", "text", "hello"),
    ("image_url", "image_url", "https://example.org/image.png")
  )) {
    test(s"$partType scalar breakpoints fail locally for struct and map content") {
      for (mapBacked <- Seq(false, true)) {
        val imageSchema = new StructType().add("url", StringType)
        val nestedImage = partType == "image_url" && !mapBacked
        val partSchema = new StructType().add("type", StringType)
          .add(valueField, if (nestedImage) imageSchema else StringType)
          .add("prompt_cache_breakpoint", StringType)
        val contentType = if (mapBacked) MapType(StringType, StringType) else partSchema
        val messagesSchema = new StructType().add("role", StringType).add("content", ArrayType(contentType))
        val marker = """{"mode":"explicit"}"""
        val part = if (mapBacked) {
          Map("type" -> partType, valueField -> value, "prompt_cache_breakpoint" -> marker)
        } else {
          val encodedValue = if (nestedImage) new GenericRowWithSchema(Array[Any](value), imageSchema) else value
          new GenericRowWithSchema(Array[Any](partType, encodedValue, marker), partSchema)
        }
        val message = new GenericRowWithSchema(Array[Any]("user", Seq(part)), messagesSchema)
        val stage: OpenAIServicesBase with HasMessagesInput = if (partType.startsWith("input_")) {
          new OpenAIResponses()
        } else {
          new OpenAIChatCompletion()
        }
        val calls = spark.sparkContext.longAccumulator
        stage.setMessagesCol("messages").setUrl("https://example.openai.azure.com/openai/v1/")
          .setSubscriptionKey("unused").setOutputCol("output").setErrorCol("error")
          .setHandler { (_: CloseableHttpClient, _: HTTPRequestData) =>
            calls.add(1L)
            throw new AssertionError("Invalid breakpoint must not reach HTTP")
          }
        val input = spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(Row(Seq(message))), 1),
          new StructType().add("messages", ArrayType(messagesSchema)))
        val rows = stage.transform(input).select("output", "error").collect()
        assert(rows.length == 1)
        assert(rows.head.isNullAt(0))
        val error = rows.head.getAs[Row]("error").getAs[String]("response")
        assert(error.contains("prompt_cache_breakpoint"))
        assert(error.contains("nested struct"))
        assert(calls.value == 0L)
      }
    }
  }
}
