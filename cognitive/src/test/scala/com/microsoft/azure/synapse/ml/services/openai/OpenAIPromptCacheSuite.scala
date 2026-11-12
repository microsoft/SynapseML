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
import org.apache.spark.ml.Serializer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col, from_json, to_json}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, FloatType, MapType, StringType, StructType}
import spray.json.DefaultJsonProtocol._
import spray.json._

class OpenAIPromptCacheSuite extends TestBase {

  import spark.implicits._

  private type CacheStage = HasOpenAIModelParams with HasCognitiveServiceInput
  private type PersistentCacheStage = OpenAIServicesBase with CacheStage

  private val persistentStages:
    Seq[(String, () => PersistentCacheStage, MLReadable[_ <: PersistentCacheStage])] = Seq(
    ("Chat", () => new OpenAIChatCompletion().setMessagesCol("messages"), OpenAIChatCompletion),
    ("Responses", () => new OpenAIResponses().setMessagesCol("messages"), OpenAIResponses),
    ("Foundry", () => new AIFoundryChatCompletion().setModel("custom-model")
      .setMessagesCol("messages"), AIFoundryChatCompletion)
  )

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

  private def breakpointRow(partType: String, breakpoint: String): Row = spark.sql(
    s"""SELECT array(named_struct('role', 'user', 'content', array(named_struct(
       |'type', '$partType', 'text', 'hello', 'prompt_cache_breakpoint', $breakpoint)))) AS messages""".stripMargin)
    .head()

  test("Chat log probability counts use a boolean switch and top_logprobs") {
    val stage = new OpenAIChatCompletion().setMessagesCol("messages").setLogProbs(3)
    val body = payload(stage, requestRow()).fields
    assert(body("logprobs") == JsTrue)
    assert(body("top_logprobs") == JsNumber(3))
  }

  test("Prompt forwards explicit Chat storage in the request") {
    val stage = new OpenAIPrompt().setMessagesCol("messages").setStore(true)
    assert(payload(stage, requestRow()).fields.get("store").contains(JsBoolean(true)))
  }

  test("Prompt preserves no-op Chat storage through copy save and load") {
    spark
    Seq(false, true).foreach { explicitFalse =>
      val stage = new OpenAIPrompt().setApiType("chat_completions").setMessagesCol("messages")
      if (explicitFalse) stage.setStore(false)
      val path = tmpDir.resolve(s"chat-no-op-store-$explicitFalse").toString
      stage.write.save(path)
      Seq(stage, stage.copy(ParamMap.empty).asInstanceOf[OpenAIPrompt], OpenAIPrompt.load(path))
        .foreach(candidate => assert(!payload(candidate, requestRow()).fields.contains("store")))
    }
    val responses = new OpenAIPrompt().setApiType("responses").setMessagesCol("messages")
    assert(payload(responses, requestRow()).fields("store") == JsFalse)
  }

  test("Responses null storage columns retain the no-storage default on both routes") {
    val schema = new StructType().add("messages", ArrayType(messageSchema)).add("persist", BooleanType)
    val message = new GenericRowWithSchema(Array[Any]("user", "hello"), messageSchema)
    Seq[CacheStage](
      new OpenAIResponses().setMessagesCol("messages").setStoreCol("persist"),
      new OpenAIPrompt().setApiType("responses").setMessagesCol("messages").setStoreCol("persist")
    ).foreach { stage =>
      Seq(None, Some(false), Some(true)).foreach { flag =>
        val row = new GenericRowWithSchema(Array[Any](Seq(message), flag.map(Boolean.box).orNull), schema)
        val expected = JsBoolean(flag.contains(true))
        assert(payload(stage, row).fields("store") == expected)
      }
    }
  }

  test("Foundry Prompt preserves no-op storage through copy save and load") {
    spark
    Seq(false, true).foreach { explicitFalse =>
      val stage = new OpenAIPrompt().setModel("foundry-model")
        .setUrl("https://example.services.ai.azure.com").setMessagesCol("messages")
      if (explicitFalse) stage.setStore(false)
      val path = tmpDir.resolve(s"foundry-store-$explicitFalse").toString
      stage.write.save(path)
      Seq(stage, stage.copy(org.apache.spark.ml.param.ParamMap.empty).asInstanceOf[OpenAIPrompt],
        OpenAIPrompt.load(path)).foreach { candidate =>
        val body = payload(candidate, requestRow()).fields
        assert(body("model") == JsString("foundry-model"))
        assert(!body.contains("store"))
      }
    }
    val row = spark.sql(
      "SELECT array(named_struct('role', 'user', 'content', 'hello')) AS messages, true AS persist").head()
    Seq(false, true).foreach { column =>
      val stage = new OpenAIPrompt().setModel("foundry-model")
        .setUrl("https://example.services.ai.azure.com").setMessagesCol("messages")
      if (column) stage.setStoreCol("persist") else stage.setStore(true)
      val path = tmpDir.resolve(s"foundry-unsupported-store-$column").toString
      stage.write.save(path)
      Seq(stage, OpenAIPrompt.load(path)).foreach { candidate =>
        val error = intercept[IllegalArgumentException] { payload(candidate, row) }
        assert(error.getMessage.contains("store"))
        assert(error.getMessage.contains("Foundry"))
      }
    }
  }

  test("Responses rejects unsupported inherited seed rather than silently dropping it") {
    val stage = new OpenAIResponses().setSeed(123)
    val error = intercept[IllegalArgumentException] {
      stage.getOptionalParams(requestRow())
    }
    assert(error.getMessage.contains("seed"))
    assert(error.getMessage.contains("Responses"))
  }

  test("Responses rejects an unsupported global seed while Chat retains it") {
    val previous = OpenAIDefaults.getSeed
    try {
      OpenAIDefaults.setSeed(42)
      val chat = new OpenAIChatCompletion().setMessagesCol("messages")
      chat.transferGlobalParamsToParamMap()
      assert(payload(chat, requestRow()).fields("seed") == JsNumber(42))
      val responses = new OpenAIResponses().setMessagesCol("messages")
      val prompt = new OpenAIPrompt().setApiType("responses").setMessagesCol("messages")
      responses.transferGlobalParamsToParamMap()
      prompt.transferGlobalParamsToParamMap()
      Seq[CacheStage](responses, prompt).foreach { stage =>
        val error = intercept[IllegalArgumentException] { payload(stage, requestRow()) }
        assert(error.getMessage.contains("seed"))
        assert(error.getMessage.contains("Responses"))
      }
    } finally {
      previous match {
        case Some(value) => OpenAIDefaults.setSeed(value)
        case None => OpenAIDefaults.resetSeed()
      }
    }
  }

  test("Responses preserves cache writes TTL accounting and cache diagnostics in parsed output") {
    val json = """{"id":"resp_cache","status":"completed","output":[],"prompt_cache_key":"batch",
      |"prompt_cache_diagnostics":{"type":"cache_miss","reason":"input_changed",
      |"cache_missed_tokens":1000,"comparison_reusable_tokens":4000},
      |"prompt_cache_options":{"mode":"explicit","ttl":"12h","comparison_response_id":"resp_previous"},
      |"usage":{"input_tokens":12000,"output_tokens":0,"total_tokens":12000,
      |"input_tokens_details":{"cached_tokens":4000,"cache_write_tokens":7000,
      |"cache_write_tokens_details":{"ttl_30m_tokens":3000,"ttl_12h_tokens":4000}},
      |"output_tokens_details":{"reasoning_tokens":0}}}""".stripMargin
    val stage = new OpenAIResponses()
    val output = Seq(json).toDF("json")
      .select(from_json(col("json"), stage.responseDataType.asInstanceOf[StructType]).alias("response"))
    val parsed = output.select(to_json(col("response"))).head().getString(0).parseJson.asJsObject
    val expected = json.parseJson.asJsObject
    Seq("prompt_cache_key", "prompt_cache_diagnostics", "prompt_cache_options", "usage").foreach { key =>
      assert(parsed.fields.get(key).contains(expected.fields(key)), key)
    }
    assert(!stage.isContentFiltered(output.head().getAs[Row]("response")))
    assert(stage.isContentFiltered(null)) // scalastyle:ignore null
    val normalized = output.select(UsageUtils.normalizeResponsesDetailed(col("response.usage")).alias("usage"))
    assert(normalized.schema("usage").dataType == UsageUtils.UsageStructType)
    val details = normalized.head().getStruct(0).getAs[Map[String, Long]]("input_token_details")
    assert(details == Map("cached_tokens" -> 4000L, "cache_write_tokens" -> 7000L,
      "cache_write_tokens_details.ttl_30m_tokens" -> 3000L,
      "cache_write_tokens_details.ttl_12h_tokens" -> 4000L))
  }

  test("model controls use API field names through direct and Prompt routes") {
    stages.foreach { stage =>
      stage.setPromptCacheKey("batch").setPromptCacheRetention("24h").setSafetyIdentifier("opaque-id")
        .setServiceTier("priority").setMetadata(Map("job" -> "example")).setTopLogProbs(0)
      val body = payload(stage, requestRow()).fields
      assert(body("prompt_cache_key") == JsString("batch"))
      assert(body("prompt_cache_retention") == JsString("24h"))
      assert(body("safety_identifier") == JsString("opaque-id"))
      assert(body("service_tier") == JsString("priority"))
      assert(body("metadata") == JsObject("job" -> JsString("example")))
      assert(body("top_logprobs") == JsNumber(0))
      if (body.contains("input")) {
        assert(body("include") == JsArray(JsString("message.output_text.logprobs")))
        assert(!body.contains("logprobs"))
      } else {
        assert(body("logprobs") == JsTrue)
      }
    }
  }

  test("model controls retain column precedence and null omission") {
    val columns = spark.sql(
      """SELECT array(named_struct('role', 'user', 'content', 'hello')) AS messages,
        |'row-key' AS cache_key, 'default' AS tier, map('job', 'row') AS metadata,
        |2 AS probability_count, array('END', 'STOP') AS stops, map('42', -10) AS bias,
        |named_struct('context', 'all_turns', 'summary', 'auto', 'mode', 'standard') AS reasoning,
        |array('reasoning.encrypted_content') AS included, 'disabled' AS truncation""".stripMargin).head()
    Seq[CacheStage](
      new OpenAIChatCompletion().setMessagesCol("messages").setStopSequencesCol("stops").setLogitBiasCol("bias"),
      new OpenAIResponses().setMessagesCol("messages").setReasoningCol("reasoning")
        .setIncludeCol("included").setTruncationCol("truncation"),
      new OpenAIPrompt().setMessagesCol("messages").setApiType("responses").setReasoningCol("reasoning")
        .setIncludeCol("included").setTruncationCol("truncation")
    ).foreach { stage =>
      stage.setPromptCacheKey("scalar").setPromptCacheKeyCol("cache_key").setServiceTierCol("tier")
        .setMetadataCol("metadata").setTopLogProbsCol("probability_count")
      val body = payload(stage, columns).fields
      assert(body("prompt_cache_key") == JsString("row-key"))
      assert(body("service_tier") == JsString("default"))
      assert(body("metadata") == JsObject("job" -> JsString("row")))
      assert(body("top_logprobs") == JsNumber(2))
      if (body.contains("input")) {
        assert(body("reasoning") ==
          JsObject("context" -> JsString("all_turns"), "summary" -> JsString("auto"), "mode" -> JsString("standard")))
        assert(body("include") ==
          JsArray(JsString("reasoning.encrypted_content"), JsString("message.output_text.logprobs")))
        assert(body("truncation") == JsString("disabled"))
      } else {
        assert(body("stop") == JsArray(JsString("END"), JsString("STOP")))
        assert(body("logit_bias") == JsObject("42" -> JsNumber(-10)))
      }
      val values = columns.toSeq.updated(columns.fieldIndex("cache_key"), null) // scalastyle:ignore null
      val nullKey = new GenericRowWithSchema(values.toArray, columns.schema)
      assert(!payload(stage, nullKey).fields.contains("prompt_cache_key"))
    }
  }

  test("Responses log probability switches preserve and deduplicate explicit include entries") {
    Seq[CacheStage](
      new OpenAIResponses().setMessagesCol("messages").setLogProbsEnabled(true),
      new OpenAIResponses().setMessagesCol("messages").setLogProbs(2),
      new OpenAIPrompt().setApiType("responses").setMessagesCol("messages").setTopLogProbs(2)
    ).foreach { stage =>
      stage.setInclude(Seq("reasoning.encrypted_content", "message.output_text.logprobs"))
      assert(payload(stage, requestRow()).fields("include") ==
        JsArray(JsString("reasoning.encrypted_content"), JsString("message.output_text.logprobs")))
    }
  }

  test("new model controls survive Prompt copy save and load for both APIs") {
    spark
    Seq("chat_completions", "responses").foreach { api =>
      val stage = new OpenAIPrompt().setMessagesCol("messages").setApiType(api)
        .setPromptCacheKey("key").setPromptCacheRetention("24h").setServiceTier("default")
        .setSafetyIdentifier("opaque-id").setMetadata(Map("job" -> "roundtrip"))
      if (api == "responses") {
        stage.setReasoning(Map("effort" -> "max", "context" -> "all_turns", "summary" -> "auto",
          "future_limits" -> Seq(Double.MaxValue, Float.MaxValue, BigDecimal("1e400")),
          "future_null" -> null)) // scalastyle:ignore null
          .setInclude(Seq("reasoning.encrypted_content")).setTruncation("disabled").setTopLogProbs(2)
      } else {
        stage.setLogProbsEnabled(false).setStopSequences(Seq("END", "STOP")).setLogitBias(Map("42" -> -10))
      }
      val expected = payload(stage, requestRow())
      val path = tmpDir.resolve(s"model-controls-$api").toString
      stage.copy(org.apache.spark.ml.param.ParamMap.empty).asInstanceOf[OpenAIPrompt].write.save(path)
      assert(payload(OpenAIPrompt.load(path), requestRow()) == expected)
    }
  }

  persistentStages.foreach { case (name, create, reader) =>
    test(s"low-level $name cache options survive copy and reload with Java deserialization disabled") {
      val session = spark.newSession()
      session.conf.set(Serializer.LegacyObjectDeserializationConfig, "false")
      Seq("unset", "scalar", "column").foreach { binding =>
        val stage = create().setUrl("https://example.openai.azure.com/openai/v1/")
        val options = Map[String, Any]("mode" -> "implicit", "ttl" -> "30m",
          "optional" -> Option.empty[String].orNull)
        if (binding == "scalar") stage.setPromptCacheOptions(options)
        if (binding == "column") stage.setPromptCacheOptionsCol("cache_options")
        val input = requestRow(Some(options))
        val expected = payload(stage, input)
        val path = tmpDir.resolve(s"safe-$name-$binding").toString
        stage.copy(ParamMap.empty).asInstanceOf[PersistentCacheStage].write.session(session).save(path)
        val restored = reader.read.session(session).load(path)
        assert(!restored.isSet(restored.handler))
        assert(restored.isSet(restored.deploymentName) == stage.isSet(stage.deploymentName))
        assert(payload(restored, input) == expected)
        val resaved = tmpDir.resolve(s"safe-$name-$binding-resaved").toString
        restored.write.session(session).save(resaved)
        assert(payload(reader.read.session(session).load(resaved), input) == expected)
      }
    }

    test(s"low-level $name explicitly saved handlers remain guarded on reload") {
      val session = spark.newSession()
      session.conf.set(Serializer.LegacyObjectDeserializationConfig, "false")
      Seq(false, true).foreach { custom =>
        val stage = create()
        if (custom) {
          stage.setHandler { (_: CloseableHttpClient, _: HTTPRequestData) =>
            throw new IllegalStateException("A saved custom handler must not run while loading")
          }
        } else {
          // Old writers saved this same UDF even when the handler was only a constructor default.
          stage.setHandler(stage.getOrDefault(stage.handler))
        }
        val path = tmpDir.resolve(s"guarded-$name-$custom").toString
        stage.write.session(session).save(path)
        val error = intercept[SecurityException] {
          reader.read.session(session).load(path)
        }
        assert(error.getMessage.contains("UserDefinedFunction"))
        assert(error.getMessage.contains(Serializer.LegacyObjectDeserializationConfig))
      }
    }
  }

  test("conflicting controls and API-specific parameters fail explicitly") {
    val invalid: Seq[(HasOpenAIModelParams, String)] = Seq(
      new OpenAIChatCompletion().setLogProbs(3).setTopLogProbs(2) -> "logProbs",
      new OpenAIChatCompletion().setLogProbsEnabled(false).setTopLogProbs(2) -> "logProbsEnabled",
      new OpenAIChatCompletion().setStop("END").setStopSequences(Seq("STOP")) -> "stop",
      new OpenAIChatCompletion().setReasoning(Map("context" -> "all_turns")) -> "Chat Completions",
      new OpenAIChatCompletion().setInclude(Seq("reasoning.encrypted_content")) -> "Chat Completions",
      new OpenAIChatCompletion().setEcho(true) -> "echo",
      new OpenAIResponses().setStopSequences(Seq("STOP")) -> "Responses",
      new OpenAIResponses().setLogProbsEnabled(false).setInclude(Seq("message.output_text.logprobs")) -> "include",
      new OpenAIResponses().setReasoning(Map("effort" -> "high")).setReasoningEffort("low") -> "reasoning.effort"
    )
    invalid.foreach { case (stage, expected) =>
      val error = intercept[IllegalArgumentException] { stage.getOptionalParams(requestRow()) }
      assert(error.getMessage.contains(expected))
    }
    val nullable = new OpenAIResponses()
      .setReasoning(Map("effort" -> null, "context" -> "all_turns")) // scalastyle:ignore null
      .setReasoningEffort("high")
    assert(nullable.getOptionalParams(requestRow())("reasoning") ==
      Map("context" -> "all_turns", "effort" -> "high"))
  }

  test("Responses included token probabilities and reasoning summaries survive JSON parsing") {
    val json = """{"output":[{"type":"reasoning","id":"rs_example","encrypted_content":"opaque",
      |"summary":[{"type":"summary_text","text":"Brief summary"}]},
      |{"type":"message","role":"assistant","status":"completed","content":[
      |{"type":"output_text","text":"ok","logprobs":[{"token":"ok","bytes":[111,107],
      |"logprob":-0.1,"top_logprobs":[]}]}]}]}""".stripMargin
    val parsed = Seq(json).toDF("json")
      .select(to_json(from_json(col("json"), OpenAIResponseSchemas.Responses)))
      .head().getString(0).parseJson.asJsObject
    assert(parsed.fields("output") == json.parseJson.asJsObject.fields("output"))
  }

  test("Chat parsed output preserves log probabilities and optional cache writes without inventing zeros") {
    val json = """{"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop",
      |"logprobs":{"content":[{"token":"ok","bytes":[111,107],"logprob":-0.1,
      |"top_logprobs":[{"token":"no","bytes":[110,111],"logprob":-2.0}]}]}}],
      |"service_tier":"default","metadata":{"job":"example"},
      |"usage":{"prompt_tokens":12,"completion_tokens":1,"total_tokens":13,
      |"prompt_tokens_details":{"cache_write_tokens":8,"cached_tokens":2}}}""".stripMargin
    val output = Seq(json).toDF("json")
      .select(from_json(col("json"), OpenAIResponseSchemas.Chat).alias("response"))
    val parsed = output.select(to_json(col("response"))).head().getString(0).parseJson.asJsObject
    val expected = json.parseJson.asJsObject
    Seq("choices", "service_tier", "metadata", "usage").foreach { key =>
      assert(parsed.fields(key) == expected.fields(key))
    }
    val usage = output.select(UsageUtils.normalize(col("response.usage"),
      UsageUtils.UsageMappings.ChatCompletionsDetailed)).head().getStruct(0)
    val details = usage.getAs[Map[String, java.lang.Long]]("input_token_details")
    assert(details("cache_write_tokens") == 8L)
    assert(details("cached_tokens") == 2L)
    assert(details("audio_tokens") == null) // scalastyle:ignore null
    assert(details("image_tokens") == null) // scalastyle:ignore null
  }

  test("unset options preserve the request and scalar options are forwarded without policy validation") {
    val options = Seq(
      Map.empty[String, Any],
      Map[String, Any]("mode" -> "explicit", "ttl" -> "30m"),
      Map[String, Any]("ttl" -> "12h", "prewarm" -> true),
      Map[String, Any]("comparison_response_id" -> "resp_previous"),
      Map[String, Any]("optional" -> Option.empty[String].orNull,
        "nested" -> Map("optional" -> Option.empty[String].orNull)),
      Map[String, Any]("numbers" -> Seq(Double.MaxValue, Float.MaxValue, BigDecimal("1e400"))),
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
        Map[String, Any]("optional" -> Option.empty[String].orNull),
        Map[String, Any]("ttl" -> "not-a-cache-lifetime")
      ).foreach { options =>
        assert(payload(stage, requestRow(Some(options))).fields("prompt_cache_options") == options.toJson)
      }
    }
  }

  test("non-finite cache option numbers fail for scalar and nested column bindings") {
    val invalidNumbers = Seq[Any](Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity,
      Float.NaN, Float.PositiveInfinity, Float.NegativeInfinity)
    stages.foreach { stage =>
      invalidNumbers.foreach { value =>
        val options = Map[String, Any]("nested" -> Seq(Map("value" -> value)))
        val scalarError = intercept[IllegalArgumentException] {
          stage.setPromptCacheOptions(options)
        }
        assert(scalarError.getMessage.contains("promptCacheOptions"))
        assert(scalarError.getMessage.contains("finite"))
        val numberType = if (value.isInstanceOf[Float]) FloatType else DoubleType
        val optionsSchema = new StructType().add("nested", ArrayType(MapType(StringType, numberType)))
        val schema = new StructType().add("messages", ArrayType(messageSchema)).add("cache_options", optionsSchema)
        val message = new GenericRowWithSchema(Array[Any]("user", "hello"), messageSchema)
        val nested = new GenericRowWithSchema(Array[Any](Seq(Map("value" -> value))), optionsSchema)
        val row = new GenericRowWithSchema(Array[Any](Seq(message), nested), schema)
        val columnError = intercept[IllegalArgumentException] {
          payload(stage.setPromptCacheOptionsCol("cache_options"), row)
        }
        assert(columnError.getMessage.contains("promptCacheOptions"))
        assert(columnError.getMessage.contains("finite"))
      }
    }
  }

  test("non-finite reasoning numbers fail for scalar and nested column bindings") {
    val invalidNumbers = Seq[Any](Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity,
      Float.NaN, Float.PositiveInfinity, Float.NegativeInfinity)
    Seq[CacheStage](
      new OpenAIResponses().setMessagesCol("messages"),
      new OpenAIPrompt().setApiType("responses").setMessagesCol("messages")
    ).foreach { stage =>
      invalidNumbers.foreach { value =>
        val scalarError = intercept[IllegalArgumentException] {
          stage.setReasoning(Map("nested" -> Seq(Map("value" -> value))))
        }
        assert(scalarError.getMessage.contains("reasoning"))
        assert(scalarError.getMessage.contains("finite"))
        val numberType = if (value.isInstanceOf[Float]) FloatType else DoubleType
        val optionsSchema = new StructType().add("nested", ArrayType(MapType(StringType, numberType)))
        val schema = new StructType().add("messages", ArrayType(messageSchema)).add("reasoning_options", optionsSchema)
        val message = new GenericRowWithSchema(Array[Any]("user", "hello"), messageSchema)
        val nested = new GenericRowWithSchema(Array[Any](Seq(Map("value" -> value))), optionsSchema)
        val row = new GenericRowWithSchema(Array[Any](Seq(message), nested), schema)
        val columnError = intercept[IllegalArgumentException] {
          payload(stage.setReasoningCol("reasoning_options"), row)
        }
        assert(columnError.getMessage.contains("reasoning"))
        assert(columnError.getMessage.contains("finite"))
      }
    }
  }

  for (api <- Seq("chat_completions", "responses")) {
    val partType = if (api == "responses") "input_text" else "text"
    val requestKey = if (api == "responses") "input" else "messages"

    def breakpointStages: Seq[CacheStage] = Seq(
      if (api == "responses") new OpenAIResponses().setMessagesCol("messages")
      else new OpenAIChatCompletion().setMessagesCol("messages"),
      new OpenAIPrompt().setApiType(api).setMessagesCol("messages")
    )

    test(s"$api nested null breakpoint values preserve array elements and omit object fields") {
      val objectValue = JsObject("x" -> JsString("one"))
      Seq(
        "array(named_struct('x', 'one'), CAST(NULL AS STRUCT<x:STRING>))" ->
          JsArray(objectValue, JsNull),
        "array(array('one'), CAST(NULL AS ARRAY<STRING>))" ->
          JsArray(JsArray(JsString("one")), JsNull),
        "array(map('x', 'one'), CAST(NULL AS MAP<STRING,STRING>))" ->
          JsArray(objectValue, JsNull),
        "map('good', named_struct('x', 'one'), 'omitted', CAST(NULL AS STRUCT<x:STRING>))" ->
          JsObject("good" -> objectValue)
      ).foreach { case (value, expectedValue) =>
        val row = breakpointRow(partType,
          s"named_struct('values', $value, 'omitted', CAST(NULL AS STRING))")
        breakpointStages.foreach { stage =>
          val content = payload(stage, row).fields(requestKey).asInstanceOf[JsArray].elements.head.asJsObject
            .fields("content").asInstanceOf[JsArray].elements.head.asJsObject
          assert(content.fields("prompt_cache_breakpoint") == JsObject("values" -> expectedValue))
        }
      }
    }

    test(s"$api non-finite breakpoint numbers fail for structs maps and nested arrays") {
      for (numberType <- Seq("DOUBLE", "FLOAT");
           number <- Seq("NaN", "Infinity", "-Infinity")) {
        val value = s"CAST('$number' AS $numberType)"
        Seq(
          s"named_struct('ttl', $value)",
          s"map('ttl', $value)",
          s"named_struct('nested', array(map('ttl', $value)))"
        ).foreach { marker =>
          val row = breakpointRow(partType, marker)
          breakpointStages.foreach { stage =>
            val error = intercept[IllegalArgumentException] { payload(stage, row) }
            assert(error.getMessage.contains("prompt_cache_breakpoint"))
            assert(error.getMessage.contains("finite"))
          }
        }
      }
    }

    test(s"$api non-finite breakpoints produce local row errors without HTTP") {
      val stage: OpenAIServicesBase with HasMessagesInput = if (api == "responses") {
        new OpenAIResponses()
      } else {
        new OpenAIChatCompletion()
      }
      val calls = spark.sparkContext.longAccumulator
      stage.setMessagesCol("messages").setUrl("https://example.openai.azure.com/openai/v1/")
        .setSubscriptionKey("unused").setOutputCol("output").setErrorCol("error")
        .setHandler { (_: CloseableHttpClient, _: HTTPRequestData) =>
          calls.add(1L)
          throw new AssertionError("Non-finite breakpoint must not reach HTTP")
        }
      Seq("DOUBLE", "FLOAT").foreach { numberType =>
        val marker = s"named_struct('nested', array(map('ttl', CAST(number AS $numberType))))"
        val input = spark.sql(
          s"""SELECT array(named_struct('role', 'user', 'content', array(named_struct(
             |'type', '$partType', 'text', 'hello', 'prompt_cache_breakpoint', $marker)))) AS messages
             |FROM VALUES ('NaN'), ('Infinity'), ('-Infinity') AS invalid(number)""".stripMargin)
        val rows = stage.transform(input).select("output", "error").collect()
        assert(rows.length == 3)
        rows.foreach { row =>
          assert(row.isNullAt(0))
          val error = row.getAs[Row]("error").getAs[String]("response")
          assert(error.contains("messages[0].content[0].prompt_cache_breakpoint"))
          assert(error.contains("finite"))
        }
        assert(calls.value == 0L)
      }
    }

    test(s"$api non-JSON breakpoint leaves produce local row errors without HTTP") {
      val stage: OpenAIServicesBase with HasMessagesInput = if (api == "responses") {
        new OpenAIResponses()
      } else {
        new OpenAIChatCompletion()
      }
      val calls = spark.sparkContext.longAccumulator
      stage.setMessagesCol("messages").setUrl("https://example.openai.azure.com/openai/v1/")
        .setSubscriptionKey("unused").setOutputCol("output").setErrorCol("error")
        .setHandler { (_: CloseableHttpClient, _: HTTPRequestData) =>
          calls.add(1L)
          throw new AssertionError("Non-JSON breakpoint must not reach HTTP")
        }
      val session = spark.newSession()
      Seq(false, true).foreach { java8Datetime =>
        session.conf.set("spark.sql.datetime.java8API.enabled", java8Datetime.toString)
        val marker =
          """named_struct('nested', array(named_struct(
            |'timestamp', CASE WHEN leaf_type = 'timestamp' THEN TIMESTAMP '2026-01-01 00:00:00' END,
            |'date', CASE WHEN leaf_type = 'date' THEN DATE '2026-01-01' END,
            |'binary', CASE WHEN leaf_type = 'binary' THEN CAST('synthetic' AS BINARY) END)))""".stripMargin
        val input = session.sql(
          s"""SELECT leaf_type, array(named_struct('role', 'user', 'content', array(named_struct(
             |'type', '$partType', 'text', 'hello', 'prompt_cache_breakpoint', $marker)))) AS messages
             |FROM VALUES ('timestamp'), ('date'), ('binary') AS invalid(leaf_type)""".stripMargin)
        val rows = stage.transform(input).select("leaf_type", "output", "error").collect()
        assert(rows.map(_.getString(0)).toSet == Set("timestamp", "date", "binary"))
        rows.foreach { row =>
          assert(row.isNullAt(1))
          val error = row.getAs[Row]("error").getAs[String]("response")
          assert(error.contains("messages[0].content[0].prompt_cache_breakpoint"))
          assert(error.contains("JSON-compatible"))
        }
        assert(calls.value == 0L)
      }
    }

    test(s"$api finite breakpoint numbers retain precision for structs maps and nested arrays") {
      Seq(
        ("DOUBLE", "1.7976931348623157E308", JsNumber(Double.MaxValue)),
        ("FLOAT", "3.4028235E38", JsNumber(Float.MaxValue.toDouble)),
        ("DECIMAL(38,18)", "12345678901234567890.123456789012345678",
          JsNumber(BigDecimal("12345678901234567890.123456789012345678")))
      ).foreach { case (numberType, number, expectedNumber) =>
        val value = s"CAST('$number' AS $numberType)"
        val expected = JsObject("ttl" -> expectedNumber)
        Seq(
          s"named_struct('ttl', $value)" -> expected,
          s"map('ttl', $value)" -> expected,
          s"named_struct('nested', array(map('ttl', $value)))" -> JsObject("nested" -> JsArray(expected))
        ).foreach { case (marker, expectedMarker) =>
          val row = breakpointRow(partType, marker)
          breakpointStages.foreach { stage =>
            val content = payload(stage, row).fields(requestKey).asInstanceOf[JsArray].elements.head.asJsObject
              .fields("content").asInstanceOf[JsArray].elements.head.asJsObject
            assert(content.fields("prompt_cache_breakpoint") == expectedMarker)
          }
        }
      }
    }
  }

  test("Responses text image and file inputs preserve explicit breakpoint objects") {
    val partSchema = new StructType()
      .add("type", StringType).add("text", StringType).add("image_url", StringType)
      .add("file_id", StringType).add("prompt_cache_breakpoint", MapType(StringType, StringType))
    val messagesSchema = new StructType().add("role", StringType).add("content", ArrayType(partSchema))
    val breakpoints = Seq(
      Map("mode" -> "explicit", "ttl" -> "30m"),
      Map("mode" -> "explicit", "ttl" -> "12h"),
      Map("mode" -> "explicit"))
    val parts = Seq(
      Array[Any]("input_text", "Reusable instructions", null, null, breakpoints(0)), // scalastyle:ignore null
      Array[Any]("input_image", null, "https://example.org/image.png", null, breakpoints(1)), // scalastyle:ignore null
      Array[Any]("input_file", null, null, "file-example", breakpoints(2)) // scalastyle:ignore null
    ).map(values => new GenericRowWithSchema(values, partSchema))
    val message = new GenericRowWithSchema(Array[Any]("developer", parts), messagesSchema)
    val response = new OpenAIResponses()
    val body = EntityUtils.toString(response.getStringEntity(Seq(message), Map.empty)).parseJson.asJsObject
    val encodedParts = body.fields("input").asInstanceOf[JsArray].elements.head.asJsObject
      .fields("content").asInstanceOf[JsArray].elements
    assert(encodedParts.length == parts.length)
    encodedParts.zip(breakpoints).foreach { case (part, breakpoint) =>
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
