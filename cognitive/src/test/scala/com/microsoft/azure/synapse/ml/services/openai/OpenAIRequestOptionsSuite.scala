// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.contracts.HasOutputCol
import com.microsoft.azure.synapse.ml.io.http.HasErrorCol
import com.microsoft.azure.synapse.ml.services.HasCognitiveServiceInput
import com.microsoft.azure.synapse.ml.services.aifoundry.AIFoundryChatCompletion
import org.apache.spark.ml.{ComplexParamsWritable, Transformer}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{array, col, lit, struct}
import spray.json._

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.immutable.ListMap

class OpenAIRequestOptionsSuite extends OpenAIPromptHttpTestBase {

  import spark.implicits._

  private type RequestStage = Transformer with HasOpenAIModelParams with HasMessagesInput
    with HasCognitiveServiceInput with HasOutputCol with HasErrorCol with ComplexParamsWritable

  private def configure(stage: RequestStage, baseUrl: String): RequestStage =
    stage.setUrl(baseUrl).setSubscriptionKey("unused").setDeploymentName("gpt-6-luna")
      .setMessagesCol("messages").setOutputCol("output").setErrorCol("error")

  private def requestInput(stage: RequestStage, data: DataFrame): DataFrame = stage match {
    case _: OpenAIPrompt => data
    case _ => data.withColumn("messages", array(struct(lit("user").as("role"), col("prompt").as("content"))))
  }

  private def takeBodies(bodies: ConcurrentLinkedQueue[String], expected: Int): Seq[JsObject] = {
    assert(bodies.size() == expected, s"Expected $expected requests, received ${bodies.size()}")
    val result = Seq.fill(expected)(Option(bodies.poll()).getOrElse(fail("Missing HTTP request")).parseJson.asJsObject)
    assert(bodies.isEmpty, "Unexpected extra HTTP requests")
    result
  }

  private def requestText(body: JsObject, api: String): String = {
    val JsArray(messages) = body.fields(if (api == "responses") "input" else "messages")
    messages.last.asJsObject.fields("content") match {
      case JsString(text) => text
      case JsArray(parts) => parts.map { part =>
        val JsString(text) = part.asJsObject.fields("text")
        text
      }.mkString
      case other => fail(s"Unexpected request content: $other")
    }
  }

  test("Responses rejects non-object reasoning columns rather than replacing their values") {
    withEchoServer { (baseUrl, bodies) =>
      for (promptRoute <- Seq(false, true); withEffort <- Seq(true, false)) {
        val stage: RequestStage = if (promptRoute) {
          new OpenAIPrompt().setApiType("responses").setPromptTemplate("{prompt}")
        } else new OpenAIResponses()
        configure(stage, baseUrl).setReasoningCol("reasoning_options")
        if (withEffort) stage.setReasoningEffort("low")
        val data = Seq(("hello", "not-an-object")).toDF("prompt", "reasoning_options")
        val error = intercept[Exception](stage.transform(requestInput(stage, data)).collect())
        val messages = Iterator.iterate(error: Throwable)(_.getCause).takeWhile(_ != null)
          .flatMap(value => Option(value.getMessage)).mkString("\n")
        assert(messages.contains("reasoning must be an object"))
        assert(bodies.isEmpty, "Invalid reasoning must fail before HTTP")
      }
    }
  }

  test("Chat legacy controls fail before HTTP after loading and succeed when cleared") {
    val factories: Seq[(String, () => RequestStage, MLReadable[_ <: RequestStage])] = Seq(
      ("Chat", () => new OpenAIChatCompletion(), OpenAIChatCompletion),
      ("Prompt", () => new OpenAIPrompt().setPromptTemplate("{prompt}"), OpenAIPrompt)
    )
    val controls: Seq[(String, HasOpenAITextParams => Unit)] = Seq(
      "echo" -> ((stage: HasOpenAITextParams) => stage.setEcho(true)),
      "bestOf" -> ((stage: HasOpenAITextParams) => stage.setBestOf(2)),
      "cacheLevel" -> ((stage: HasOpenAITextParams) => stage.setCacheLevel(1))
    )
    withEchoServer { (baseUrl, bodies) =>
      for ((route, create, reader) <- factories; (name, setControl) <- controls) {
        val stage = create().setMessagesCol("messages").setUrl(baseUrl)
          .setCustomUrlRoot(s"$baseUrl/chat/completions")
        setControl(stage)
        val path = tmpDir.resolve(s"legacy-control-$route-$name").toString
        stage.write.save(path)
        val restored = reader.load(path)
        val data = Seq("hello").toDF("prompt")
        Seq(stage, restored).foreach { candidate =>
          configure(candidate, baseUrl)
          val error = intercept[Exception](candidate.transform(requestInput(candidate, data)).collect())
          val messages = Iterator.iterate(error: Throwable)(_.getCause).takeWhile(_ != null)
            .flatMap(value => Option(value.getMessage)).mkString("\n")
          assert(messages.contains(s"$name not supported by Chat Completions"))
          assert(bodies.isEmpty)
        }
        restored.clear(restored.getParam(name))
        val rows = restored.transform(requestInput(restored, data)).collect()
        assert(rows.length == 1 && rows.head.isNullAt(rows.head.fieldIndex("error")))
        val body = takeBodies(bodies, 1).head
        assert(!body.fields.contains(name) && !body.fields.contains("cache_level") && !body.fields.contains("best_of"))
      }
    }
  }

  test("Foundry rejects resolved storage values but omits null columns and Prompt scalar false") {
    withEchoServer { (baseUrl, bodies) =>
      val stages: Seq[RequestStage] = Seq(
        new AIFoundryChatCompletion().setModel("foundry-model").setStoreCol("persist"),
        new OpenAIPrompt().setModel("foundry-model").setPromptTemplate("{prompt}").setStoreCol("persist")
      )
      stages.foreach { stage =>
        configure(stage, baseUrl).setUrl("https://example.services.ai.azure.com")
          .setCustomUrlRoot(s"$baseUrl/chat/completions")
        Seq(Some(false), Some(true), None).foreach { value =>
          val data = Seq(("hello", value)).toDF("prompt", "persist")
          if (value.isDefined) {
            val error = intercept[Exception](stage.transform(requestInput(stage, data)).collect())
            val messages = Iterator.iterate(error: Throwable)(_.getCause).takeWhile(_ != null)
              .flatMap(cause => Option(cause.getMessage)).mkString("\n")
            assert(messages.contains("store not supported by AI Foundry model inference"))
            assert(bodies.isEmpty)
          } else {
            val rows = stage.transform(requestInput(stage, data)).collect()
            assert(rows.length == 1 && rows.head.isNullAt(rows.head.fieldIndex("error")))
            assert(!takeBodies(bodies, 1).head.fields.contains("store"))
          }
        }
        val data = requestInput(stage, Seq("hello").toDF("prompt"))
        stage match {
          case prompt: OpenAIPrompt =>
            val row = prompt.setStore(false).transform(data).head()
            assert(row.isNullAt(row.fieldIndex("error")))
            assert(!takeBodies(bodies, 1).head.fields.contains("store"))
          case foundry: AIFoundryChatCompletion =>
            val error = intercept[Exception](foundry.setStore(false).transform(data).collect())
            val messages = Iterator.iterate(error: Throwable)(_.getCause).takeWhile(_ != null)
              .flatMap(cause => Option(cause.getMessage)).mkString("\n")
            assert(messages.contains("store not supported by AI Foundry model inference"))
            assert(bodies.isEmpty)
          case other => fail(s"Unexpected Foundry test stage: $other")
        }
      }
    }
  }

  test("Cache option normalization retains caller field order for scalar maps and structs") {
    val names = Seq("zeta", "alpha", "theta", "beta", "gamma", "delta")
    val options = ListMap(names.map(name => name -> name): _*)
    withEchoServer { (baseUrl, bodies) =>
      Seq(false, true).foreach { column =>
        val stage = configure(new OpenAIChatCompletion(), baseUrl)
        val data = Seq("hello").toDF("prompt")
          .withColumn("cache_options", struct(names.map(name => lit(name).as(name)): _*))
        if (column) stage.setPromptCacheOptionsCol("cache_options") else stage.setPromptCacheOptions(options)
        val row = stage.transform(requestInput(stage, data)).head()
        assert(row.isNullAt(row.fieldIndex("error")))
        assert(bodies.size() == 1)
        val body = Option(bodies.poll()).getOrElse(fail("Missing HTTP request"))
        val positions = names.map(name => body.indexOf(s""""$name":"""))
        assert(positions.forall(_ >= 0) && positions == positions.sorted)
        assert(bodies.isEmpty)
      }
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
            val body = takeBodies(bodies, 1).head.fields
            assert(body("prompt_cache_key") == JsString("warmup"))
            assert(body("prompt_cache_options").asJsObject.fields("prewarm") == JsTrue)
            assert(body("reasoning").asJsObject.fields("context") == JsString("all_turns"))
            assert(body("include") == JsArray(JsString("message.output_text.logprobs")))
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
        val input = Seq(("stored", true), ("not-stored", false)).toDF("prompt", "persist").coalesce(1)
        val result = stage.transform(input)
        assert(result.schema == stage.transformSchema(input.schema))
        val rows = result.collect()
        assert(rows.forall(row => row.isNullAt(row.fieldIndex("error"))))
        assert(rows.find(_.getAs[Boolean]("persist")).get.getAs[String]("response_id").nonEmpty)
        assert(rows.find(!_.getAs[Boolean]("persist")).get.isNullAt(result.schema.fieldIndex("response_id")))
        val stored = takeBodies(bodies, 2).map(body => requestText(body, api) -> body.fields("store")).toMap
        assert(stored == Map("stored" -> JsTrue, "not-stored" -> JsFalse))
      }
    }
  }

  test("Storage columns may be replaced by output usage or response ID columns") {
    withEchoServer { (baseUrl, bodies) =>
      for (api <- Seq("chat_completions", "responses"); name <- Seq("output", "usage", "response_id")) {
        val stage = prompt(baseUrl, api).setColumnTypes(Map.empty[String, String])
          .setStoreCol(name).setUsageCol("usage").setResponseIdCol("response_id")
        val input = Seq(("stored", Some(true)), ("not-stored", Some(false)), ("unset", None))
          .toDF("prompt", name).coalesce(1)
        val result = stage.transform(input)
        val rows = result.collect()
        assert(result.schema == stage.transformSchema(input.schema))
        assert(result.columns.distinct.length == result.columns.length)
        assert(rows.forall(row => row.getAs[String]("output") == "ok" && row.isNullAt(row.fieldIndex("error"))))
        assert(rows.find(_.getAs[String]("prompt") == "stored").get.getAs[String]("response_id").nonEmpty)
        assert(rows.filter(_.getAs[String]("prompt") != "stored")
          .forall(_.isNullAt(result.schema.fieldIndex("response_id"))))
        val stored = takeBodies(bodies, 3).map(body => requestText(body, api) -> body.fields.get("store")).toMap
        val expected = Map("stored" -> Some(JsTrue), "not-stored" -> Some(JsFalse),
          "unset" -> (if (api == "responses") Some(JsFalse) else None))
        assert(stored == expected)
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
        assert(takeBodies(bodies, 1).head.fields("prompt_cache_options") ==
          JsObject("mode" -> JsString("implicit"), "ttl" -> JsString("30m")))

        val rows = Seq(("hello", Map("mode" -> "explicit", "ttl" -> "12h"))).toDF("prompt", "cache_options")
        val column = transformer.setPromptCacheOptionsCol("cache_options").transform(rows).head()
        assert(column.isNullAt(column.fieldIndex("error")))
        assert(takeBodies(bodies, 1).head.fields("prompt_cache_options") ==
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
          assert(takeBodies(bodies, 1).head.fields("prompt_cache_options") ==
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
        .withColumn("cache_options", struct(lit("30m").as("ttl"), col("prewarm"),
          lit(Option.empty[String].orNull).as("optional")))
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
          JsObject("ttl" -> JsString("30m"), "prewarm" -> JsBoolean(true), "optional" -> JsNull),
          JsObject("ttl" -> JsString("30m"), "prewarm" -> JsBoolean(false), "optional" -> JsNull)
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

}
