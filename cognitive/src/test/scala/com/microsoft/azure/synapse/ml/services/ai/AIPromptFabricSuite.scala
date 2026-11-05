// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.{HTTPRequestData, HTTPSchema, HandlingUtils, URLKey}
import com.microsoft.azure.synapse.ml.param.GlobalParams
import com.microsoft.azure.synapse.ml.services.{OpenAIApiVersion, OpenAISubscriptionKey}
import com.microsoft.azure.synapse.ml.services.openai.OpenAIDecisions
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{MapType, StringType, StructType}
import spray.json._

import java.nio.file.Files
import java.util.Collections
import scala.collection.JavaConverters._
import scala.util.Using

class TestableAIHeaderPrompt(override val uid: String) extends AIPrompt(uid) {
  override protected val subscriptionKeyHeaderName: String = "x-model-key"
  override protected val aadHeaderName: String = "x-model-token"
}

class TestableAISharedHeaderPrompt(override val uid: String) extends AIPrompt(uid) {
  override protected val subscriptionKeyHeaderName: String = "AUTHORIZATION"
}

class AIPromptFabricSuite extends TestBase {
  private val root = "https://fabric.example.invalid/cognitive/openai"
  private val chat = """{"choices":[{"index":0,"message":{"role":"assistant","content":"ok"}}]}"""
  private val responses = """{"output":[{"type":"message","role":"assistant",
    |"content":[{"type":"output_text","text":"ok"}]}]}""".stripMargin
  private val embeddings = """{"data":[{"index":0,"embedding":[0.5]}]}"""

  test("JSON Content-Type replaces all caller casings in scalar and column header maps") {
    val custom = Map("content-type" -> "text/plain", "Content-Type" -> "application/xml", "X-Custom" -> "retained")
    val telemetry = Map("CONTENT-TYPE" -> "application/octet-stream", "X-Telemetry" -> "retained")
    val schema = new StructType().add("text", StringType)
      .add("custom", MapType(StringType, StringType)).add("telemetry", MapType(StringType, StringType))
    val data = spark.createDataFrame(Seq(Row("hello", custom, telemetry)).asJava, schema)
    Seq(false, true).foreach { perRow =>
      Seq("chat_completions" -> chat, "decisions" -> NativeRequestFixtures.DecisionResponse)
        .foreach { case (api, reply) =>
          Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
            val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
              .setInputCol("text").setOutputCol("answer").setErrorCol("error")
            if (api == "decisions") stage.setQuestions(NativeRequestFixtures.DecisionQuestions)
            if (perRow) {
              stage.setVectorParam(stage.customHeaders, "custom")
              stage.setVectorParam(stage.telemHeaders, "telemetry")
            } else stage.setCustomHeaders(custom).setTelemHeaders(telemetry)
            val row = stage.transform(data).head()
            assert(row.getAs[Row]("error") == null && row.getAs[Any]("answer") != null)
            assert(server.requests.size() == 1)
            val headers = server.headers.peek()
            assert(headers("content-type") == "application/json")
            assert(headers("x-custom") == "retained" && headers("x-telemetry") == "retained")
          }
        }
    }
  }

  test("HTTP header columns reject malformed names and values before sending valid siblings") {
    val invalid = Seq(
      "X-Caller: ignored\r\nX-Injected" -> "synthetic-value",
      "X-Caller\nX-Injected" -> "synthetic-value",
      "X-\u0000Caller" -> "synthetic-value",
      "X Caller" -> "synthetic-value",
      "X:Caller" -> "synthetic-value",
      "" -> "synthetic-value",
      "ap\u0131-key" -> "synthetic-value",
      "author\u0131zation" -> "synthetic-value",
      "X-Caller" -> "synthetic-value\r\nX-Injected: yes",
      "X-Caller" -> "synthetic-\u0000value",
      "X-Caller" -> "synthetic-\u010d\u010aX-Injected: yes",
      "X-Caller" -> "synthetic-\u0085value",
      "X-Caller" -> "synthetic-\u0159value")
    val schema = new StructType().add("text", StringType).add("headers", MapType(StringType, StringType))
    val data = spark.createDataFrame((invalid.map { case (name, value) => Row("invalid", Map(name -> value)) } :+
      Row("valid", Map("X-Caller" -> "ordinary value", "X-Empty" -> "", "X-Latin" -> "caf\u00e9"))).asJava, schema)
    Seq(false, true).foreach { telemetry =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> chat)) { server =>
        val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
          .setOutputCol("answer").setErrorCol("error")
        stage.setVectorParam(if (telemetry) stage.telemHeaders else stage.customHeaders, "headers")
        val rows = stage.transform(data).collect()
        rows.filter(_.getAs[String]("text") == "invalid").foreach { row =>
          val diagnostic = row.getAs[Row]("error").getString(0).parseJson.asJsObject.fields
          assert(diagnostic("code") == JsString("invalid_request"))
          assert(Set[JsValue](JsString("requirement failed: Invalid HTTP header name"),
            JsString("requirement failed: Invalid HTTP header value"))
            .contains(diagnostic("message")))
          assert(row.getAs[String]("answer") == null)
        }
        assert(rows.find(_.getAs[String]("text") == "valid").get.getAs[Row]("error") == null)
        assert(server.requests.size() == 1)
        assert(server.headers.peek()("x-caller") == "ordinary value")
        assert(server.headers.peek()("x-empty") == "")
        assert(server.headers.peek()("x-latin") == "caf\u00e9")
      }
    }
  }

  test("HTTP header validation covers scalar maps and resolved native credentials without exposing values") {
    val cases = Seq(
      ("systemone", NativeRequestFixtures.SystemResponse, NativeRequestFixtures.SystemQuestions),
      ("decisions", NativeRequestFixtures.DecisionResponse, NativeRequestFixtures.DecisionQuestions))
    cases.foreach { case (api, reply, questions) =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        def stage: AIPrompt = new AIPrompt().setApiType(api).setQuestions(questions)
          .setUrl(if (api == "systemone") server.url else server.url + "/v1")
          .setModel("model").setInputCol("text").setOutputCol("answer").setErrorCol("error")
        val input = spark.createDataFrame(Collections.singletonList(Row("hello")),
          new StructType().add("text", StringType))
        val invalid = Seq("synthetic-private-value\r\nX-Injected: yes",
          "synthetic-private-value\u010d\u010aX-Injected: yes", "synthetic-private-value\u0085")
        val scalarCases = invalid.flatMap { value =>
          Seq[AIPrompt => AIPrompt](
            _.setCustomHeaders(Map("X-Caller" -> value)),
            _.setTelemHeaders(Map("X-Caller" -> value)),
            _.setSubscriptionKey(value),
            _.setAADToken(value),
            _.setCustomAuthHeader(value))
        } ++ Seq("ap\u0131-key", "author\u0131zation").flatMap { name =>
          Seq[AIPrompt => AIPrompt](
            _.setCustomHeaders(Map(name -> "synthetic-private-value")),
            _.setTelemHeaders(Map(name -> "synthetic-private-value")))
        }
        scalarCases.foreach { configure =>
          val candidate = configure(stage)
          Seq(
            intercept[IllegalArgumentException](candidate.transformSchema(input.schema)),
            intercept[IllegalArgumentException](candidate.transform(input))
          ).foreach { error =>
            assert(error.getMessage.contains("Invalid HTTP header") && Option(error.getCause).isEmpty)
            assert(!error.getMessage.contains("synthetic-private-value") && !error.getMessage.contains("X-Injected"))
          }
        }
        assert(server.requests.isEmpty)
        val data = spark.createDataFrame((invalid.map(value => Row("bad", value)) :+
          Row("good", "synthetic-value")).asJava,
          new StructType().add("text", StringType).add("credential", StringType))
        val columnCases: Seq[AIPrompt => AIPrompt] = Seq(
          _.setSubscriptionKeyCol("credential"), _.setAADTokenCol("credential"),
          _.setCustomAuthHeaderCol("credential"))
        columnCases.foreach { configure =>
          val rows = configure(stage).transform(data).collect()
          rows.filter(_.getAs[String]("text") == "bad").foreach { row =>
            val diagnostic = row.getAs[Row]("error").getString(0)
            assert(diagnostic.parseJson.asJsObject.fields("message") ==
              JsString("requirement failed: Invalid HTTP header value"))
          }
          assert(rows.find(_.getAs[String]("text") == "good").get.getAs[Row]("error") == null)
        }
        assert(server.requests.size() == columnCases.size)
      }
    }
  }

  test("scalar header preflight preserves ignored credential and reserved marker values") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chat)) { server =>
      val data = spark.createDataFrame(Collections.singletonList(Row("hello")),
        new StructType().add("text", StringType))
      val malformed = "synthetic-ignored\r\nvalue"
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model")
        .setInputCol("text").setErrorCol("error").setSubscriptionKey("synthetic-key")
        .setAADToken(malformed).setCustomAuthHeader(malformed)
        .setCustomHeaders(Map("Authorization" -> malformed, "X-Caller" -> malformed,
          "content-type" -> malformed, HTTPRequestData.FabricAuthMarkerHeader -> malformed))
        .setTelemHeaders(Map("api-key" -> malformed, "X-Caller" -> "retained"))
      assert(stage.transformSchema(data.schema).fieldNames.contains("error"))
      assert(stage.transform(data).head().getAs[Row]("error") == null)
      assert(server.requests.size() == 1 && server.headers.peek()("x-caller") == "retained")
      assert(server.headers.peek()("api-key") == "synthetic-key")
    }
  }

  test("mixed header preflight defers conditional scalar credentials to each row") {
    val malformed = "synthetic-ignored\r\nvalue"
    val data = spark.createDataFrame(Seq(
      Row("valid", "synthetic-key"), Row("null", Option.empty[String].orNull), Row("blank", " ")).asJava,
      new StructType().add("text", StringType).add("key", StringType))
    val cases: Seq[AIPrompt => AIPrompt] = Seq(
      _.setSubscriptionKeyCol("key").setAADToken(malformed),
      _.setAADTokenCol("key").setCustomAuthHeader(malformed),
      _.setCustomAuthHeaderCol("key").setCustomHeaders(Map("api-key" -> malformed)))
    cases.foreach { configure =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> chat)) { server =>
        val stage = configure(new AIPrompt().setUrl(server.url + "/v1").setModel("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error"))
        assert(stage.transformSchema(data.schema).fieldNames.contains("answer"))
        val rows = stage.transform(data).collect()
        assert(rows.find(_.getAs[String]("text") == "valid").get.getAs[Row]("error") == null)
        rows.filter(_.getAs[String]("text") != "valid").foreach { row =>
          val diagnostic = row.getAs[Row]("error").getString(0)
          assert(diagnostic.contains("invalid_request") && !diagnostic.contains("synthetic-ignored"))
          assert(row.getAs[String]("answer") == null)
        }
        assert(server.requests.size() == 1)
        stage.setSubscriptionKey(malformed)
        assert(intercept[IllegalArgumentException](stage.transformSchema(data.schema))
          .getMessage.contains("Invalid HTTP header value"))
      }
    }
  }

  test("mixed header preflight defers telemetry overrides but preserves guaranteed scalar authentication") {
    val malformed = "synthetic-ignored\r\nvalue"
    val data = spark.createDataFrame(Seq(
      Row("valid", Map("X-Caller" -> "retained")),
      Row("null", Option.empty[Map[String, String]].orNull), Row("missing", Map.empty[String, String])).asJava,
      new StructType().add("text", StringType).add("telemetry", MapType(StringType, StringType)))
    Using.resource(new AIRecordingServer((_, _) => 200 -> chat)) { server =>
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
        .setOutputCol("answer").setErrorCol("error")
        .setCustomHeaders(Map("X-Caller" -> malformed, "api-key" -> "synthetic-key"))
      stage.setVectorParam(stage.telemHeaders, "telemetry")
      val rows = stage.transform(data).collect()
      assert(rows.find(_.getAs[String]("text") == "valid").get.getAs[Row]("error") == null)
      assert(rows.filter(_.getAs[String]("text") != "valid").forall(row =>
        row.getAs[Row]("error").getString(0).contains("invalid_request")))
      assert(server.requests.size() == 1 && server.headers.peek()("x-caller") == "retained")
      assert(server.headers.peek()("api-key") == "synthetic-key")
      Seq(Map("api-key" -> malformed), Map("Invalid Header" -> "value")).foreach { headers =>
        stage.setCustomHeaders(headers)
        assert(intercept[IllegalArgumentException](stage.transformSchema(data.schema))
          .getMessage.contains("Invalid HTTP header"))
      }
    }
  }

  test("native bearer conversion preserves credential sources when authentication header names coincide") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
      val data = spark.createDataFrame(Collections.singletonList(Row("hello", "synthetic-token")),
        new StructType().add("text", "string").add("token", "string"))
      val base = new TestableAISharedHeaderPrompt("shared-auth-header").setApiType("decisions")
        .setUrl(server.url + "/v1").setModel("model").setInputCol("text").setErrorCol("error")
        .setQuestions(NativeRequestFixtures.DecisionQuestions)
      val cases: Seq[(AIPrompt => Unit, String)] = Seq(
        (_.setAADToken("synthetic-token"), "Bearer synthetic-token"),
        (_.setAADTokenCol("token"), "Bearer synthetic-token"),
        (_.setCustomAuthHeader("Custom synthetic-token"), "Custom synthetic-token"),
        (_.setCustomAuthHeaderCol("token"), "synthetic-token"),
        (_.setSubscriptionKey("synthetic-key"), "Bearer synthetic-key"),
        (_.setCustomHeaders(Map("authorization" -> "synthetic-key")), "Bearer synthetic-key"),
        (stage => {
          stage.setSubscriptionKey(" ").setAADToken("synthetic-token")
            .setCustomAuthHeader("Custom ignored").setCustomHeaders(Map("authorization" -> "ignored"))
        }, "Bearer synthetic-token"))
      cases.foreach { case (configure, expected) =>
        val stage = base.copy(org.apache.spark.ml.param.ParamMap.empty)
        configure(stage)
        assert(stage.transform(data).head().getAs[Row]("error") == null)
        assert(server.headers.remove().get("authorization").contains(expected))
      }
      assert(server.requests.size() == cases.size)
    }
  }

  test("public inference honors inherited authentication header names including native bearer conversion") {
    Seq("chat_completions" -> chat, "decisions" -> NativeRequestFixtures.DecisionResponse).foreach {
      case (api, response) =>
        Using.resource(new AIRecordingServer((_, _) => 200 -> response)) { server =>
          val data = spark.createDataFrame(Collections.singletonList(Row("hello")),
            new StructType().add("text", "string"))
          val stage = new TestableAIHeaderPrompt("custom-auth-headers").setApiType(api)
            .setUrl(server.url + "/v1").setModel("model").setInputCol("text").setErrorCol("error")
          if (api == "decisions") stage.setQuestions(NativeRequestFixtures.DecisionQuestions)
          Seq(false, true).foreach { useToken =>
            if (useToken) {
              stage.clear(stage.subscriptionKey)
              stage.setAADToken("synthetic-token")
            } else stage.setSubscriptionKey("synthetic-key")
            assert(stage.transform(data).head().getAs[Row]("error") == null)
            val headers = server.headers.remove()
            val bearer = useToken || api == "decisions"
            val name = if (bearer) "x-model-token" else "x-model-key"
            val credential = if (useToken) "synthetic-token" else "synthetic-key"
            assert(headers.get(name).contains(if (bearer) "Bearer " + credential else credential))
            assert(!headers.contains("api-key") && !headers.contains("authorization"))
          }
          assert(server.requests.size() == 2)
        }
    }
  }

  Seq(false, true).foreach { endpointConfigured =>
    test(s"native stages ignore global endpoint and API-key defaults with explicit endpoint=$endpointConfigured") {
      val previousUrl = GlobalParams.getGlobalParam(URLKey)
      val previousKey = GlobalParams.getGlobalParam(OpenAISubscriptionKey)
      try {
        Seq(new AIPrompt().setApiType("systemone"), new AISystemOne(),
          new AIPrompt().setApiType("decisions"), new OpenAIDecisions()).foreach { stage =>
          val systemone = stage.getApiType == "systemone"
          val reply = if (systemone) NativeRequestFixtures.SystemResponse else NativeRequestFixtures.DecisionResponse
          Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
            GlobalParams.setGlobalParam(URLKey, server.url)
            GlobalParams.setGlobalParam(OpenAISubscriptionKey, Left("synthetic-unrelated-key"))
            val data = spark.createDataFrame(Collections.singletonList(Row("hello")),
              new StructType().add("text", "string"))
            stage.setModel("model").setInputCol("text").setErrorCol("error")
              .setQuestions(if (systemone) NativeRequestFixtures.SystemQuestions
                else NativeRequestFixtures.DecisionQuestions)
            if (endpointConfigured) {
              stage.setUrl(server.url)
              assert(stage.transform(data).head().getAs[Row]("error") == null)
              val anonymous = server.headers.peek()
              assert(!anonymous.contains("authorization") && !anonymous.contains("api-key"))
              server.headers.clear()
              stage.setSubscriptionKey("synthetic-native-key")
              assert(stage.transform(data).head().getAs[Row]("error") == null)
              val name = if (systemone) "api-key" else "authorization"
              val expected = if (systemone) "synthetic-native-key" else "Bearer synthetic-native-key"
              assert(server.headers.peek()(name) == expected && server.requests.size() == 2)
            } else {
              val error = intercept[IllegalArgumentException](stage.transformSchema(data.schema))
              assert(error.getMessage.contains("Configure an endpoint"))
              intercept[IllegalArgumentException](stage.transform(data))
              assert(server.requests.isEmpty && !stage.isDefined(stage.url))
            }
          }
        }
      } finally {
        previousUrl match {
          case Some(value) => GlobalParams.setGlobalParam(URLKey, value)
          case None => GlobalParams.resetGlobalParam(URLKey)
        }
        previousKey match {
          case Some(value) => GlobalParams.setGlobalParam(OpenAISubscriptionKey, value)
          case None => GlobalParams.resetGlobalParam(OpenAISubscriptionKey)
        }
      }
    }
  }

  private def withoutGlobalUrl(action: => Unit): Unit = {
    val previous = GlobalParams.getGlobalParam(URLKey)
    try {
      GlobalParams.resetGlobalParam(URLKey)
      action
    } finally {
      previous match {
        case Some(value) => GlobalParams.setGlobalParam(URLKey, value)
        case None => GlobalParams.resetGlobalParam(URLKey)
      }
    }
  }

  Seq(false, true).foreach { fail =>
    test(s"managed endpoint probe isolates caller global authentication and restores it with failure=$fail") {
      val previousUrl = GlobalParams.getGlobalParam(URLKey)
      val previousKey = GlobalParams.getGlobalParam(OpenAISubscriptionKey)
      val callerUrl = "https://caller.example.invalid/v1"
      val callerKey = Left("synthetic-caller-key")
      val stopped = new IllegalStateException("synthetic probe interruption")
      try {
        GlobalParams.setGlobalParam(URLKey, callerUrl)
        GlobalParams.setGlobalParam(OpenAISubscriptionKey, callerKey)
        val create = () => {
          if (fail) throw stopped
          new TestableAIFabricAuthPrompt("managed-endpoint-probe")
        }
        if (fail) {
          assert(intercept[IllegalStateException] {
            AIPromptFabricProbe.verifyImplicitEndpoints(spark, create)
          } eq stopped)
        } else {
          assert(AIPromptFabricProbe.verifyImplicitEndpoints(spark, create) ==
            """["chat_completions","responses","embeddings"]""")
        }
        assert(GlobalParams.getGlobalParam(URLKey).contains(callerUrl))
        assert(GlobalParams.getGlobalParam(OpenAISubscriptionKey).contains(callerKey))
      } finally {
        previousUrl match {
          case Some(value) => GlobalParams.setGlobalParam(URLKey, value)
          case None => GlobalParams.resetGlobalParam(URLKey)
        }
        previousKey match {
          case Some(value) => GlobalParams.setGlobalParam(OpenAISubscriptionKey, value)
          case None => GlobalParams.resetGlobalParam(OpenAISubscriptionKey)
        }
      }
    }
  }

  test("explicit Fabric credentials never evaluate an unavailable implicit fallback") {
    withoutGlobalUrl {
      val reply = chat
      val data = spark.createDataFrame(Collections.singletonList(
        Row("hello", "model", "synthetic-credential", Map("Authorization" -> "Embedded synthetic-credential"))),
        new StructType().add("text", StringType).add("model", StringType).add("credential", StringType)
          .add("headers", MapType(StringType, StringType)))
      val cases: Seq[(AIPrompt => AIPrompt, String, String)] = Seq(
        (_.setSubscriptionKey("synthetic-credential"), "api-key", "synthetic-credential"),
        (_.setAADToken("synthetic-credential"), "authorization", "Bearer synthetic-credential"),
        (_.setCustomAuthHeader("Custom synthetic-credential"), "authorization", "Custom synthetic-credential"),
        (_.setSubscriptionKeyCol("credential"), "api-key", "synthetic-credential"),
        (_.setAADTokenCol("credential"), "authorization", "Bearer synthetic-credential"),
        (_.setCustomAuthHeaderCol("credential"), "authorization", "synthetic-credential"),
        (_.setCustomHeaders(Map("API-KEY" -> "synthetic-credential")), "api-key", "synthetic-credential"),
        (_.setCustomHeaders(Map("AUTHORIZATION" -> "Embedded synthetic-credential")),
          "authorization", "Embedded synthetic-credential"),
        (stage => stage.setVectorParam(stage.customHeaders, "headers"), "authorization",
          "Embedded synthetic-credential"))
      cases.foreach { case (configure, name, expected) =>
        val stage = configure(new TestableAIThrowingFabricAuthPrompt("unused-fabric-fallback")
          .setModelCol("model").setInputCol("text").setOutputCol("answer").setErrorCol("error"))
        stage.setHandler { (_, request) =>
          val headers = request.headers.map(h => h.name.toLowerCase(java.util.Locale.ROOT) -> h.value).toMap
          assert(headers(name) == expected)
          assert(!request.headers.exists(_.name == HTTPRequestData.FabricAuthMarkerHeader))
          HTTPSchema.stringToResponse(reply, 200, "OK")
        }
        val row = stage.transform(data).head()
        assert(row.getAs[Row]("error") == null && row.getAs[String]("answer") == "ok")
      }
    }
  }

  Seq("chat_completions" -> chat, "responses" -> responses, "embeddings" -> embeddings)
    .foreach { case (api, reply) =>
      test(s"$api automatically uses an unsaved Fabric endpoint when no URL is configured") {
        withoutGlobalUrl {
          val previousVersion = GlobalParams.getGlobalParam(OpenAIApiVersion)
          val directory = Files.createTempDirectory("aiprompt-implicit-fabric-")
          try {
            GlobalParams.setGlobalParam(OpenAIApiVersion, Left("synthetic-version"))
            val data = spark.createDataFrame(Collections.singletonList(Row("hello")),
              new StructType().add("text", "string"))
            val stage = new TestableAIFabricAuthPrompt(s"implicit-$api").setApiType(api)
              .setModel("model").setInputCol("text").setOutputCol("answer").setErrorCol("error")
            stage.setHandler { (_, request) =>
              assert(request.requestLine.uri.startsWith("https://fabric.example.invalid/cognitive/openai/"))
              assert(request.requestLine.uri.contains("api-version=synthetic-version"))
              val headers = request.headers.map(h => h.name.toLowerCase(java.util.Locale.ROOT) -> h.value).toMap
              assert(headers("authorization") == "MwcToken synthetic")
              assert(headers("x-taxonomy-traffictype") == "Background" && headers("x-llm-service-tier") == "flex")
              assert(request.headers.exists(_.name == HTTPRequestData.FabricAuthMarkerHeader))
              HTTPSchema.stringToResponse(reply, 200, "OK")
            }
            stage.transformSchema(data.schema)
            assert(!stage.isDefined(stage.url) && !stage.isSet(stage.url))
            val row = stage.transform(data).head()
            assert(row.getAs[Row]("error") == null && !row.isNullAt(row.fieldIndex("answer")))
            val path = directory.resolve("stage").toString
            stage.write.save(path)
            val loaded = AIPrompt.load(path)
            assert(!loaded.isDefined(loaded.url) && !loaded.isSet(loaded.url))
            assert(loaded.effectiveCopy().getUrl == root)
            assert(GlobalParams.getGlobalParam(URLKey).isEmpty)
          } finally {
            org.apache.commons.io.FileUtils.deleteDirectory(directory.toFile)
            previousVersion match {
              case Some(value) => GlobalParams.setGlobalParam(OpenAIApiVersion, value)
              case None => GlobalParams.resetGlobalParam(OpenAIApiVersion)
            }
          }
        }
      }
    }

  test("missing explicit endpoints fail clearly outside Fabric and for native Fabric APIs") {
    withoutGlobalUrl {
      val schema = new StructType().add("text", "string")
      Seq(new AIPrompt().setInputCol("text"),
        new TestableAIFabricAuthPrompt("no-native-route").setApiType("systemone")
          .setInputCol("text").setQuestions(NativeRequestFixtures.SystemQuestions),
        new TestableAIFabricAuthPrompt("no-decisions-route").setApiType("decisions")
          .setInputCol("text").setQuestions(NativeRequestFixtures.DecisionQuestions)).foreach { stage =>
        stage.setModel("model")
        val error = intercept[IllegalArgumentException](stage.transformSchema(schema))
        assert(error.getMessage.contains("endpoint") && error.getMessage.contains("setUrl"))
        assert(!stage.isDefined(stage.url))
      }

    }
  }

  test("injected bounded Fabric handler preserves rejected row diagnostics without authentication replay") {
    val rejected = """{"error":{"code":"Unauthorized","message":"Synthetic rejection"}}"""
    Using.resource(new AIRecordingServer(
      (attempt, _) => if (attempt == 1) 401 -> rejected else 200 -> chat,
      responseHeaders = Map("Retry-After" -> "120"))) { server =>
      val data = spark.createDataFrame(Collections.singletonList(Row("hello")),
        new StructType().add("text", "string"))
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
        .setOutputCol("answer").setErrorCol("error").setRawResponseCol("raw")
      stage.setHandler { (client, data) =>
        val request = data.toHTTPCore
        try HandlingUtils.convertAndClose(HandlingUtils.sendWithRetryPolicy(
          client, request, Array(0, 0), Set.empty, 0, boundedPolicy = true, quiet = true,
          refreshAuth = Some(_ => "MwcToken fresh")))
        finally request.releaseConnection()
      }
      val row = stage.transform(data).head()
      assert(row.isNullAt(row.fieldIndex("answer")))
      assert(row.getAs[Row]("error").getString(0).contains("http_error"))
      assert(row.getAs[Row]("error").getString(0).contains("HTTP 401"))
      assert(row.getAs[String]("raw") == rejected && server.requests.size() == 1)
    }
  }

  test("Fabric refresh failures become terminal HTTP row errors instead of Spark task failures") {
    val rejected = """{"error":{"code":"Unauthorized","message":"Synthetic rejection"}}"""
    Using.resource(new AIRecordingServer((_, _) => 401 -> rejected)) { server =>
      val data = spark.createDataFrame(Collections.singletonList(Row("hello")),
        new StructType().add("text", "string"))
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
        .setOutputCol("answer").setErrorCol("error").setRawResponseCol("raw")
      stage.setHandler { (client, data) =>
        val request = data.toHTTPCore
        try HandlingUtils.convertAndClose(HandlingUtils.sendWithRetryPolicy(
          client, request, Array(0, 0), Set.empty, 0, boundedPolicy = true, quiet = true,
          refreshAuth = Some(_ => throw new NoSuchMethodException("Synthetic token refresh failure"))))
        finally request.releaseConnection()
      }
      val row = stage.transform(data).head()
      assert(row.isNullAt(row.fieldIndex("answer")))
      assert(row.getAs[Row]("error").getString(0).contains("http_error"))
      assert(row.getAs[Row]("error").getString(0).contains("HTTP 401"))
      assert(row.getAs[String]("raw") == rejected && server.requests.size() == 1)
    }
  }
}
