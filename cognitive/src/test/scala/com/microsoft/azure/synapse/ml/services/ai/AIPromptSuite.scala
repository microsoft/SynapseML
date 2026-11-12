// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.core.test.fuzzing.TestObject
import com.microsoft.azure.synapse.ml.core.utils.ModelEquality
import com.microsoft.azure.synapse.ml.io.http.{HTTPSchema, URLKey}
import com.microsoft.azure.synapse.ml.param.GlobalParams
import com.microsoft.azure.synapse.ml.services.openai.{OpenAIDecisions, OpenAIDeploymentNameKey,
  OpenAIEmbeddingDeploymentNameKey, OpenAIPrompt}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Encoders, Row}
import org.apache.spark.sql.types._
import spray.json._

import java.nio.file.Files
import scala.collection.JavaConverters._
import scala.util.Using

class AIPromptSuite extends AIPromptFuzzing[AIPrompt] {
  override def reader: MLReadable[_] = AIPrompt

  override def testObjects(): Seq[TestObject[AIPrompt]] = Seq(new TestObject(
    new AIPrompt("aiprompt-fuzzing").setUrl("https://example.invalid/v1").setDeploymentName("synthetic-model")
      .setSubscriptionKey("synthetic-key").setInputCol("text").setOutputCol("answer").setErrorCol("error")
      .setExtraBody(Map("metadata" -> Map("source" -> "fuzzing"))),
    spark.createDataFrame(Seq(Row(Option.empty[String].orNull)).asJava, new StructType().add("text", StringType))))

  private val chatReply =
    """{"id":"chat-1","choices":[{"index":0,"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}],
      |"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3},"unknown":{"retained":true}}""".stripMargin
  private val responseReply =
    """{"id":"response-1","status":"completed","output":[
      |{"type":"message","role":"assistant","content":[{"type":"output_text","text":"one"},
      |{"type":"output_text","text":"two"}]},
      |{"type":"function_call","arguments":"not prose"},
      |{"type":"message","role":"assistant","content":[{"type":"output_text","text":"three"}]}],
      |"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3},"unknown":true}""".stripMargin
  private val embeddingReply =
    """{"data":[{"index":0,"embedding":[0.25,0.5]}],"model":"returned-model",
      |"usage":{"prompt_tokens":2,"total_tokens":2}}""".stripMargin

  private def input: DataFrame = spark.createDataFrame(
    Seq(Row("hello"), Row("world")).asJava, new StructType().add("text", StringType))

  private def prompt(server: AIRecordingServer, api: String = "chat_completions"): AIPrompt =
    new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setDeploymentName("explicit-model")
      .setInputCol("text").setOutputCol("answer").setErrorCol("error")
      .setRawResponseCol("raw").setUsageCol("usage")

  test("R constructors preserve nested JSON values as single escaped arguments and retain column bindings") {
    val questions = NativeRequestFixtures.DecisionQuestions
    val body = """{"items":[{"quoted":"a \"quote\" and \\ path\nline","value":true},{"value":null}]}"""
    val cases = Seq(
      (new OpenAIDecisions().setQuestions(questions), "questions", questions),
      (new AISystemOne().setQuestions(NativeRequestFixtures.SystemQuestions),
        "questions", NativeRequestFixtures.SystemQuestions),
      (new AIPrompt().setExtraBody(body), "extraBody", body))
    def argument(stage: AIPrompt, name: String): String = {
      val prefix = s",$name="
      rTestInstantiateModel(stage, 0).split("\n").map(_.trim)
        .find(_.startsWith(prefix)).getOrElse(fail(s"Missing R argument $name")).stripPrefix(prefix)
    }
    cases.foreach { case (stage, name, expected) =>
      val json = AIJson.parse(argument(stage, name)) match {
        case JsString(value) => value
        case _ => fail(s"$name must be one JSON string argument")
      }
      assert(AIJson.parse(json) == AIJson.parse(expected))
      name match {
        case "questions" => stage.setQuestions(json).setQuestionsCol("json_column")
        case "extraBody" => stage.setExtraBody(json).setExtraBodyCol("json_column")
      }
      assert(argument(stage, name + "Col") == "\"json_column\"")
    }
  }

  test("R fuzzing fixtures preserve implicit UID-derived defaults for all three public stages") {
    val constructors = Seq[String => AIPrompt](
      uid => new AIPrompt(uid), uid => new AISystemOne(uid), uid => new OpenAIDecisions(uid))
    constructors.foreach { create =>
      def configured(uid: String): AIPrompt =
        create(uid).setModel("model").setInputCol("text").setOutputCol("answer").setErrorCol("error")
      val original = configured("r-constructor-source")
      val uidArgument = rTestInstantiateModel(original, 0).split("\n").map(_.trim)
        .find(_.startsWith(",uid = ")).getOrElse(fail("Missing generated R UID"))
        .stripPrefix(",uid = ").stripSuffix(")")
      val uid = AIJson.parse(uidArgument) match {
        case JsString(value) => value
        case _ => fail("Expected a string UID")
      }
      val recreated = configured(uid)
      assert(!recreated.isSet(recreated.messagesCol) && !recreated.isSet(recreated.responseIdCol))
      ModelEquality.assertEqual(original, recreated)
    }
  }

  test("all new stages are lazy and retain original columns, raw JSON and usage") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val stage = prompt(server)
      val result = stage.transform(input)
      assert(server.requests.isEmpty)
      assert(result.schema == stage.transformSchema(input.schema))
      val rows = result.collect()
      assert(rows.forall(_.getAs[Row]("error") == null))
      assert(rows.map(_.getAs[String]("answer")).toSeq == Seq("answer", "answer"))
      assert(server.requests.size() == 2)
      assert(result.collect().forall(_.getAs[String]("raw").contains("retained")))
      assert(server.requests.size() == 4)
      assert(server.requests.asScala.forall { case (path, body) =>
        path == "/v1/chat/completions" && AIJson.objectString(body).fields("model") == JsString("explicit-model")
      })
    }
  }

  test("Responses selector is explicit, concatenates assistant text, and preserves non-text items") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> responseReply)) { server =>
      val stage = prompt(server, "responses").setStore(true).setResponseIdCol("response_id")
        .setExtraBody(Map("text" -> Map("verbosity" -> "low"), "provider_extension" -> Seq(1, 2)))
      val df = stage.transform(input.limit(1))
      assert(df.schema == stage.transformSchema(input.limit(1).schema))
      val row = df.head()
      assert(row.getAs[String]("answer") == "onetwothree")
      assert(row.getAs[String]("response_id") == "response-1")
      assert(row.getAs[String]("raw").contains("not prose"))
      val (path, body) = server.requests.peek()
      assert(path == "/v1/responses")
      val payload = AIJson.objectString(body)
      assert(payload.fields("text").asJsObject.fields("verbosity") == JsString("low"))
      assert(payload.fields("provider_extension") == JsArray(JsNumber(1), JsNumber(2)))
    }
  }

  test("fused Dataset maps do not require input Rows to carry schemas") {
    val original = input
    val mapped = original.map(row => Row.fromSeq(row.toSeq))(Encoders.row(original.schema))
    val cases = Seq(
      ("chat_completions", chatReply, None),
      ("responses", responseReply, None),
      ("embeddings", embeddingReply, None),
      ("systemone", NativeRequestFixtures.SystemResponse, Some(NativeRequestFixtures.SystemQuestions)),
      ("decisions", NativeRequestFixtures.DecisionResponse, Some(NativeRequestFixtures.DecisionQuestions)))
    cases.foreach { case (api, reply, questions) =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val stage = prompt(server, api)
        if (api == "systemone") stage.setUrl(server.url)
        questions.foreach(stage.setQuestions)
        if (Set("systemone", "decisions", "embeddings")(api)) {
          val adapter = new AIRequestAdapter(stage, original.schema)
          val (_, body) = adapter.request(Row("schema-less"), None, None)
          val field = if (api == "systemone") "state" else "input"
          assert(AIJson.objectString(body).fields(field) == JsString("schema-less"))
        }
        val result = stage.transform(mapped)
        assert(result.schema == stage.transformSchema(original.schema))
        val rows = result.collect()
        assert(rows.forall(_.getAs[Row]("error") == null))
        assert(rows.forall(!_.isNullAt(result.schema.fieldIndex("answer"))))
        assert(server.requests.size() == 2)
      }
    }
  }

  test("complete routes are not appended twice and contradictory API endpoints fail preflight") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> responseReply)) { server =>
      prompt(server, "responses").setUrl(server.url + "/v1/responses").transform(input.limit(1)).collect()
      assert(server.requests.peek()._1 == "/v1/responses")
      prompt(server, "responses").setCustomUrlRoot(server.url + "/proxy?version=custom")
        .transform(input.limit(1)).collect()
      assert(server.requests.asScala.last._1 == "/proxy?version=custom")
      intercept[IllegalArgumentException] {
        prompt(server, "responses").setCustomUrlRoot(server.url + "/v1/chat/completions")
          .transformSchema(input.schema)
      }
      assert(server.requests.size() == 2)
    }
  }

  test("complete deployment routes reject scalar and row mismatches without sending") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val address = server.url + "/openai/deployments/explicit-model/chat/completions"
      prompt(server).setUrl(address).transform(input.limit(1)).collect()
      assert(server.requests.peek()._1.startsWith("/openai/deployments/explicit-model/chat/completions?"))
      assert(server.requests.peek()._1.count(_ == '/') == 5)
      intercept[IllegalArgumentException] {
        prompt(server).setUrl(address).setDeploymentName("different").transformSchema(input.schema)
      }
      val row = prompt(server).setUrl(address).setDeploymentNameCol("text").transform(input.limit(1)).head()
      assert(row.getAs[Row]("error").getString(0).contains("invalid_request"))
      intercept[IllegalArgumentException] {
        prompt(server).setUrl(server.url + "/models/chat/completions").transformSchema(input.schema)
      }
      assert(server.requests.size() == 1)
      val models = prompt(server).setUrl(server.url + "/models/chat/completions")
      models.clear(models.deploymentName)
      models.setModel("model").transform(input.limit(1)).collect()
      assert(server.requests.asScala.last._1.startsWith("/models/chat/completions?"))
      assert(server.requests.size() == 2)
    }
  }

  test("map-backed chat content rejects empty arrays unsupported types and malformed text before HTTP") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model")
        .setMessagesCol("messages").setOutputCol("answer").setErrorCol("error")
      val rows = stage.transform(AIChatContentFixtures.mixed(spark)).collect()
      val errors = rows.filter(_.getAs[Row]("error") != null)
      assert(rows.length == 6 && errors.length == 5)
      assert(errors.forall(_.getAs[Row]("error").getString(0).contains("invalid_request")))
      assert(rows.filter(_.getAs[Row]("error") == null).map(_.getAs[String]("answer")).toSeq == Seq("answer"))
      assert(server.requests.size() == 1 && server.requests.peek()._2.contains("valid"))
    }
  }

  test("planning uses the input session resolver and restores the active session") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val session = spark.newSession()
      session.conf.set("spark.sql.caseSensitive", "true")
      val previous = org.apache.spark.sql.SparkSession.getActiveSession
      org.apache.spark.sql.SparkSession.setActiveSession(spark)
      try {
        val schema = new StructType().add("text", StringType).add("TEXT", StringType)
        val data = session.createDataFrame(Seq(Row("lower", "upper")).asJava, schema)
        val stage = prompt(server).setPromptTemplate("{TEXT}")
        stage.clear(stage.inputCol)
        val result = stage.transform(data)
        assert(result.head().getAs[String]("answer") == "answer")
        assert(server.requests.peek()._2.contains("upper"))
        assert(org.apache.spark.sql.SparkSession.getActiveSession.contains(spark))
      } finally {
        previous match {
          case Some(active) => org.apache.spark.sql.SparkSession.setActiveSession(active)
          case None => org.apache.spark.sql.SparkSession.clearActiveSession()
        }
      }
    }
  }

  test("embedding user scalar and column reach HTTP and explicit deployment beats the embedding default") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> embeddingReply)) { server =>
      val previous = GlobalParams.getGlobalParam(OpenAIEmbeddingDeploymentNameKey)
      GlobalParams.setGlobalParam(OpenAIEmbeddingDeploymentNameKey, Left("wrong-global"))
      try {
        val stage = prompt(server, "embeddings").setUser("scalar-user")
        val result = stage.transform(input.limit(1))
        assert(result.schema == stage.transformSchema(input.limit(1).schema))
        assert(result.head().getAs[Vector]("answer").toArray.toSeq == Seq(0.25, 0.5))
        assert(AIJson.objectString(server.requests.peek()._2).fields("user") == JsString("scalar-user"))
        stage.setUserCol("text").transform(input.limit(1)).collect()
        assert(AIJson.objectString(server.requests.asScala.last._2).fields("user") == JsString("hello"))
        assert(server.requests.asScala.forall(r =>
          AIJson.objectString(r._2).fields("model") == JsString("explicit-model")))
        assert(GlobalParams.getGlobalParam(OpenAIEmbeddingDeploymentNameKey).contains(Left("wrong-global")))
      } finally {
        previous match {
          case Some(value) => GlobalParams.setGlobalParam(OpenAIEmbeddingDeploymentNameKey, value)
          case None => GlobalParams.resetGlobalParam(OpenAIEmbeddingDeploymentNameKey)
        }
      }
    }
  }

  test("endpoint routing uses case-insensitive hosts without overriding explicit API selection") {
    val root = "https://RESOURCE.SERVICES.AI.AZURE.COM"
    val stage = new AIPrompt().setUrl(root).setModel("model").setInputCol("text")
    stage.transformSchema(input.schema)
    val chat = new AIRequestAdapter(stage, input.schema)
    val (chatUrl, _) = chat.request(Row("hello"), None,
      Some(Seq(new org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema(
        Array[Any]("user", "hello"), new StructType().add("role", StringType).add("content", StringType)))))
    assert(chatUrl == root + "/models/chat/completions?api-version=2025-04-01-preview")
    stage.setApiType("responses")
    stage.transformSchema(input.schema)
    val responses = new AIRequestAdapter(stage, input.schema)
    val (responsesUrl, _) = responses.request(Row("hello"), None,
      Some(Seq(new org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema(
        Array[Any]("user", "hello"), new StructType().add("role", StringType).add("content", StringType)))))
    assert(responsesUrl == root + "/openai/responses?api-version=2025-04-01-preview")
    intercept[IllegalArgumentException] {
      new AISystemOne().setUrl("https://API.OPENAI.COM").setDeploymentName("model")
        .setStateCol("text").setQuestions(NativeRequestFixtures.SystemQuestions).transformSchema(input.schema)
    }
    intercept[IllegalArgumentException] {
      new com.microsoft.azure.synapse.ml.services.openai.OpenAIDecisions().setUrl(root)
        .setModel("model").setInputCol("text").setQuestions(NativeRequestFixtures.DecisionQuestions)
        .transformSchema(input.schema)
    }
  }

  test("implicit Fabric credentials follow the effective destination and never a non-Fabric global URL") {
    val previous = GlobalParams.getGlobalParam(URLKey)
    val root = "https://fabric.example.invalid/cognitive/openai"
    val reply = chatReply
    try {
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        GlobalParams.setGlobalParam(URLKey, server.url + "/v1")
        val stage = new TestableAIFabricAuthPrompt("fabric-global-test").setDeploymentName("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error")
        val row = stage.transform(input.limit(1)).head()
        assert(row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null)
        assert(!server.headers.peek().contains("authorization"))
      }
      val cases = Seq(root -> true, (root + "-other") -> false,
        "https://other.example.invalid/cognitive/openai" -> false,
        root.replace("https:", "http:") -> false,
        root.replace(".invalid/", ".invalid:444/") -> false,
        (root + "/../outside") -> false, (root + "/%2e%2e/outside") -> false)
      cases.foreach { case (url, expectedFabric) =>
        GlobalParams.setGlobalParam(URLKey, url)
        val stage = new TestableAIFabricAuthPrompt("fabric-destination-test").setDeploymentName("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error")
        stage.setHandler { (_, request) =>
          assert(request.usesFabricAuth == expectedFabric, s"Unexpected Fabric fallback for $url")
          assert(request.authorizationHeader == (if (expectedFabric) Some("MwcToken synthetic") else None))
          HTTPSchema.stringToResponse(reply, 200, "OK")
        }
        assert(stage.transform(input.limit(1)).head().getAs[String]("answer") == "answer")
      }
      GlobalParams.setGlobalParam(URLKey, root)
      Seq("url", "customRoot", "credential").foreach { explicit =>
        val stage = new TestableAIFabricAuthPrompt("fabric-explicit-test").setDeploymentName("model")
          .setInputCol("text").setOutputCol("answer")
        explicit match {
          case "url" => stage.setUrl(root)
          case "customRoot" => stage.setCustomUrlRoot(root + "/v1/chat/completions")
          case "credential" => stage.setSubscriptionKey("synthetic-explicit-key")
        }
        stage.setHandler { (_, request) =>
          assert(!request.usesFabricAuth && request.authorizationHeader.isEmpty)
          HTTPSchema.stringToResponse(reply, 200, "OK")
        }
        assert(stage.transform(input.limit(1)).head().getAs[String]("answer") == "answer")
      }
    } finally {
      previous match {
        case Some(value) => GlobalParams.setGlobalParam(URLKey, value)
        case None => GlobalParams.resetGlobalParam(URLKey)
      }
    }
  }

  test("explicit model bindings ignore unused global deployment columns and scalar values") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> embeddingReply)) { server =>
      val previous = GlobalParams.getGlobalParam(OpenAIDeploymentNameKey)
      val stage = prompt(server, "embeddings")
      stage.clear(stage.deploymentName)
      stage.setModel("explicit-model")
      try {
        GlobalParams.resetGlobalParam(OpenAIDeploymentNameKey)
        val originalSchema = stage.transformSchema(input.schema)
        Seq(Right("absent-global-column"), Left("")).foreach { default =>
          GlobalParams.setGlobalParam(OpenAIDeploymentNameKey, default)
          val effective = stage.effectiveCopy()
          assert(effective.transformSchema(input.schema) == originalSchema)
          val row = stage.transform(input.limit(1)).head()
          assert(row.getAs[Row]("error") == null)
          assert(AIJson.objectString(server.requests.asScala.last._2).fields("model") == JsString("explicit-model"))
          stage.copy(ParamMap.empty).setModelCol("text").transform(input.limit(1)).collect()
          assert(AIJson.objectString(server.requests.asScala.last._2).fields("model") == JsString("hello"))
          assert(GlobalParams.getGlobalParam(OpenAIDeploymentNameKey).contains(default))
        }
      } finally {
        previous match {
          case Some(value) => GlobalParams.setGlobalParam(OpenAIDeploymentNameKey, value)
          case None => GlobalParams.resetGlobalParam(OpenAIDeploymentNameKey)
        }
      }
    }
  }

  test("malformed response format columns remain row errors without replaying valid requests") {
    Seq("chat_completions" -> chatReply, "responses" -> responseReply).foreach { case (api, reply) =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val data = spark.createDataFrame(Seq(
          Row("valid", """{"type":"json_object"}"""),
          Row("invalid number", """{"type":"json_schema","json_schema":42}"""),
          Row("invalid null", """{"type":"json_schema","json_schema":null}"""),
          Row("invalid array", """{"type":"json_schema","json_schema":[]}"""),
          Row("invalid type", """{"type":null}""")).asJava,
          new StructType().add("text", StringType).add("format", StringType))
        val rows = prompt(server, api).setResponseFormatCol("format").transform(data.coalesce(1)).collect()
        assert(rows.head.getAs[Row]("error") == null)
        assert(rows.tail.forall(_.getAs[Row]("error").getString(0).contains("invalid_request")))
        assert(server.requests.size() == 1)
      }
    }
  }

  test("malformed row extensions and null explicit model produce terminal row errors without HTTP") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> embeddingReply)) { server =>
      val schema = new StructType().add("text", StringType).add("body", StringType).add("deployment", StringType)
      val data = spark.createDataFrame(Seq(
        Row("ok", """{"encoding_format":"float"}""", "row-model"),
        Row("bad", """{"encoding_format":"base64"}""", "row-model"),
        Row("duplicate", """{"x":1,"x":2}""", "row-model"),
        Row("missing", "{}", Option.empty[String].orNull)).asJava, schema)
      val stage = prompt(server, "embeddings").setDeploymentNameCol("deployment").setExtraBodyCol("body")
      val result = stage.transform(data).collect()
      assert(result.length == 4)
      assert(result.count(_.getAs[Row]("error") != null) == 3)
      assert(server.requests.size() == 1)
      assert(result.last.getAs[Row]("error").getString(0).contains("missing_input"))
    }
  }

  test("deployment path segments reject dot routes and preserve encoded model names") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> embeddingReply)) { server =>
      val stage = prompt(server, "embeddings").setUrl(server.url)
      val invalid = Seq(".", "..", "../other", "foo/../../admin", ".\\admin", "x\\..\\admin",
        "%2e%2e%2fsecret", "..%252fsecret")
      invalid.foreach { model =>
        intercept[IllegalArgumentException](stage.setDeploymentName(model).transformSchema(input.schema))
      }
      val model = "a b/c+\u00e9"
      stage.setDeploymentName(model).transform(input.limit(1)).collect()
      assert(server.requests.peek()._1.startsWith("/openai/deployments/a%20b%2Fc%2B%C3%A9/embeddings?"))
      val data = spark.createDataFrame((invalid :+ model).map(Row(_)).asJava,
        new StructType().add("text", StringType))
      val rows = stage.setDeploymentNameCol("text").transform(data.coalesce(1)).collect()
      assert(rows.dropRight(1).forall(_.getAs[Row]("error").getString(0).contains("invalid_request")))
      assert(rows.last.getAs[Row]("error") == null && server.requests.size() == 2)
      stage.setUrl(server.url + "/v1").setDeploymentName(".").transform(input.limit(1)).collect()
      assert(AIJson.objectString(server.requests.asScala.last._2).fields("model") == JsString("."))
    }
  }

  test("HTTP failures retain usage and identity without replacing the authoritative status diagnostic") {
    val bodies = Seq(
      """{"id":"failed-id","usage":{"total_tokens":3},"error":{"code":"rejected"}}""",
      """{"id":42,"usage":{"total_tokens":3},"error":{"code":"rejected"}}""",
      """{"id":"failed-id","usage":{"total_tokens":-1},"error":{"code":"rejected"}}""",
      "not JSON")
    Using.resource(new AIRecordingServer((n, _) => 400 -> bodies(n - 1))) { server =>
      val stage = prompt(server, "responses").setResponseIdCol("response_id")
      val rows = bodies.map(_ => stage.transform(input.limit(1)).head())
      rows.foreach { row =>
        assert(row.getAs[Row]("error").getString(0).contains("http_error"))
        assert(row.getAs[Row]("error").getStruct(1).getInt(1) == 400)
      }
      assert(rows.head.getAs[Row]("usage").getLong(2) == 3L)
      assert(rows.head.getAs[String]("response_id") == "failed-id")
      assert(rows(1).getAs[Row]("usage").getLong(2) == 3L && rows(1).getAs[String]("response_id") == null)
      assert(rows(2).getAs[Row]("usage") == null && rows(2).getAs[String]("response_id") == "failed-id")
      assert(rows.last.getAs[String]("raw") == "not JSON")
    }
  }

  test("invalid scalar settings fail before actions and never send requests") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val invalid = Seq(
        prompt(server).setTemperature(0.5).setExtraBody(Map("temperature" -> 0.5)),
        prompt(server).setMaxCompletionTokens(5).setExtraBody(Map("max_tokens" -> 5)),
        prompt(server).setExtraBody(Map("model" -> "other")),
        prompt(server).setExtraBody(Map("stream" -> true)),
        prompt(server).setExtraBody(Map("background" -> true)),
        prompt(server, "embeddings").setExtraBody(Map("encoding_format" -> "base64")),
        prompt(server, "responses").setN(2))
      invalid.foreach(stage => intercept[IllegalArgumentException](stage.transform(input)))
      assert(server.requests.isEmpty)
    }
  }

  test("invalid endpoint and typed column bindings fail before any HTTP or external input reads") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      Seq("not a URL", "file:///tmp/model", "https://user:password@example.invalid",
        "https://example.invalid#fragment", "https://example.invalid?version=ignored").foreach { url =>
        intercept[IllegalArgumentException](prompt(server).setUrl(url).transformSchema(input.schema))
      }
      val data = input.withColumn("wrong_type", org.apache.spark.sql.functions.lit(1))
      val invalid = Seq(
        prompt(server).setTemperatureCol("wrong_type"),
        prompt(server).setStoreCol("wrong_type").setApiType("responses"),
        prompt(server).setUserCol("absent"),
        prompt(server).setModel("conflicts-with-explicit-deployment"),
        prompt(server).setText("embedding-only"),
        prompt(server).setStateCol("text"),
        prompt(server, "embeddings").setSystemPrompt("unsupported"),
        prompt(server, "embeddings").setDimensions(-1),
        prompt(server, "responses").setApiVersionCol("wrong_type"),
        prompt(server).setPromptTemplate("{text}"))
      invalid.foreach(stage => intercept[IllegalArgumentException](stage.transformSchema(data.schema)))
      assert(server.requests.isEmpty)
    }
  }

  test("native scalar credentials and row headers reach the selected protocol without entering the body") {
    val cases = Seq(
      ("systemone", NativeRequestFixtures.SystemResponse, NativeRequestFixtures.SystemQuestions, "api-key"),
      ("decisions", NativeRequestFixtures.DecisionResponse, NativeRequestFixtures.DecisionQuestions, "authorization"))
    cases.foreach { case (api, reply, questions, keyHeader) =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val stage = prompt(server, api).setQuestions(questions).setSubscriptionKey("synthetic-api-key")
        if (api == "systemone") stage.setUrl(server.url)
        stage.transform(input.limit(1)).collect()
        val expected = if (api == "decisions") "Bearer synthetic-api-key" else "synthetic-api-key"
        assert(server.headers.peek()(keyHeader) == expected)
        assert(!server.requests.peek()._2.contains("synthetic-api-key"))
        stage.clear(stage.subscriptionKey)
        stage.setAADTokenCol("text").transform(input.limit(1)).collect()
        assert(server.headers.asScala.last("authorization") == "Bearer hello")
      }
    }
  }

  test("nested extension siblings merge but explicitly owned format subtree cannot be replaced") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> responseReply)) { server =>
      val stage = prompt(server, "responses").setResponseFormat("json_object")
        .setExtraBody(Map("text" -> Map("verbosity" -> "low")))
      stage.transform(input.limit(1)).collect()
      val text = AIJson.objectString(server.requests.peek()._2).fields("text").asJsObject
      assert(text.fields.keySet == Set("format", "verbosity"))
      stage.setExtraBody(Map("text" -> Map("format" -> Map("type" -> "json_object"))))
      intercept[IllegalArgumentException](stage.transform(input))
      assert(server.requests.size() == 1)
    }
  }

  test("output roles replace inputs in place and reject resolver aliases") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val metadata = new MetadataBuilder().putString("description", "retained input metadata").build()
      val data = spark.createDataFrame(Seq(Row("hello", 7, "keep")).asJava,
        new StructType().add("text", StringType).add("answer", IntegerType)
          .add("a.b", StringType, nullable = false, metadata))
      val stage = prompt(server)
      val result = stage.transform(data)
      assert(result.schema == stage.transformSchema(data.schema))
      assert(result.schema.fieldNames.take(3).toSeq == Seq("text", "answer", "a.b"))
      assert(result.head().getAs[String]("a.b") == "keep")
      val old = spark.conf.get("spark.sql.caseSensitive")
      try {
        spark.conf.set("spark.sql.caseSensitive", "false")
        intercept[IllegalArgumentException](stage.setUsageCol("ANSWER").transformSchema(data.schema))
        spark.conf.set("spark.sql.caseSensitive", "true")
        assert(stage.transformSchema(data.schema).fieldNames.contains("ANSWER"))
      } finally spark.conf.set("spark.sql.caseSensitive", old)
    }
  }

  test("missing input stays in output and bounded retry counts physical sends") {
    Using.resource(new AIRecordingServer((attempt, _) =>
      if (attempt == 1) 429 -> """{"error":{"message":"retry"}}""" else 200 -> chatReply)) { server =>
      val data = spark.createDataFrame(Seq(Row(Option.empty[String].orNull), Row("valid")).asJava,
        new StructType().add("text", StringType))
      val rows = prompt(server).transform(data.coalesce(1)).collect()
      assert(rows.length == 2)
      assert(rows.head.getAs[Row]("error").getString(0).contains("missing_input"))
      assert(rows.last.getAs[String]("answer") == "answer")
      assert(server.requests.size() == 2)
    }
  }

  test("scalar and column setters obey last setter wins and copy/save/load preserve behavior") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val stage = prompt(server).setExtraBodyCol("does_not_exist").setExtraBody(Map("custom" -> true))
      val copied = stage.copy(ParamMap.empty)
      val directory = Files.createTempDirectory("aiprompt-params-").resolve("stage").toString
      try {
        copied.write.session(spark).save(directory)
        val loaded = AIPrompt.load(directory)
        assert(loaded.getExtraBody == stage.getExtraBody)
        assert(!loaded.isSet(loaded.store))
        assert(!loaded.isSet(loaded.postProcessing))
        assert(!loaded.isSet(loaded.handler))
        assert(loaded.transform(input.limit(1)).head().getAs[String]("answer") == "answer")
      } finally {
        org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File(directory).getParentFile)
      }
    }
  }

  test("new API selectors do not change OpenAIPrompt's accepted API types") {
    intercept[IllegalArgumentException](new OpenAIPrompt().setApiType("systemone"))
    assert(new AISystemOne().getApiType == "systemone")
    assert(new com.microsoft.azure.synapse.ml.services.openai.OpenAIDecisions().getApiType == "decisions")
    intercept[IllegalArgumentException](new AISystemOne().setApiType("chat_completions"))
  }

  test("JSON-valued body parameters preserve exact numbers and JVM arrays through save load") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val extension = Map[String, Any](
        "array" -> Array(1, 2),
        "decimal" -> BigDecimal("0.12345678901234567890123456789"),
        "integer" -> BigInt("9223372036854775808123456789"),
        "nested" -> java.util.Arrays.asList("value", java.lang.Boolean.TRUE))
      val expected = AIJson.fromAny(extension)
      val stage = prompt(server).setExtraBody(extension)
      val directory = Files.createTempDirectory("aiprompt-json-params-").resolve("stage").toString
      try {
        stage.copy(ParamMap.empty).write.session(spark).save(directory)
        val loaded = AIPrompt.load(directory)
        assert(AIJson.fromAny(loaded.getExtraBody) == expected)
        val output = stage.transformSchema(input.schema)
        assert(loaded.transformSchema(input.schema) == output)
        assert(loaded.transform(input.limit(1)).head().getAs[String]("answer") == "answer")
        val body = AIJson.objectString(server.requests.peek()._2)
        expected.asJsObject.fields.foreach { case (key, value) => assert(body.fields(key) == value) }
        val questions = """[{"type":"score","minimum":0.12345678901234567890123456789}]"""
        stage.setQuestions(questions)
        val encoded = stage.questions.jsonEncode(stage.get(stage.questions).get)
        val decoded = stage.questions.jsonDecode(encoded).left.get
        assert(AIJson.fromAny(decoded) == AIJson.parse(questions))
      } finally {
        org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File(directory).getParentFile)
      }
    }
  }

  test("all supported chat scalar and column options reach their wire fields") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val stage = prompt(server).setMaxTokens(32).setTemperature(0.7).setTopP(0.8).setUser("caller")
        .setN(2).setStop("END").setPresencePenalty(0.1).setFrequencyPenalty(0.2).setSeed(7)
        .setVerbosity("low").setReasoningEffort("low").setResponseFormat("json_object")
      stage.transform(input.limit(1)).collect()
      val scalar = AIJson.objectString(server.requests.peek()._2).fields
      val expected = Map("max_completion_tokens" -> JsNumber(32), "temperature" -> JsNumber(0.7),
        "top_p" -> JsNumber(0.8), "user" -> JsString("caller"), "n" -> JsNumber(2), "stop" -> JsString("END"),
        "presence_penalty" -> JsNumber(0.1), "frequency_penalty" -> JsNumber(0.2), "seed" -> JsNumber(7),
        "verbosity" -> JsString("low"), "reasoning_effort" -> JsString("low"),
        "response_format" -> JsObject("type" -> JsString("json_object")))
      expected.foreach { case (key, value) => assert(scalar(key) == value, key) }
      val data = spark.createDataFrame(Seq(Row("row input", 11, 0.3, 0.4, "row-user", 1,
        "STOP", 0.5, 0.6, 42, "high", "high", """{"type":"json_object"}""")).asJava,
        new StructType().add("text", StringType).add("tokens", IntegerType)
          .add("temperature", DoubleType).add("top", DoubleType).add("user", StringType)
          .add("n", IntegerType).add("stop", StringType).add("presence", DoubleType)
          .add("frequency", DoubleType).add("seed", IntegerType).add("verbosity", StringType)
          .add("reasoning", StringType).add("format", StringType))
      stage.setMaxTokensCol("tokens").setTemperatureCol("temperature").setTopPCol("top").setUserCol("user")
        .setNCol("n").setStopCol("stop").setPresencePenaltyCol("presence").setFrequencyPenaltyCol("frequency")
        .setSeedCol("seed").setVerbosityCol("verbosity").setReasoningEffortCol("reasoning")
        .setResponseFormatCol("format").transform(data).collect()
      val column = AIJson.objectString(server.requests.asScala.last._2).fields
      assert(column("max_completion_tokens") == JsNumber(11))
      assert(column("temperature") == JsNumber(0.3) && column("top_p") == JsNumber(0.4))
      assert(column("user") == JsString("row-user") && column("n") == JsNumber(1))
      assert(column("stop") == JsString("STOP") && column("presence_penalty") == JsNumber(0.5))
      assert(column("frequency_penalty") == JsNumber(0.6) && column("seed") == JsNumber(42))
      assert(column("verbosity") == JsString("high") && column("reasoning_effort") == JsString("high"))
      assert(column("response_format") == expected("response_format"))
    }
  }

  test("Responses column options map to nested wire paths and do not rewrite postprocessing") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> responseReply)) { server =>
      val data = spark.createDataFrame(Seq(Row("hello", true, "prior", """{"type":"json_object"}""")).asJava,
        input.schema.add("store", BooleanType).add("prior", StringType).add("format", StringType))
      val stage = prompt(server, "responses").setStoreCol("store").setPreviousResponseIdCol("prior")
        .setResponseFormatCol("format").setVerbosity("low").setReasoningEffort("medium").setMaxCompletionTokens(20)
      val result = stage.transform(data).head()
      assert(result.getAs[String]("answer") == "onetwothree")
      val body = AIJson.objectString(server.requests.peek()._2).fields
      assert(body("store") == JsTrue && body("previous_response_id") == JsString("prior"))
      assert(body("max_output_tokens") == JsNumber(20))
      assert(body("text").asJsObject.fields == Map("format" -> JsObject("type" -> JsString("json_object")),
        "verbosity" -> JsString("low")))
      assert(body("reasoning") == JsObject("effort" -> JsString("medium")))
    }
  }

  test("postprocessing and retained messages have matching planned schemas and safe scratch columns") {
    Using.resource(new AIRecordingServer((_, _) => 200 ->
      chatReply.replace("answer", """{\"value\":\"ok\"}"""))) { server =>
      val stage = prompt(server).setPromptTemplate("Read {`a.b`}").setDropPrompt(false)
        .setPostProcessingOptions(Map("jsonSchema" -> "value STRING"))
      stage.clear(stage.inputCol)
      val data = spark.createDataFrame(Seq(Row("hello")).asJava, new StructType().add("a.b", StringType))
      val result = stage.transform(data)
      assert(result.schema == stage.transformSchema(data.schema))
      val row = result.head()
      assert(row.getAs[Row]("error") == null)
      assert(row.getAs[Row]("answer").getAs[String]("value") == "ok")
      assert(row.getSeq[Row](row.fieldIndex(stage.getMessagesCol)).nonEmpty)
    }
  }

  test("unsupported message fields become explicit row errors instead of disappearing") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val message = new StructType().add("role", StringType).add("content", StringType).add("tool_call_id", StringType)
      val data = spark.createDataFrame(Seq(Row(Seq(Row("tool", "answer", "call-1")))).asJava,
        new StructType().add("messages", ArrayType(message)))
      val stage = prompt(server).setMessagesCol("messages")
      stage.clear(stage.inputCol)
      assert(stage.transform(data).head().getAs[Row]("error").getString(0).contains("invalid_request"))
      assert(server.requests.isEmpty)
    }
  }

  test("raw message arrays and header maps use Spark-produced collections without losing names") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val message = new StructType().add("role", StringType).add("content", StringType).add("name", StringType)
      val data = spark.createDataFrame(Seq(Row(Seq(Row("user", "hello", "caller")),
        Map("X-Test-Trace" -> "recording"))).asJava,
        new StructType().add("messages", ArrayType(message))
          .add("headers", MapType(StringType, StringType)))
      val stage = prompt(server).setMessagesCol("messages")
      stage.setVectorParam(stage.customHeaders, "headers")
      stage.clear(stage.inputCol)
      val mapped = data.map { row =>
        Row(Seq(Row("user", "hello", "caller")), row.getMap[String, String](1).toMap)
      }(Encoders.row(data.schema))
      assert(stage.transform(mapped).head().getAs[String]("answer") == "answer")
      assert(server.headers.peek()("x-test-trace") == "recording")
      val messages = AIJson.objectString(server.requests.peek()._2).fields("messages").asInstanceOf[JsArray]
      assert(messages.elements.head.asJsObject.fields("name") == JsString("caller"))
    }
  }

  test("schema-less nested messages preserve inline media and map-backed content") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val part = new StructType().add("type", StringType).add("text", StringType)
        .add("image_url", MapType(StringType, StringType))
      val message = new StructType().add("role", StringType).add("content", ArrayType(part))
      val schema = new StructType().add("messages", ArrayType(message))
      val image = "data:image/png;base64,AQID"
      val data = spark.range(1).map { _ =>
        Row(Seq(Row("user", Seq(Row("image_url", Option.empty[String].orNull, Map("url" -> image))))))
      }(Encoders.row(schema))
      val stage = prompt(server).setMessagesCol("messages")
      stage.clear(stage.inputCol)
      assert(stage.transform(data).head().getAs[Row]("error") == null)
      val messages = AIJson.objectString(server.requests.peek()._2).fields("messages").asInstanceOf[JsArray]
      val content = messages.elements.head.asJsObject.fields("content").asInstanceOf[JsArray]
      assert(content.elements.head.asJsObject.fields("image_url").asJsObject.fields("url") == JsString(image))
    }
  }

  test("filename scratch columns never replace inputs or consume a public answer") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val data = spark.createDataFrame(
        Seq(Row("data:text/plain;base64,aGVsbG8=", "original", "also original")).asJava,
        new StructType().add("file.name", StringType).add("file.name_filename", StringType)
          .add("file.name_filename_1", StringType))
      val stage = prompt(server).setPromptTemplate("Read {`file.name`}")
        .setColumnTypes(Map("file.name" -> "path")).setOutputCol("file.name_filename_2")
      stage.clear(stage.inputCol)
      val result = stage.transform(data)
      assert(result.schema == stage.transformSchema(data.schema))
      val row = result.head()
      assert(row.getAs[String]("file.name_filename") == "original")
      assert(row.getAs[String]("file.name_filename_1") == "also original")
      assert(row.getAs[String]("file.name_filename_2") == "answer")
      assert(row.getAs[Row]("error") == null)
      assert(server.requests.peek()._2.contains("Read attachment.txt"))
      assert(!result.columns.contains("file.name_filename_3"))
    }
  }

  test("postprocessing failure retains raw content with a distinct parse diagnostic") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val stage = prompt(server).setPostProcessingOptions(Map("jsonSchema" -> "ARRAY<INT>"))
      val result = stage.transform(input.limit(1))
      assert(result.schema == stage.transformSchema(input.schema))
      val row = result.head()
      assert(row.isNullAt(row.fieldIndex("answer")))
      assert(row.getAs[Row]("error").getString(0).contains("parse_error"))
      assert(row.getAs[String]("raw").contains("answer"))
    }
  }

  test("malformed provider answers retain valid usage and raw JSON on the public output") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply.replace("\"answer\"", "42"))) { server =>
      val row = prompt(server).transform(input.limit(1)).head()
      assert(row.getAs[String]("answer") == None.orNull)
      assert(row.getAs[Row]("error").getString(0).contains("parse_error"))
      assert(row.getAs[Row]("usage").getLong(2) == 3L)
      assert(row.getAs[String]("raw").contains("\"content\":42"))
      assert(server.requests.size() == 1)
    }
  }

  test("template schema planning resolves literal names and rejects missing columns without HTTP") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val stage = prompt(server).setPromptTemplate("Read {missing}")
      stage.clear(stage.inputCol)
      val error = intercept[IllegalArgumentException](stage.transformSchema(input.schema))
      assert(error.getMessage.startsWith("Invalid prompt template") && error.getCause == null)
      assert(server.requests.isEmpty)
    }
  }

  test("bounded 429 retries terminate and preserve the final raw response") {
    Using.resource(new AIRecordingServer((_, _) => 429 -> """{"error":{"code":"rate_limit"}}""")) { server =>
      val row = prompt(server).transform(input.limit(1)).head()
      assert(server.requests.size() == 4)
      assert(row.getAs[Row]("error").getString(0).contains("http_error"))
      assert(row.getAs[String]("raw").contains("rate_limit"))
    }
  }
}
