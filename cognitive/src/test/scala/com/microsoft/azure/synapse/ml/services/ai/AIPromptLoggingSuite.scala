// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.io.http.{HTTPTransformer, HandlingUtils}
import com.microsoft.azure.synapse.ml.services.openai.{OpenAIChatCompletion, OpenAIDecisions, OpenAIResponses}
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.{Filter, LogEvent, LoggerContext}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.spark.ml.{Pipeline, PipelineModel, Serializer}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}

import java.io.{PrintWriter, StringWriter}
import java.net.InetSocketAddress
import java.nio.file.Files
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._
import scala.util.Using

private[ai] class TestableAIMutableDefaultHandlerPrompt(override val uid: String) extends AIPrompt(uid) {
  def replaceHandlerDefault(): this.type = setDefault(handler -> HandlingUtils.advancedUDF(0))
}

private[ai] class TestableAIAlternateDefaultHandlerPrompt(override val uid: String) extends AIPrompt(uid) {
  setDefault(handler -> HandlingUtils.advancedUDF(0))
}

private[ai] class TestableAIDefaultTimeoutPrompt(override val uid: String) extends AIPrompt(uid) {
  def setConcurrentTimeoutDefault(value: Double): this.type = setDefault(concurrentTimeout -> value)
}

private[ai] class TestableAIMutableJsonDefaultPrompt(override val uid: String) extends AIPrompt(uid) {
  def replaceJsonDefault[T](param: Param[T], value: T): this.type = setDefault(param -> value)
}

class AIPromptLoggingSuite extends AIPromptTestBase {
  private val chatReply =
    """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}]}"""

  private def input: DataFrame = spark.createDataFrame(
    Seq(Row("hello")).asJava, new StructType().add("text", StringType))

  private def withLogs(action: ConcurrentLinkedQueue[String] => Unit): Unit = {
    val logged = new ConcurrentLinkedQueue[String]()
    val layout = PatternLayout.newBuilder().withPattern("%m%n%throwable").build()
    val noFilter = Option.empty[Filter].orNull
    val appender = new AbstractAppender("aiprompt-test", noFilter, layout, false, Array.empty[Property]) {
      override def append(event: LogEvent): Unit = logged.add(layout.toSerializable(event))
    }
    val root = LogManager.getContext(false).asInstanceOf[LoggerContext].getRootLogger
    val loggers = Seq(classOf[AIPrompt], classOf[OpenAIChatCompletion], classOf[OpenAIResponses],
      classOf[HTTPTransformer])
      .map(cls => LogManager.getLogger(cls).asInstanceOf[org.apache.logging.log4j.core.Logger])
    val levels = loggers.map(logger => logger -> logger.getLevel)
    appender.start()
    root.addAppender(appender)
    loggers.foreach(_.setLevel(Level.INFO))
    try action(logged) finally {
      levels.foreach { case (logger, level) => logger.setLevel(level) }
      root.removeAppender(appender)
      appender.stop()
    }
  }

  test("request planning uses pure encoders without constructing legacy OpenAI stages") {
    val data = input
    Seq("chat_completions", "responses", "systemone", "decisions", "embeddings").foreach { api =>
      val endpoint = if (api == "systemone") "https://example.services.ai.azure.com" else "https://example.invalid/v1"
      val stage = new AIPrompt().setApiType(api).setUrl(endpoint).setModel("model").setInputCol("text")
      if (api == "systemone") stage.setQuestions(NativeRequestFixtures.SystemQuestions)
      if (api == "decisions") stage.setQuestions(NativeRequestFixtures.DecisionQuestions)
      withLogs { logged =>
        stage.transformSchema(data.schema)
        stage.transform(data)
        assert(logged.asScala.exists(_.contains("constructor")), "Constructor logging must be observable")
        Seq("OpenAIChatCompletion", "OpenAIResponses").foreach { legacy =>
          assert(!logged.asScala.exists(_.contains(s".$legacy")), s"Planning constructed $legacy for $api")
        }
      }
    }
  }

  Seq(false, true).foreach { succeeds =>
    test(s"public transform logging uses the execution session with succeeds=$succeeds") {
      val original = SparkSession.getActiveSession
      try {
        val previous = spark.newSession()
        val execution = spark.newSession()
        previous.conf.set("trident.artifact.id", "synthetic-previous-artifact")
        execution.conf.set("trident.artifact.id", "synthetic-execution-artifact")
        val data = execution.createDataFrame(
          Seq(Row("hello")).asJava, new StructType().add("text", StringType))
        withServer { server =>
          Seq(Some(previous), None).foreach { prior =>
            val stage = prompt(server)
            if (!succeeds) stage.setInputCol("missing")
            prior match {
              case Some(session) => SparkSession.setActiveSession(session)
              case None => SparkSession.clearActiveSession()
            }
            withLogs { logged =>
              def invoke(): DataFrame = stage.transform(data)
              if (succeeds) {
                val result = invoke()
                assert(SparkSession.getActiveSession == prior && result.sparkSession == execution)
                assert(result.count() == 1)
              } else {
                intercept[IllegalArgumentException](invoke())
                assert(SparkSession.getActiveSession == prior)
              }
              val messages = logged.asScala.filter(message =>
                message.contains(s""""modelUid":"${stage.uid}"""") && message.contains(""""method":"transform""""))
              assert(messages.size == 1)
              assert(messages.head.contains(""""artifactId":"synthetic-execution-artifact""""))
            }
          }
        }
      } finally {
        original match {
          case Some(session) => SparkSession.setActiveSession(session)
          case None => SparkSession.clearActiveSession()
        }
      }
    }
  }

  Seq(1, 2).foreach { concurrency =>
    test(s"bounded transport never replays response cookies across row credentials at concurrency $concurrency") {
      val keys = (1 to 4).map(number => s"synthetic-key-$number")
      val data = spark.createDataFrame(keys.map(key => Row("hello", key)).asJava,
        new StructType().add("text", StringType).add("credential", StringType)).coalesce(1)
      Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply,
        responseHeaders = Map("Set-Cookie" -> "provider-session=synthetic; Path=/"))) { server =>
        val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
          .setOutputCol("answer").setErrorCol("error").setSubscriptionKeyCol("credential")
          .setConcurrency(concurrency)
        val rows = stage.transform(data).collect()
        assert(rows.length == 4 && rows.forall(row =>
          row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null))
        val headers = server.headers.asScala.toVector
        assert(headers.size == 4 && headers.map(_("api-key")).sorted == keys.sorted)
        assert(headers.forall(!_.contains("cookie")))
      }
    }
  }

  test("bounded transport preserves caller cookies while explicit handlers retain legacy cookie management") {
    Seq(false, true).foreach { legacy =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply,
        responseHeaders = Map("Set-Cookie" -> "provider-session=synthetic; Path=/"))) { server =>
        val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
          .setOutputCol("answer").setErrorCol("error").setConcurrency(1)
        if (legacy) stage.setHandler(HandlingUtils.advancedUDF(0))
        else stage.setCustomHeaders(Map("Cookie" -> "caller-session=explicit"))
        val rows = stage.transform(input.union(input).coalesce(1)).collect()
        assert(rows.length == 2 && rows.forall(row =>
          row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null))
        val cookies = server.headers.asScala.toVector.map(_.get("cookie"))
        if (legacy) assert(cookies == Vector(None, Some("provider-session=synthetic")))
        else assert(cookies == Vector.fill(2)(Some("caller-session=explicit")))
      }
    }
  }

  test("redirects retain original HTTP diagnostics without forwarding or logging provider credentials") {
    Seq(false, true).foreach { truncated =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { target =>
        val source = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
        val received = new ConcurrentLinkedQueue[String]()
        source.createContext("/", (exchange: HttpExchange) => {
          received.add(exchange.getRequestMethod)
          Using.resource(exchange.getRequestBody)(_.readAllBytes())
          exchange.getResponseHeaders.add("Location", target.url + "/v1/chat/completions")
          exchange.sendResponseHeaders(303, if (truncated) 100 else -1)
          if (truncated) exchange.getResponseBody.write('{'.toInt)
          exchange.close()
        })
        source.start()
        try {
          withLogs { logged =>
            val key = "synthetic-redirect-key"
            Seq(1, 2).foreach { concurrency =>
              val stage = new AIPrompt().setUrl(s"http://127.0.0.1:${source.getAddress.getPort}/v1")
                .setModel("model").setInputCol("text").setOutputCol("answer").setErrorCol("error")
                .setRawResponseCol("raw").setSubscriptionKey(key).setConcurrency(concurrency).setTimeout(2)
              val row = stage.transform(input).head()
              assert(target.requests.isEmpty && target.headers.isEmpty)
              val error = row.getAs[Row]("error")
              assert(row.isNullAt(row.fieldIndex("answer")) && error.getString(0).contains("http_error"))
              assert(error.getStruct(1).getAs[Int]("statusCode") == 303)
              if (truncated) assert(row.getAs[String]("raw").contains("Could not read"))
            }
            assert(received.asScala.toSeq == Seq("POST", "POST"))
            assert(!logged.isEmpty && logged.asScala.forall(!_.contains(key)))
          }
        } finally source.stop(0)
      }
    }
  }

  for (binding <- Seq("constructor", "runtime", "explicit"); concurrency <- Seq(1, 2)) {
    test(s"custom handlers retain legacy redirects with $binding binding at concurrency $concurrency") {
      Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { target =>
        val source = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
        val received = new ConcurrentLinkedQueue[String]()
        source.createContext("/", (exchange: HttpExchange) => {
          received.add(exchange.getRequestMethod)
          Using.resource(exchange.getRequestBody)(_.readAllBytes())
          exchange.getResponseHeaders.add("Location", target.url + "/v1/chat/completions")
          exchange.sendResponseHeaders(303, -1)
          exchange.close()
        })
        source.start()
        try {
          val stage = (binding match {
            case "explicit" => new AIPrompt().setHandler(HandlingUtils.advancedUDF(0))
            case "runtime" => new TestableAIMutableDefaultHandlerPrompt("runtime-redirect").replaceHandlerDefault()
            case _ => new TestableAIAlternateDefaultHandlerPrompt("alternate-redirect")
          })
            .setUrl(s"http://127.0.0.1:${source.getAddress.getPort}/v1")
            .setModel("model").setInputCol("text").setOutputCol("answer").setErrorCol("error")
            .setConcurrency(concurrency)
          val restored = if (binding == "explicit") {
            val saved = path()
            stage.write.session(spark).save(saved)
            val config = Serializer.LegacyObjectDeserializationConfig
            val previous = spark.conf.getOption(config)
            try {
              spark.conf.unset(config)
              val error = intercept[SecurityException](AIPrompt.read.session(spark).load(saved))
              assert(error.getMessage.contains(config))
              spark.conf.set(config, "true")
              AIPrompt.read.session(spark).load(saved)
            } finally previous.fold(spark.conf.unset(config))(spark.conf.set(config, _))
          } else stage.copy(ParamMap.empty)
          assert(restored.isSet(restored.handler) == (binding == "explicit"))
          val row = restored.transform(input).head()
          assert(row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null)
          assert(received.asScala.toSeq == Seq("POST") && target.requests.size() == 1)
        } finally source.stop(0)
      }
    }
  }

  test("bounded default handlers load without unsafe Java deserialization opt-in") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val saved = path()
      new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text").setOutputCol("answer")
        .write.session(spark).save(saved)
      val config = Serializer.LegacyObjectDeserializationConfig
      val previous = spark.conf.getOption(config)
      try {
        spark.conf.unset(config)
        val restored = AIPrompt.read.session(spark).load(saved)
        assert(restored.transform(input).head().getAs[String]("answer") == "answer")
        assert(server.requests.size() == 1)
      } finally previous.fold(spark.conf.unset(config))(spark.conf.set(config, _))
    }
  }

  Seq(false, true).foreach { allowed =>
    test(s"custom handlers use their assigned Spark session deserialization policy with allowed=$allowed") {
      val config = Serializer.LegacyObjectDeserializationConfig
      val active = spark.newSession()
      active.conf.set(config, (!allowed).toString)
      val readerSession = spark.newSession()
      readerSession.conf.set(config, allowed.toString)
      val previous = SparkSession.getActiveSession
      SparkSession.setActiveSession(active)
      try {
        Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
          val saved = path()
          new AIPrompt().setHandler(HandlingUtils.advancedUDF(0))
            .setUrl(server.url + "/v1").setModel("model").setInputCol("text").setOutputCol("answer")
            .write.session(readerSession).save(saved)
          if (allowed) {
            val restored = AIPrompt.read.session(readerSession).load(saved)
            assert(restored.transform(input).head().getAs[String]("answer") == "answer")
            assert(server.requests.size() == 1)
          } else {
            val error = intercept[SecurityException](AIPrompt.read.session(readerSession).load(saved))
            assert(error.getMessage.contains(config) && server.requests.isEmpty)
          }
        }
      } finally previous.fold(SparkSession.clearActiveSession())(SparkSession.setActiveSession)
    }
  }

  for (binding <- Seq("explicit", "default", "default-copy", "default-load"); concurrency <- Seq(1, 2)) {
    test(s"concurrent timeout cancels bounded retry workers with $binding binding at concurrency $concurrency") {
      Using.resource(new AIRecordingServer((number, _) =>
        if (number <= concurrency) 429 -> "{}" else 200 -> chatReply, retryAfter = "3")) { server =>
        val configured = (if (binding == "explicit") new AIPrompt().setConcurrentTimeout(1)
          else new TestableAIDefaultTimeoutPrompt("default-timeout").setConcurrentTimeoutDefault(1))
          .setUrl(server.url + "/v1").setModel("model").setInputCol("text")
          .setConcurrency(concurrency).setTimeout(5)
        val stage = binding match {
          case "default-copy" => configured.copy(ParamMap.empty)
          case "default-load" =>
            val saved = tmpDir.resolve(s"timeout-$concurrency").toString
            configured.write.session(spark).save(saved)
            AIPrompt.read.session(spark).load(saved)
          case _ => configured
        }
        assert(stage.isSet(stage.concurrentTimeout) == (binding == "explicit"))
        intercept[org.apache.spark.SparkException] {
          stage.transform(input.union(input).coalesce(1)).collect()
        }
        assert(server.requests.size() == concurrency)
        Thread.sleep(4000)
        assert(server.requests.size() == concurrency, "Timed-out workers issued another paid request")
      }
    }
  }

  Seq("original", "copy", "deferred", "constructor").foreach { mode =>
    test(s"saving replaced handler defaults fails before overwriting existing files with mode=$mode") {
      val stage = new TestableAIMutableDefaultHandlerPrompt("runtime-default")
      val deferred = stage.write.session(spark)
      stage.replaceHandlerDefault()
      val candidate = mode match {
        case "copy" => stage.copy(ParamMap.empty)
        case "constructor" => new TestableAIAlternateDefaultHandlerPrompt("constructor-default")
        case _ => stage
      }
      val saved = Files.createTempDirectory(tmpDir, "handler-default")
      val marker = saved.resolve("keep")
      Files.write(marker, Array[Byte](1, 2, 3))
      val failure = intercept[IllegalArgumentException] {
        val writer = if (mode == "deferred") deferred else candidate.write.session(spark)
        writer.overwrite().save(saved.toString)
      }
      assert(failure.getMessage.contains("setHandler"))
      assert(Files.readAllBytes(marker).toSeq == Seq[Byte](1, 2, 3))
    }
  }

  test("public native writers reject invalid parsers before replacing saved stages") {
    Seq[() => AIPrompt](() => new AIPrompt(), () => new AISystemOne(), () => new OpenAIDecisions())
      .foreach { create =>
        Seq("csv", "json").foreach { parser =>
          Seq(false, true).foreach { deferred =>
            val saved = path()
            create().setPostProcessingOptions(Map("delimiter" -> "|")).write.session(spark).save(saved)
            val stage = create()
            if (parser == "csv") stage.setPostProcessingOptions(Map("delimiter" -> ";"))
            val writer = if (deferred) Some(stage.write.session(spark).overwrite()) else None
            if (parser == "csv") stage.clear(stage.postProcessing) else stage.set(stage.postProcessing, "json")
            val error = intercept[IllegalArgumentException] {
              writer.getOrElse(stage.write.session(spark).overwrite()).save(saved)
            }
            assert(error.getMessage == (if (parser == "csv") "postProcessing must be 'csv'"
              else "requirement failed: jsonSchema must be specified with json postProcessing"))
            val restored = stage match {
              case _: AISystemOne => AISystemOne.read.session(spark).load(saved)
              case _: OpenAIDecisions => OpenAIDecisions.read.session(spark).load(saved)
              case _ => AIPrompt.read.session(spark).load(saved)
            }
            assert(restored.getClass == stage.getClass)
            assert(restored.getPostProcessing == "csv" && restored.getPostProcessingOptions == Map("delimiter" -> "|"))
          }
        }
      }
  }

  test("unencodable generic JSON bindings fail before direct deferred and copied stage overwrites") {
    val saved = path()
    new AIPrompt().setExtraBody(Map("keep" -> 1)).write.session(spark).save(saved)
    def unchanged(): Unit = assert(AIPrompt.read.session(spark).load(saved).getExtraBody == Map("keep" -> 1))
    val invalid = Map[String, Any]("x" -> Double.NaN)
    Seq[() => AIPrompt](() => new AIPrompt(), () => new AISystemOne(), () => new OpenAIDecisions())
      .foreach { create =>
        val stage = create()
        def verify[T](param: Param[T], value: T): Unit = {
          val deferred = stage.write.session(spark).overwrite()
          val copied = stage.copy(ParamMap(param -> value))
          stage.set(param, value)
          Seq(() => stage.write.session(spark).overwrite().save(saved), () => deferred.save(saved),
            () => copied.write.session(spark).overwrite().save(saved)).foreach { save =>
            assert(intercept[IllegalArgumentException](save()).getMessage.contains("finite"))
            unchanged()
          }
          stage.clear(param)
        }
        verify(stage.extraBody, Left(invalid))
        verify(stage.responseFormat, Left(invalid))
        verify(stage.questions, Left(invalid))
      }
  }

  test("shadowed unencodable JSON defaults fail before overwriting a saved stage") {
    val saved = path()
    new AIPrompt().setExtraBody(Map("keep" -> 1)).write.session(spark).save(saved)
    val invalid = Map[String, Any]("x" -> Double.PositiveInfinity)
    Seq("extraBody", "responseFormat", "questions").foreach { name =>
      val stage = new TestableAIMutableJsonDefaultPrompt("invalid-json-default")
        .setExtraBody(Map("valid" -> 1)).setQuestions("""{"valid":1}""")
        .setResponseSchema(Map("type" -> "object"))
      val writer = stage.write.session(spark).overwrite()
      name match {
        case "extraBody" => stage.replaceJsonDefault(stage.extraBody, Left(invalid))
        case "responseFormat" => stage.replaceJsonDefault(stage.responseFormat, Left(invalid))
        case "questions" => stage.replaceJsonDefault(stage.questions, Left(invalid))
      }
      assert(intercept[IllegalArgumentException](writer.save(saved)).getMessage.contains("finite"))
      assert(AIPrompt.read.session(spark).load(saved).getExtraBody == Map("keep" -> 1))
    }
  }

  test("oversized native JSON fails before explicit default copied and deferred stage overwrites") {
    val large = Map[String, Any]("description" -> ("x" * (32 * 1024 * 1024)))
    val small = Map[String, Any]("keep" -> 1)
    Seq("explicit" -> "extraBody", "default" -> "responseFormat", "shadowed" -> "questions",
      "copied" -> "extraBody", "deferred" -> "responseFormat").foreach { case (mode, name) =>
      val saved = path()
      new AIPrompt().setExtraBody(small).write.session(spark).save(saved)
      val stage = new TestableAIMutableJsonDefaultPrompt(s"large-json-$mode")
      val deferred = stage.write.session(spark).overwrite()
      val param = name match {
        case "extraBody" => stage.extraBody
        case "responseFormat" => stage.responseFormat
        case "questions" => stage.questions
      }
      if (mode == "default" || mode == "shadowed") {
        stage.replaceJsonDefault(param, Left(large))
        if (mode == "shadowed") stage.set(param, Left(small))
      } else {
        stage.set(param, Left(large))
      }
      val candidate = if (mode == "copied") stage.copy(ParamMap.empty) else stage
      val writer = if (mode == "deferred") deferred else candidate.write.session(spark).overwrite()
      val error = intercept[IllegalArgumentException](writer.save(saved))
      assert(error.getMessage.contains("Spark JSON parsing limits") && error.getCause == null)
      val restored = AIPrompt.read.session(spark).load(saved)
      assert(restored.getExtraBody == small)
    }
  }

  test("large ordinary string params fail before replacing saved stages with unreadable metadata") {
    val large = "x" * (32 * 1024 * 1024)
    val saved = path()
    new AIPrompt().setSystemPrompt("keep").write.session(spark).save(saved)
    val stage = new AIPrompt().setSystemPrompt(large)
    intercept[com.fasterxml.jackson.core.exc.StreamConstraintsException] {
      stage.systemPrompt.jsonDecode(stage.systemPrompt.jsonEncode(large))
    }
    val error = intercept[IllegalArgumentException] {
      stage.write.session(spark).overwrite().save(saved)
    }
    assert(error.getMessage.contains("Spark JSON parsing limits") && error.getCause == null)
    assert(AIPrompt.read.session(spark).load(saved).getSystemPrompt == "keep")
  }

  Seq(false, true).foreach { fitted =>
    test(s"Spark pipeline writers reject oversized native JSON with fitted=$fitted") {
      val large = Map[String, Any]("description" -> ("x" * (32 * 1024 * 1024)))
      val stage = new AIPrompt().setExtraBody(large).setInputCol("text")
        .setUrl("http://localhost/v1").setModel("model")
      val pipeline = new Pipeline().setStages(Array(stage))
      val saved = path()
      val error = intercept[IllegalArgumentException] {
        if (fitted) pipeline.fit(input).write.session(spark).save(saved)
        else pipeline.write.session(spark).save(saved)
      }
      assert(error.getMessage.contains("Spark JSON parsing limits") && error.getCause == null)
    }

    test(s"Spark pipelines preserve supported native JSON and exact numbers with fitted=$fitted") {
      val body = Map[String, Any]("description" -> ("x" * (32 * 1024)), "number" -> BigDecimal("1e400"))
      val stage = new AIPrompt().setExtraBody(body).setInputCol("text")
        .setUrl("http://localhost/v1").setModel("model")
      val pipeline = new Pipeline().setStages(Array(stage))
      val saved = path()
      val restored = (if (fitted) {
        pipeline.fit(input).write.session(spark).save(saved)
        PipelineModel.read.session(spark).load(saved).stages.head
      } else {
        pipeline.write.session(spark).save(saved)
        Pipeline.read.session(spark).load(saved).getStages.head
      }).asInstanceOf[AIPrompt]
      assert(restored.getExtraBody == body && restored.uid == stage.uid)
      assert(restored.isSet(restored.extraBody) && !restored.isSet(restored.temperature))
    }
  }

  test("null ordinary and service params fail before direct deferred and copied stage overwrites") {
    val saved = path()
    val missing = Option.empty[String].orNull
    new AIPrompt().setExtraBody(Map("keep" -> 1)).write.session(spark).save(saved)
    Seq[() => AIPrompt](() => new AIPrompt(), () => new AISystemOne(), () => new OpenAIDecisions())
      .foreach { create =>
        val stage = create()
        def verify[T](param: Param[T], value: T, setter: () => Unit): Unit = {
          val deferred = stage.write.session(spark).overwrite()
          val copied = stage.copy(ParamMap(param -> value))
          setter()
          Seq(() => stage.write.session(spark).overwrite().save(saved), () => deferred.save(saved),
            () => copied.write.session(spark).overwrite().save(saved)).foreach { save =>
            if (param == stage.user) intercept[IllegalArgumentException](save())
            else intercept[NullPointerException](save())
            assert(AIPrompt.read.session(spark).load(saved).getExtraBody == Map("keep" -> 1))
          }
          stage.clear(param)
        }
        verify(stage.systemPrompt, missing, () => { stage.setSystemPrompt(missing); () })
        verify(stage.user, Left(missing), () => { stage.setUser(missing); () })
      }
  }

  test("shadowed null ordinary and service defaults fail before overwriting a saved stage") {
    val saved = path()
    val missing = Option.empty[String].orNull
    new AIPrompt().setExtraBody(Map("keep" -> 1)).write.session(spark).save(saved)
    Seq("systemPrompt", "user").foreach { name =>
      val stage = new TestableAIMutableJsonDefaultPrompt("invalid-scalar-default")
        .setSystemPrompt("valid").setUser("valid")
      val writer = stage.write.session(spark).overwrite()
      name match {
        case "systemPrompt" => stage.replaceJsonDefault(stage.systemPrompt, missing)
        case "user" => stage.replaceJsonDefault(stage.user, Left(missing))
      }
      if (name == "user") intercept[IllegalArgumentException](writer.save(saved))
      else intercept[NullPointerException](writer.save(saved))
      assert(AIPrompt.read.session(spark).load(saved).getExtraBody == Map("keep" -> 1))
    }
  }

  test("prompt template failures keep literal text out of logs and throwable chains") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val marker = "SYNTHETIC_PRIVATE_PROMPT_MARKER"
      withLogs { logged =>
        Seq(s"$marker {", s"$marker {text", s"$marker {missing_column}").foreach { template =>
          val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setPromptTemplate(template)
          val failure = intercept[IllegalArgumentException](stage.transform(input))
          val trace = new StringWriter()
          failure.printStackTrace(new PrintWriter(trace))
          assert(!trace.toString.contains(marker), "Template text escaped through a throwable")
          assert(failure.getCause == null && failure.getSuppressed.isEmpty)
        }
        assert(!logged.isEmpty, "The regression must observe the transform failure logger")
        assert(logged.asScala.forall(!_.contains(marker)), "Template text escaped through a log event")
        assert(server.requests.isEmpty)
      }
    }
  }
}
