// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.io.http.{HTTPSchema, URLKey}
import com.microsoft.azure.synapse.ml.param.GlobalParams
import com.microsoft.azure.synapse.ml.services.OpenAIApiVersion
import com.microsoft.azure.synapse.ml.services.aifoundry.AIFoundryModel
import com.microsoft.azure.synapse.ml.services.openai.{OpenAIDecisions, OpenAIEmbeddingDeploymentNameKey}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spray.json._

import java.io.File
import java.util.Collections
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConverters._
import scala.util.Using

class AIPromptDurableBindingsSuite extends AIPromptOutputTestBase {
  private val chatReply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""

  Seq("chat_completions", "responses").foreach { api =>
    test(s"durable $api preserves cache extensions and rejects changed cache identity on resume") {
      delta()
      val data = spark.range(2).select(col("id").cast(StringType).as("text"))
      val reply = if (api == "chat_completions") chatReply else {
        """{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}"""
      }
      val extension = """{"prompt_cache_key":"cache-key","prompt_cache_options":{"mode":"auto"}}"""
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val root = path()
        val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error")
          .setExtraBody(extension).setOutputPath(root).setOutputBatchSize(1)
        intercept[IllegalStateException] {
          AIPromptOutput.transformObserved(stage, data, event =>
            if (event == "batch_committed") throw new IllegalStateException("stop after committed batch"))
        }
        val status = AIPrompt.getOutputStatus(spark, root)
        assert(status.committedCount == 1 && status.pendingCount == 1 && server.requests.size() == 1)
        AIPrompt.releaseOutputOwner(spark, root, status.ownerToken.get, confirmStopped = true)
        val changed = stage.copy(ParamMap.empty).setExtraBody(extension.replace("cache-key", "changed-key"))
        assert(intercept[IllegalArgumentException](changed.resume(spark)).getMessage.contains("configuration"))
        assert(server.requests.size() == 1 && AIPrompt.getOutputStatus(spark, root).ownerToken.isEmpty)
        val rows = stage.copy(ParamMap.empty).resume(spark).collect()
        assert(rows.length == 2 && rows.forall(_.getAs[String]("answer") == "answer"))
        assert(rows.forall(_.getAs[Row]("error") == null))
        assert(AIPrompt.loadOutput(spark, root).collect().toSet == rows.toSet && server.requests.size() == 2)
        server.requests.asScala.foreach { case (_, body) =>
          val fields = AIJson.objectString(body).fields
          assert(fields("prompt_cache_key") == JsString("cache-key"))
          assert(fields("prompt_cache_options") == JsObject("mode" -> JsString("auto")))
        }
        assert(AIPrompt.getOutputStatus(spark, root).allSucceeded)
      }
    }
  }

  test("durable custom URL roots ignore unrelated global endpoint changes during recovery") {
    delta()
    val previous = GlobalParams.getGlobalParam(URLKey)
    try {
      GlobalParams.setGlobalParam(URLKey, "https://first.example.invalid")
      withServer { server =>
        val session = spark
        import session.implicits._
        val root = path()
        val stage = new AIPrompt().setCustomUrlRoot(server.url).setModel("model").setInputCol("text")
          .setOutputCol("answer").setErrorCol("error").setOutputPath(root).setOutputBatchSize(2)
        intercept[IllegalStateException] {
          AIPromptOutput.transformObserved(stage, Seq("one", "two", "three").toDF("text"), event =>
            if (event == "batch_committed") throw new IllegalStateException("stop after committed batch"))
        }
        val status = AIPrompt.getOutputStatus(session, root)
        assert(status.committedCount == 2 && status.pendingCount == 1 && server.requests.get() == 2)
        AIPrompt.releaseOutputOwner(session, root, status.ownerToken.get, confirmStopped = true)
        GlobalParams.setGlobalParam(URLKey, "https://second.example.invalid")
        assert(stage.resume(session).count() == 3 && server.requests.get() == 3)
        GlobalParams.resetGlobalParam(URLKey)
        assert(stage.resume(session).count() == 3 && server.requests.get() == 3)
        assert(AIPrompt.getOutputStatus(session, root).ownerToken.isEmpty)
      }
    } finally {
      previous match {
        case Some(value) => GlobalParams.setGlobalParam(URLKey, value)
        case None => GlobalParams.resetGlobalParam(URLKey)
      }
    }
  }

  Seq("chat_completions", "responses", "systemone", "decisions", "embeddings").foreach { api =>
    test(s"durable $api template evaluation failures stop initialization before any HTTP requests") {
      delta()
      val session = spark.newSession()
      session.conf.set("spark.sql.legacy.timeParserPolicy", "EXCEPTION")
      import session.implicits._
      Using.resource(new AIRecordingServer((_, _) => 200 -> "{}")) { server =>
        val root = path()
        val stage = new AIPrompt().setApiType(api)
          .setUrl(if (api == "systemone") server.url else server.url + "/v1").setModel("model")
          .setPromptTemplate("{cast(to_date(text, 'MM/dd/yyyy') as string)}")
          .setOutputPath(root).setOutputBatchSize(1)
        if (api == "systemone") stage.setQuestions(NativeRequestFixtures.SystemQuestions)
        else if (api == "decisions") stage.setQuestions(NativeRequestFixtures.DecisionQuestions)
        val input = Seq("01/02/2020", "1/2/2020").toDF("text")
        val error = intercept[Exception](stage.transform(input))
        assert(Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
          .exists(_.getClass.getSimpleName == "SparkUpgradeException"))
        assert(server.requests.isEmpty)
        val qualified = AIPromptDelta.qualify(session, root)
        assert(AIPromptDelta.control(session, qualified).state == "initializing")
        assert(!new File(root, "results").exists())
        val resumeError = intercept[IllegalArgumentException](stage.resume(session))
        assert(resumeError.getMessage.contains("initialization is incomplete") && server.requests.isEmpty)
      }
    }
  }

  test("durable fingerprints distinguish explicit and default bindings with identical telemetry") {
    val schema = StructType(Seq(StructField("text", StringType)))
    val output = StructType(Seq(StructField("answer", StringType)))
    val defaults = new AIPrompt("same")
    def defaultTemperature(): AIPrompt = new AIPrompt("same") {
      setDefault(temperature -> Left(0.5),
        telemHeaders -> defaults.getDefault(defaults.telemHeaders).get,
        handler -> defaults.getDefault(defaults.handler).get)
    }
    val implicitTemperature = defaultTemperature()
    val explicitTemperature = defaultTemperature().setTemperature(0.5)
    def values(stage: AIPrompt): Map[String, Any] =
      stage.extractParamMap().toSeq.map(pair => pair.param.name -> pair.value).toMap
    assert(implicitTemperature.getClass == explicitTemperature.getClass)
    assert(values(implicitTemperature) == values(explicitTemperature))
    assert(AIPromptOutput.fingerprint(implicitTemperature, schema, output, spark) ==
      AIPromptOutput.fingerprint(defaultTemperature(), schema, output, spark))
    assert(AIPromptOutput.fingerprint(implicitTemperature, schema, output, spark) !=
      AIPromptOutput.fingerprint(explicitTemperature, schema, output, spark))
  }

  Seq(true).foreach { durable =>
    test(s"attachment wildcard templates use only public columns with durable=$durable") {
      if (durable) delta()
      val session = spark
      val inline = "data:image/png;base64,AQID"
      val data = session.range(3).select(col("id").cast(StringType).as("ticket"),
        lit(inline).as("asset"), lit("user-owned").as("asset_filename"))
      val expected = data.toJSON.collect().toVector.sorted
      Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
        val root = path()
        val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model")
          .setPromptTemplate("{asset};{to_json(struct(*))}").setColumnTypes(Map("asset" -> "path"))
          .setOutputCol("answer").setErrorCol("error")
        if (durable) {
          stage.setOutputPath(root).setOutputBatchSize(1)
          intercept[IllegalStateException] {
            AIPromptOutput.transformObserved(stage, data, event =>
              if (event == "batch_committed") throw new IllegalStateException("stop after committed batch"))
          }
          val status = AIPrompt.getOutputStatus(session, root)
          assert(status.pendingCount == 2 && server.requests.size() == 1)
          AIPrompt.releaseOutputOwner(session, root, status.ownerToken.get, confirmStopped = true)
        }
        val result = if (durable) stage.resume(session) else stage.transform(data)
        val rows = result.collect()
        assert(result.schema == stage.transformSchema(data.schema))
        assert(rows.length == 3 && rows.forall(_.getAs[Row]("error") == null))
        val contents = server.requests.asScala.toVector.map { case (_, body) =>
          val parts = AIJson.objectString(body).fields("messages").asInstanceOf[JsArray].elements.last
            .asJsObject.fields("content").asInstanceOf[JsArray].elements
          val texts = parts.flatMap(_.asJsObject.fields.get("text")).collect {
            case JsString(text) if text.startsWith("attachment.png;") => text.stripPrefix("attachment.png;")
          }
          assert(texts.size == 1)
          texts.head
        }.sorted
        assert(contents == expected && server.requests.size() == 3)
        if (durable) assert(stage.resume(session).count() == 3 && server.requests.size() == 3)
      }
    }
  }

  Seq(false, true).foreach { perRow =>
    test(s"durable top-level metadata preserves inert binding and media names with perRow=$perRow") {
      delta()
      val metadata = Map("conversation" -> "ticket-42", "prompt" -> "v3", "tools" -> "label",
        "image_url" -> "https://example.invalid/inert", "file_data" -> "ordinary text")
      val extension = Map("metadata" -> metadata)
      val data = spark.range(2).withColumn("text", lit("hello"))
        .withColumn("body", lit(AIJson.fromAny(extension).compactPrint))
      Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
        val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), path(), server.url + "/v1")
        if (perRow) stage.setExtraBodyCol("body") else stage.setExtraBody(extension)
        val result = stage.transform(data)
        assert(result.collect().forall(_.getAs[Row]("error") == null))
        assert(server.requests.size() == 2)
        server.requests.asScala.foreach { case (_, body) =>
          assert(AIJson.objectString(body).fields("metadata") == AIJson.fromAny(metadata))
        }
        assert(stage.resume(spark).count() == 2 && server.requests.size() == 2)
        assert(AIPrompt.getOutputStatus(spark, stage.getOutputPath).allSucceeded)
      }
    }
  }

  test("durable shuffle parallelism supports automatic sizing and caps explicit partition counts") {
    Seq("auto", " auto ").foreach(value => assert(AIPromptOutput.batchPartitions(3, value) == 3))
    Seq(199, 200, 100000).foreach { rows =>
      assert(AIPromptOutput.batchPartitions(rows, "auto") == math.min(rows, 200))
    }
    Seq("2", " 2", "2 ", "\t2\n").foreach(value => assert(AIPromptOutput.batchPartitions(3, value) == 2))
    assert(AIPromptOutput.batchPartitions(3, "20") == 3)
    Seq("0", "-1", "unknown").foreach { setting =>
      val error = intercept[IllegalArgumentException](AIPromptOutput.batchPartitions(3, setting))
      assert(error.getMessage.contains("spark.sql.shuffle.partitions"))
    }
  }

  test("durable batches use configured shuffle parallelism rather than snapshot file packing") {
    delta()
    val session = spark.newSession()
    session.conf.set("spark.sql.shuffle.partitions", " 2 ")
    assert(session.conf.get("spark.sql.shuffle.partitions") == " 2 ")
    session.conf.set("spark.sql.adaptive.enabled", "true")
    val arrived = new CountDownLatch(2)
    val overlapped = new AtomicBoolean(true)
    Using.resource(new AIRecordingServer((_, _) => {
      arrived.countDown()
      if (!arrived.await(10, TimeUnit.SECONDS)) overlapped.set(false)
      200 -> chatReply
    })) { server =>
      val data = session.createDataFrame(Seq(Row("first"), Row("last")).asJava,
        new StructType().add("text", StringType)).coalesce(1)
      val root = path()
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
        .setOutputCol("answer").setErrorCol("error").setConcurrency(1)
        .setOutputPath(root).setOutputBatchSize(2)
      val rows = stage.transform(data).collect()
      assert(rows.length == 2 && rows.forall(_.getAs[String]("answer") == "answer"))
      assert(overlapped.get(), "Two durable rows should reach the provider concurrently at concurrency=1")
      assert(server.requests.size() == 2)
      session.conf.set("spark.sql.shuffle.partitions", " 1 ")
      assert(stage.resume(session).count() == 2 && server.requests.size() == 2)
      assert(AIPrompt.getOutputStatus(session, root).allSucceeded)
    }
  }

  test("active durable batches reject changed SQL semantics before sending and release quiescent ownership") {
    delta()
    val session = spark.newSession()
    session.conf.set("spark.sql.ansi.enabled", "false")
    session.conf.set("spark.sql.legacy.sizeOfNull", "true")
    val data = session.range(3).withColumn("tags", expr("cast(null as array<string>)"))
    val reply =
      """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}]}"""
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      val root = path()
      val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), root, server.url + "/v1/chat/completions")
        .setPromptTemplate("{coalesce(size(tags), 0)}").setOutputBatchSize(1)
      def contents: Vector[String] = server.requests.asScala.toVector.map { case (_, body) =>
        AIJson.objectString(body).fields("messages").asInstanceOf[JsArray].elements.last
          .asJsObject.fields("content").asInstanceOf[JsString].value
      }
      val mismatch = intercept[IllegalArgumentException] {
        AIPromptOutput.transformObserved(stage, data, event =>
          if (event == "batch_committed") session.conf.set("spark.sql.legacy.sizeOfNull", "false"))
      }
      assert(mismatch.getMessage.contains("Spark SQL semantics"))
      val status = AIPrompt.getOutputStatus(session, root)
      assert(status.committedCount == 1 && status.pendingCount == 2 && status.ownerToken.isEmpty)
      assert(contents == Vector("-1"))
      intercept[IllegalArgumentException](stage.resume(session))
      assert(contents == Vector("-1"))
      session.conf.set("spark.sql.legacy.sizeOfNull", "true")
      session.conf.set("spark.sql.shuffle.partitions", "1")
      assert(stage.resume(session).count() == 3 && contents == Vector("-1", "-1", "-1"))
      assert(AIPrompt.getOutputStatus(session, root).isComplete)
      assert(stage.resume(session).count() == 3 && contents.size == 3)
    }
  }

  test("durable JSON parsing identity rejects corrupt-record setting changes between batches and on resume") {
    delta()
    val session = spark.newSession()
    val setting = "spark.sql.columnNameOfCorruptRecord"
    session.conf.set(setting, "_corrupt_record")
    val input = """{"a":"bad"}"""
    val data = session.range(3).withColumn("text", lit(input))
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val root = path()
      val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), root, server.url + "/v1")
        .setPromptTemplate("{to_json(from_json(text, 'a INT, _corrupt_record STRING'))}").setOutputBatchSize(1)
      def contents: Vector[String] = server.requests.asScala.toVector.map { case (_, body) =>
        AIJson.objectString(body).fields("messages").asInstanceOf[JsArray].elements.last
          .asJsObject.fields("content").asInstanceOf[JsString].value
      }
      intercept[IllegalArgumentException] {
        AIPromptOutput.transformObserved(stage, data, event =>
          if (event == "batch_committed") session.conf.set(setting, "other_corrupt_record"))
      }
      val status = AIPrompt.getOutputStatus(session, root)
      assert(status.committedCount == 1 && status.pendingCount == 2 && status.ownerToken.isEmpty)
      assert(contents.size == 1 && AIJson.objectString(contents.head).fields("_corrupt_record") == JsString(input))
      val original = contents.head
      intercept[IllegalArgumentException](stage.resume(session))
      assert(contents.size == 1)
      session.conf.set(setting, "_corrupt_record")
      assert(stage.resume(session).count() == 3 && contents == Vector.fill(3)(original))
      assert(stage.resume(session).count() == 3 && contents.size == 3)
    }
  }

  test("durable JSON partial-result semantics reject changed defaults between batches and on resume") {
    delta()
    val session = spark.newSession()
    val setting = "spark.sql.json.enablePartialResults"
    session.conf.unset(setting)
    val data = session.range(3).withColumn("text", lit("""{"a":{"ok":1,"bad":"x"}}"""))
    val expression = "to_json(from_json(text, 'a STRUCT<ok: INT, bad: INT>'))"
    val original = data.selectExpr(expression).first().getString(0)
    session.conf.set(setting, "false")
    val changed = data.selectExpr(expression).first().getString(0)
    assert(AIJson.objectString(original) == AIJson.objectString("""{"a":{"ok":1}}"""))
    assert(AIJson.objectString(changed) == JsObject.empty)
    session.conf.unset(setting)
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val root = path()
      val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), root, server.url + "/v1")
        .setPromptTemplate("{" + expression + "}").setOutputBatchSize(1)
      def contents: Vector[String] = server.requests.asScala.toVector.map { case (_, body) =>
        AIJson.objectString(body).fields("messages").asInstanceOf[JsArray].elements.last
          .asJsObject.fields("content").asInstanceOf[JsString].value
      }
      val mismatch = intercept[IllegalArgumentException] {
        AIPromptOutput.transformObserved(stage, data, event =>
          if (event == "batch_committed") session.conf.set(setting, "false"))
      }
      assert(mismatch.getMessage.contains("Spark SQL semantics"))
      val status = AIPrompt.getOutputStatus(session, root)
      assert(status.committedCount == 1 && status.pendingCount == 2 && status.ownerToken.isEmpty)
      assert(contents == Vector(original))
      intercept[IllegalArgumentException](stage.resume(session))
      assert(contents == Vector(original))
      session.conf.unset(setting)
      assert(stage.resume(session).count() == 3 && contents == Vector.fill(3)(original))
      assert(AIPrompt.getOutputStatus(session, root).allSucceeded)
      assert(stage.resume(session).count() == 3 && contents.size == 3)
    }
  }

  test("Decisions JSON null message types normalize consistently through direct and facade durable stages") {
    delta()
    val session = spark
    import session.implicits._
    Seq(new OpenAIDecisions(), new AIPrompt().setApiType("decisions")).foreach { stage =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
        val variants = Seq("absent" -> None, "null" -> Some(JsNull), "message" -> Some(JsString("message")),
          "unknown" -> Some(JsString("other")), "number" -> Some(JsNumber(42)))
        val data = variants.map { case (name, tag) =>
          val fields = Map("role" -> JsString("user"), "content" -> JsString("Evidence")) ++ tag.map("type" -> _)
          name -> JsArray(JsObject(fields)).compactPrint
        }.toDF("tag", "text")
        stage.setUrl(server.url + "/v1").setModel("model").setInputCol("text").setInputMode("json")
          .setQuestions(NativeRequestFixtures.DecisionQuestions).setOutputCol("answer").setErrorCol("error")
        stage.setOutputPath(path())
        val rows = stage.transform(data).collect().map(row => row.getAs[String]("tag") -> row).toMap
        Seq("absent", "null", "message").foreach(name => assert(rows(name).getAs[Row]("error") == null))
        Seq("unknown", "number").foreach(name =>
          assert(rows(name).getAs[Row]("error").getString(0).contains("invalid_request")))
        assert(server.requests.size() == 3)
        server.requests.asScala.foreach { case (_, body) =>
          val message = AIJson.objectString(body).fields("input").asInstanceOf[JsArray].elements.head.asJsObject
          assert(message.fields.get("type").forall(_ == JsString("message")))
        }
        assert(stage.resume(session).count() == 5 && server.requests.size() == 3)
        assert(AIPrompt.getOutputStatus(session, stage.getOutputPath).rowErrorCount == 2)
      }
    }
  }

  test("durable non-row-local templates are rejected before storage but materialized windows are retained") {
    delta()
    val session = spark.newSession()
    val data = session.range(3).withColumn("text", col("id").cast(StringType))
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      Seq("{cast(row_number() over (order by text) as string)}: {text}",
        "{cast(max(text) as string)}", "{cast(explode(array(text)) as string)}").foreach { template =>
        val root = path()
        val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), root, server.url + "/v1")
          .setPromptTemplate(template)
        intercept[IllegalArgumentException](stage.transform(data))
        assert(!new File(root).exists() && server.requests.isEmpty)
      }
      val materialized = data.withColumn("rank", expr("row_number() over (order by text)"))
      val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), path(), server.url + "/v1")
        .setPromptTemplate("{rank}: {text}")
      assert(stage.transform(materialized).count() == 3 && server.requests.size() == 3)
      assert(stage.resume(session).count() == 3 && server.requests.size() == 3)
    }
  }

  Seq("systemone", "decisions").foreach { api =>
    test(s"durable $api recovery requires the original stage class") {
      delta()
      val questions = if (api == "systemone") NativeRequestFixtures.SystemQuestions
        else NativeRequestFixtures.DecisionQuestions
      val reply = if (api == "systemone") NativeRequestFixtures.SystemResponse
        else NativeRequestFixtures.DecisionResponse
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val root = path()
        def direct(): AIPrompt = if (api == "systemone") new AISystemOne() else new OpenAIDecisions()
        def configure(stage: AIPrompt): AIPrompt = stage.setApiType(api).setUrl(server.url).setModel("model")
          .setInputCol("text").setQuestions(questions).setOutputCol("answer").setErrorCol("error")
          .setOutputPath(root)
        val data = spark.createDataFrame(Collections.singletonList(Row("hello")),
          new StructType().add("text", StringType))
        assert(configure(direct()).transform(data).count() == 1)
        val mismatch = intercept[IllegalArgumentException](configure(new AIPrompt()).resume(spark))
        assert(mismatch.getMessage.contains("stage class"))
        assert(configure(direct()).resume(spark).count() == 1)
        assert(server.requests.size() == 1)
      }
    }
  }

  Seq("responses", "decisions").foreach { api =>
    test(s"$api message type tags are validated before HTTP in durable execution") {
      delta()
      val reply = if (api == "decisions") NativeRequestFixtures.DecisionResponse else
        """{"status":"completed","output":[{"type":"message","role":"assistant",
          |"content":[{"type":"output_text","text":"answer"}]}]}""".stripMargin
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val message = new StructType().add("role", StringType).add("content", StringType).add("type", StringType)
        val schema = new StructType().add("tag", StringType).add("messages", ArrayType(message))
        val data = spark.createDataFrame(Seq("message", "unsupported", "", Option.empty[String].orNull)
          .map(tag => Row(tag, Seq(Row("user", "hello", tag)))).asJava, schema)
        val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
          .setMessagesCol("messages").setOutputCol("answer").setErrorCol("error")
        if (api == "decisions") stage.setQuestions(NativeRequestFixtures.DecisionQuestions)
        stage.setOutputPath(path())
        val result = stage.transform(data)
        assert(result.schema == stage.transformSchema(data.schema))
        val rows = result.collect()
        rows.foreach { row =>
          if (row.isNullAt(0) || row.getString(0) == "message") {
            assert(row.getAs[Row]("error") == null && !row.isNullAt(row.fieldIndex("answer")))
          } else {
            assert(row.getAs[Row]("error").getString(0).contains("invalid_request"))
          }
        }
        assert(server.requests.size() == 2)
        assert(stage.resume(spark).count() == 4 && server.requests.size() == 2)
        assert(AIPrompt.getOutputStatus(spark, stage.getOutputPath).rowErrorCount == 2)
        val invalidType = spark.createDataFrame(Collections.singletonList(Row(Seq(Row("user", "hello", 1)))),
          new StructType().add("messages", ArrayType(
            new StructType().add("role", StringType).add("content", StringType).add("type", IntegerType))))
        stage.clear(stage.outputPath)
        assert(stage.transform(invalidType).head().getAs[Row]("error").getString(0).contains("invalid_request"))
        assert(server.requests.size() == 2)
      }
    }
  }

  test("implicit embedding deployment excludes unused model defaults from planning and durable identity") {
    val previousModel = GlobalParams.getGlobalParam(AIFoundryModel)
    val previousEmbedding = GlobalParams.getGlobalParam(OpenAIEmbeddingDeploymentNameKey)
    val reply = """{"data":[{"index":0,"embedding":[0.25,0.5]}],"model":"embedding-model"}"""
    val input = spark.createDataFrame(Collections.singletonList(Row("hello")), new StructType().add("text", StringType))
    try {
      GlobalParams.setGlobalParam(OpenAIEmbeddingDeploymentNameKey, Left("embedding-model"))
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val stage = new AIPrompt().setApiType("embeddings").setUrl(server.url + "/v1").setInputCol("text")
          .setOutputCol("answer").setErrorCol("error")
        GlobalParams.setGlobalParam(AIFoundryModel, Left("unused-model"))
        val expected = stage.transformSchema(input.schema)
        val fingerprint = AIPromptOutput.fingerprint(stage.effectiveCopy(), input.schema, expected, spark)
        Seq[Either[String, String]](Right("absent-model"), Left("changed-unused-model")).foreach { value =>
          GlobalParams.setGlobalParam(AIFoundryModel, value)
          assert(stage.transformSchema(input.schema) == expected)
          assert(AIPromptOutput.fingerprint(stage.effectiveCopy(), input.schema, expected, spark) == fingerprint)
          assert(stage.transform(input).head().getAs[Row]("error") == null)
          assert(AIJson.objectString(server.requests.asScala.last._2).fields("model") == JsString("embedding-model"))
        }
        assert(server.requests.size() == 2)
      }
    } finally {
      previousModel match {
        case Some(value) => GlobalParams.setGlobalParam(AIFoundryModel, value)
        case None => GlobalParams.resetGlobalParam(AIFoundryModel)
      }
      previousEmbedding match {
        case Some(value) => GlobalParams.setGlobalParam(OpenAIEmbeddingDeploymentNameKey, value)
        case None => GlobalParams.resetGlobalParam(OpenAIEmbeddingDeploymentNameKey)
      }
    }
  }

  test("durable null API versions commit row errors and are not replayed on recovery") {
    delta()
    val data = spark.createDataFrame(
      Seq(Row("first", "selected-version"), Row("missing", Option.empty[String].orNull),
        Row("last", "selected-version")).asJava,
      new StructType().add("text", StringType).add("version", StringType)).coalesce(1)
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val root = path()
      val stage = new AIPrompt().setUrl(server.url).setModel("model").setInputCol("text")
        .setApiVersionCol("version").setOutputCol("answer").setErrorCol("error")
        .setOutputPath(root).setOutputBatchSize(2)
      intercept[IllegalStateException] {
        AIPromptOutput.transformObserved(stage, data, event =>
          if (event == "batch_committed") throw new IllegalStateException("simulated lost acknowledgement"))
      }
      val partial = AIPrompt.getOutputStatus(spark, root)
      assert(partial.committedCount == 2 && partial.rowErrorCount == 1 && server.requests.size() == 1)
      AIPrompt.releaseOutputOwner(spark, root, partial.ownerToken.get, confirmStopped = true)
      val rows = stage.resume(spark).collect().map(row => row.getAs[String]("text") -> row).toMap
      assert(rows.size == 3 && rows("missing").getAs[String]("answer") == null)
      assert(AIJson.objectString(rows("missing").getAs[Row]("error").getString(0))
        .fields("code") == JsString("missing_input"))
      assert(server.requests.size() == 2)
      assert(server.requests.asScala.forall(_._1.endsWith("?api-version=selected-version")))
      val complete = AIPrompt.getOutputStatus(spark, root)
      assert(complete.isComplete && complete.rowErrorCount == 1 && complete.ownerToken.isEmpty)
    }
  }

  test("durable embedding recovery ignores a changed unused implicit model default") {
    delta()
    val previousModel = GlobalParams.getGlobalParam(AIFoundryModel)
    val previousEmbedding = GlobalParams.getGlobalParam(OpenAIEmbeddingDeploymentNameKey)
    val session = spark
    import session.implicits._
    try {
      GlobalParams.setGlobalParam(AIFoundryModel, Left("unused-first"))
      GlobalParams.setGlobalParam(OpenAIEmbeddingDeploymentNameKey, Left("embedding-model"))
      withServer { server =>
        val root = path()
        val stage = new AIPrompt().setApiType("embeddings")
          .setUrl(server.url.replace("chat/completions", "embeddings")).setInputCol("text")
          .setOutputCol("answer").setErrorCol("error").setOutputPath(root).setOutputBatchSize(1)
        intercept[IllegalStateException] {
          AIPromptOutput.transformObserved(stage, Seq("first", "last").toDF("text"), event =>
            if (event == "batch_committed") throw new IllegalStateException("simulated lost acknowledgement"))
        }
        val partial = AIPrompt.getOutputStatus(spark, root)
        assert(partial.committedCount == 1 && server.requests.get() == 1)
        AIPrompt.releaseOutputOwner(spark, root, partial.ownerToken.get, confirmStopped = true)
        GlobalParams.setGlobalParam(AIFoundryModel, Left("unused-changed"))
        assert(stage.resume(spark).count() == 2 && server.requests.get() == 2)
        assert(AIPrompt.getOutputStatus(spark, root).allSucceeded)
        GlobalParams.setGlobalParam(AIFoundryModel, Right("absent-model"))
        assert(stage.resume(spark).count() == 2 && server.requests.get() == 2)
      }
    } finally {
      previousModel match {
        case Some(value) => GlobalParams.setGlobalParam(AIFoundryModel, value)
        case None => GlobalParams.resetGlobalParam(AIFoundryModel)
      }
      previousEmbedding match {
        case Some(value) => GlobalParams.setGlobalParam(OpenAIEmbeddingDeploymentNameKey, value)
        case None => GlobalParams.resetGlobalParam(OpenAIEmbeddingDeploymentNameKey)
      }
    }
  }

  test("durable Responses accepts null optional resource fields in map and struct content") {
    delta()
    val session = spark.newSession()
    session.conf.set("spark.sql.jsonGenerator.ignoreNullFields", "false")
    val empty = Option.empty[String].orNull
    val inline = "data:text/plain;base64,aGVsbG8="
    val partSchema = new StructType().add("type", StringType).add("filename", StringType)
      .add("file_data", StringType).add("file_id", StringType)
    val parts: Seq[(DataType, Any)] = Seq(
      MapType(StringType, StringType) ->
        Map("type" -> "input_file", "filename" -> "note.txt", "file_data" -> inline, "file_id" -> empty),
      partSchema -> Row("input_file", "note.txt", inline, empty))
    val reply = """{"status":"completed","output":[{"type":"message","role":"assistant",
      |"content":[{"type":"output_text","text":"answer"}]}]}""".stripMargin
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      parts.foreach { case (partType, part) =>
        val messageType = new StructType().add("role", StringType).add("id", StringType)
          .add("content", ArrayType(partType))
        val data = session.createDataFrame(Collections.singletonList(Row(Seq(Row("user", empty, Seq(part))))),
          new StructType().add("messages", ArrayType(messageType)))
        val stage = new AIPrompt().setApiType("responses").setUrl(server.url + "/v1").setModel("model")
          .setMessagesCol("messages").setOutputCol("answer").setErrorCol("error")
        assert(stage.transform(data).head().getAs[String]("answer") == "answer")
        val root = path()
        val row = stage.setOutputPath(root).transform(data).head()
        assert(row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null)
        assert(AIPrompt.getOutputStatus(session, root).allSucceeded)
        assert(stage.resume(session).count() == 1)
      }
      assert(server.requests.size() == 4)
      server.requests.asScala.foreach { case (_, body) =>
        val message = AIJson.objectString(body).fields("input").asInstanceOf[JsArray].elements.head.asJsObject
        assert(!message.fields.contains("id"))
        val part = message.fields("content").asInstanceOf[JsArray].elements.head.asJsObject
        assert(!part.fields.contains("file_id") && part.fields("file_data") == JsString(inline))
      }
    }
  }

  test("durable resume ignores auth-named scalar telemetry but keeps ordinary telemetry") {
    delta()
    import spark.implicits._
    withServer { server =>
      val root = path()
      val first = prompt(root, server).setTelemHeaders(
        Map("x-trace" -> "same", "Authorization" -> "synthetic-first"))
      assert(first.transform(Seq("hello").toDF("text")).count() == 1)
      val second = first.copy(ParamMap.empty).setTelemHeaders(
        Map("x-trace" -> "same", "AUTHORIZATION" -> "synthetic-rotated", "Api-Key" -> "synthetic-key"))
      assert(second.resume(spark).count() == 1)
      second.setTelemHeaders(Map("x-trace" -> "changed"))
      intercept[IllegalArgumentException](second.resume(spark))
      assert(server.requests.get() == 1)
    }
  }

  test("durable telemetry column bindings fail before snapshot creation or HTTP") {
    delta()
    import spark.implicits._
    withServer { server =>
      Seq("Authorization", "api-key", "x-trace").foreach { header =>
        val data = Seq("hello").toDF("text")
          .withColumn("headers", map(lit(header), lit("synthetic-value")))
        val root = path()
        val stage = prompt(root, server)
        stage.setVectorParam(stage.telemHeaders, "headers")
        intercept[IllegalArgumentException](stage.transform(data))
        assert(!new File(root).exists())
      }
      assert(server.requests.get() == 0)
    }
  }

  Seq("chat_completions", "responses").foreach { api =>
    test(s"$api JSON NOT NULL fields have nullable durable output schemas") {
      delta()
      val text = """{"value":7,"nested":{},"items":[{},null],"lookup":{"key":{}}}"""
      val encoded = JsString(text).compactPrint
      val reply = if (api == "chat_completions") {
        s"""{"choices":[{"index":0,"message":{"role":"assistant","content":$encoded}}]}"""
      } else {
        s"""{"status":"completed","output":[{"type":"message","role":"assistant",
           |"content":[{"type":"output_text","text":$encoded}]}]}""".stripMargin
      }
      val nested = new StructType().add("n", IntegerType)
      val expected = new StructType().add("value", IntegerType).add("nested", nested)
        .add("items", ArrayType(nested)).add("lookup", MapType(StringType, nested))
      val data = spark.createDataFrame(Collections.singletonList(Row("hello")),
        new StructType().add("text", StringType))
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error")
          .setPostProcessingOptions(Map("jsonSchema" ->
            """value INT NOT NULL, nested STRUCT<n: INT NOT NULL> NOT NULL,
              |items ARRAY<STRUCT<n: INT NOT NULL>>, lookup MAP<STRING, STRUCT<n: INT NOT NULL>>""".stripMargin))
        stage.setOutputPath(path())
        assert(stage.transformSchema(data.schema)("answer").dataType == expected)
        val result = stage.transform(data)
        assert(result.schema == stage.transformSchema(data.schema))
        val row = result.head()
        val answer = row.getAs[Row]("answer")
        assert(row.getAs[Row]("error") == null && answer.getInt(0) == 7)
        assert(answer.getStruct(1).isNullAt(0))
        assert(answer.getSeq[Row](2).head.isNullAt(0) && answer.getSeq[Row](2).last == null)
        assert(answer.getMap[String, Row](3)("key").isNullAt(0))
        val resumed = stage.resume(spark)
        assert(resumed.schema == result.schema && resumed.head() == row)
        assert(AIPrompt.getOutputStatus(spark, stage.getOutputPath).allSucceeded)
        assert(server.requests.size() == 1)
      }
    }
  }

  test("durable large inline messages remain valid without hiding remote media") {
    delta()
    val inline = "data:image/png;base64," + ("A" * 20000004)
    val parts = Seq(Map("type" -> "input_image", "image_url" -> inline),
      Map("type" -> "input_image", "image_url" -> "https://example.invalid/image.png"))
    val message = new StructType().add("role", StringType)
      .add("content", ArrayType(MapType(StringType, StringType)))
    val data = spark.createDataFrame(Collections.singletonList(Row(Seq(Row("user", parts)))),
      new StructType().add("messages", ArrayType(message)))
    val reply = """{"status":"completed","output":[{"type":"message","role":"assistant",
      |"content":[{"type":"output_text","text":"answer"}]}]}""".stripMargin
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      val root = path()
      val stage = new AIPrompt().setApiType("responses").setUrl(server.url + "/v1").setModel("model")
        .setMessagesCol("messages").setOutputCol("answer").setErrorCol("error")
      val inlineData = spark.createDataFrame(Collections.singletonList(Row(Seq(Row("user", parts.take(1))))),
        data.schema)
      val lazyRow = stage.transform(inlineData).head()
      val saved = stage.setOutputPath(path()).transform(inlineData)
      assert(saved.schema == stage.transformSchema(inlineData.schema) && saved.head() == lazyRow)
      assert(lazyRow.getAs[String]("answer") == "answer" && lazyRow.getAs[Row]("error") == null)
      assert(stage.resume(spark).count() == 1 && server.requests.size() == 2)
      assert(server.requests.asScala.forall(_._2.contains(inline)))
      val failure = intercept[org.apache.spark.SparkException](stage.setOutputPath(root).transform(data))
      assert(failure.getMessage.contains("Durable media must be inline base64"))
      assert(server.requests.size() == 2 && !new File(root, "results").exists())
    }
  }

  test("durable nested empty structs fail before storage while a zero-column root remains supported") {
    delta()
    val empty = new StructType()
    val values = Seq[(DataType, Any)](empty -> Row.empty, ArrayType(empty) -> Seq(Row.empty),
      MapType(StringType, empty) -> Map("key" -> Row.empty),
      new StructType().add("nested", empty) -> Row(Row.empty))
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      values.foreach { case (dataType, value) =>
        val data = spark.createDataFrame(Collections.singletonList(Row(value)),
          new StructType().add("extra", dataType))
        val root = path()
        val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setPromptTemplate("hello")
          .setOutputCol("answer").setErrorCol("error").setOutputPath(root)
        val failure = intercept[IllegalArgumentException](stage.transform(data))
        assert(failure.getMessage.contains("empty structs"))
        assert(!new File(root).exists() && server.requests.isEmpty)
      }
      val data = spark.createDataFrame(Collections.singletonList(Row.empty), empty)
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setPromptTemplate("hello")
        .setOutputCol("answer").setErrorCol("error").setOutputPath(path())
      val result = stage.transform(data)
      assert(result.schema == stage.transformSchema(empty) && result.head().getAs[String]("answer") == "answer")
      assert(stage.resume(spark).count() == 1 && server.requests.size() == 1)
    }
  }
}
