// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.param.GlobalParams
import com.microsoft.azure.synapse.ml.services.OpenAIApiVersion
import com.microsoft.azure.synapse.ml.services.openai.OpenAIDecisions
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import spray.json._

import java.io.File
import java.nio.file.Files
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit, TimeoutException}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.util.Using

class AIPromptOutputPreflightSuite extends AIPromptOutputTestBase {
  test("rendered template types are validated before durable initialization") {
    import spark.implicits._
    withServer { server =>
      Seq("{array(text)}", "{encode(text, 'UTF-8')}").foreach { template =>
        val root = path()
        val stage = prompt(root, server).setPromptTemplate(template)
        val data = Seq("hello").toDF("text")
        val schemaError = intercept[IllegalArgumentException](stage.transformSchema(data.schema))
        val transformError = intercept[IllegalArgumentException](stage.transform(data))
        Seq(schemaError, transformError).foreach { error =>
          assert(error.getMessage.contains("Prompt templates must produce strings") && error.getCause == null)
        }
        assert(!new File(root).exists() && server.requests.get() == 0)
      }
    }
  }

  test("durable malformed response schema names are terminal row errors and are not resent on resume") {
    delta()
    import spark.implicits._
    withServer { server =>
      val root = path()
      val data = Seq(
        ("invalid", """{"properties":{},"name":null}"""),
        ("valid", """{"type":"text"}""")).toDF("text", "format")
      val stage = prompt(root, server).setResponseFormatCol("format")
      val result = stage.transform(data)
      assert(result.schema == stage.transformSchema(data.schema))
      val rows = result.collect().map(row => row.getAs[String]("text") -> row).toMap
      assert(rows("invalid").getAs[Row]("error").getString(0).contains("invalid_request"))
      assert(rows("valid").getAs[Row]("error") == null && server.requests.get() == 1)
      val status = AIPrompt.getOutputStatus(spark, root)
      assert(status.state == "complete" && status.pendingCount == 0 && status.ownerToken.isEmpty)
      assert(stage.resume(spark).count() == 2 && server.requests.get() == 1)
    }
  }

  test("durable direct and facade Decisions commit blank inputs as terminal errors without resending") {
    delta()
    import spark.implicits._
    Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions()).foreach { stage =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
        val root = path()
        val data = Seq(" \t\n", " evidence ").toDF("text")
        stage.setUrl(server.url + "/v1").setModel("model").setInputCol("text")
          .setQuestions(NativeRequestFixtures.DecisionQuestions).setErrorCol("error")
          .setOutputPath(root).setOutputBatchSize(2)
        val result = stage.transform(data)
        assert(result.schema == stage.transformSchema(data.schema))
        val rows = result.collect().map(row => row.getAs[String]("text") -> row).toMap
        assert(rows(" \t\n").getAs[Row]("error").getString(0).contains("invalid_request"))
        assert(rows(" evidence ").getAs[Row]("error") == null && server.requests.size() == 1)
        val status = AIPrompt.getOutputStatus(spark, root)
        assert(status.state == "complete" && status.pendingCount == 0 && status.ownerToken.isEmpty)
        assert(stage.resume(spark).count() == 2 && server.requests.size() == 1)
      }
    }
  }

  test("global API versions do not prevent v1 durable recovery or replay committed requests") {
    delta()
    import spark.implicits._
    val previous = GlobalParams.getGlobalParam(OpenAIApiVersion)
    try {
      GlobalParams.resetGlobalParam(OpenAIApiVersion)
      withServer { server =>
        val root = path()
        val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), root, server.url)
        intercept[IllegalStateException] {
          AIPromptOutput.transformObserved(stage, Seq("one", "two", "three").toDF("text"), event =>
            if (event == "batch_committed") throw new IllegalStateException("stop after first commit"))
        }
        val before = AIPrompt.getOutputStatus(spark, root)
        assert(before.committedCount == 2 && before.pendingCount == 1 && server.requests.get() == 2)
        AIPrompt.releaseOutputOwner(spark, root, before.ownerToken.get, confirmStopped = true)
        GlobalParams.setGlobalParam(OpenAIApiVersion, Left("irrelevant-version"))
        val resumed = AIPromptStorageTestConfig.configure(new AIPrompt(), root, server.url).resume(spark)
        assert(resumed.count() == 3 && server.requests.get() == 3)
        GlobalParams.setGlobalParam(OpenAIApiVersion, Right("absentVersion"))
        assert(AIPromptStorageTestConfig.configure(new AIPrompt(), root, server.url).resume(spark).count() == 3)
        assert(server.requests.get() == 3 && AIPrompt.getOutputStatus(spark, root).ownerToken.isEmpty)
      }
    } finally {
      previous match {
        case Some(value) => GlobalParams.setGlobalParam(OpenAIApiVersion, value)
        case None => GlobalParams.resetGlobalParam(OpenAIApiVersion)
      }
    }
  }

  test("durable templates reject mutable functions and runtime values but snapshot materialized input expressions") {
    delta()
    val session = spark.newSession()
    import session.implicits._
    session.udf.register("aiprompt_render_ticket", (text: String) => "old:" + text)
    val data = Seq("hello", "hello", "hello").toDF("text")
    val reply =
      """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}]}"""
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      def contents: Vector[String] = server.requests.asScala.toVector.map { case (_, body) =>
        AIJson.objectString(body).fields("messages").asInstanceOf[JsArray].elements.last
          .asJsObject.fields("content").asInstanceOf[JsString].value
      }
      Seq("{aiprompt_render_ticket(text)}", "{concat('prefix:', aiprompt_render_ticket(text))}",
        "{current_timestamp()}", "{current_user()}", "{rand()}").foreach { template =>
        val root = path()
        val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), root,
          server.url + "/v1/chat/completions").setPromptTemplate(template)
        val error = intercept[IllegalArgumentException](stage.transform(data))
        assert(error.getMessage.contains("stable built-in expressions") && Option(error.getCause).isEmpty)
        assert(!new File(root).exists() && server.requests.isEmpty)
      }
      val root = path()
      val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), root,
        server.url + "/v1/chat/completions").setPromptTemplate("{aiprompt_render_ticket(text)}")
      val lazyStage = stage.copy(ParamMap.empty)
      lazyStage.clear(lazyStage.outputPath)
      assert(lazyStage.transform(data.limit(1)).count() == 1 && contents == Vector("old:hello"))
      server.requests.clear()
      stage.setPromptTemplate(
        "{element_at(transform(array(rendered), x -> x), 1)}:{cast(round(pi()) as int)}:{cast(round(e()) as int)}")
      val materialized = data.withColumn("rendered", expr("aiprompt_render_ticket(text)"))
      intercept[IllegalStateException] {
        AIPromptOutput.transformObserved(stage, materialized, event =>
          if (event == "batch_committed") throw new IllegalStateException("stop after committed batch"))
      }
      val status = AIPrompt.getOutputStatus(session, root)
      assert(status.pendingCount == 1 && contents == Vector.fill(2)("old:hello:3:3"))
      AIPrompt.releaseOutputOwner(session, root, status.ownerToken.get, confirmStopped = true)
      session.udf.register("aiprompt_render_ticket", (text: String) => "new:" + text)
      assert(stage.resume(session).count() == 3 && contents == Vector.fill(3)("old:hello:3:3"))
    }
  }

  Seq(false, true).foreach { recovering =>
    val operation = if (recovering) "resume" else "transform"
    test(s"durable $operation releases quiescent ownership after a between-batch preparation failure") {
      delta()
      val session = spark.newSession()
      import session.implicits._
      val initiallyCommitted = if (recovering) 2 else 0
      val total = initiallyCommitted + 3
      val rebound = new AtomicBoolean()
      val reply =
        """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}]}"""
      Using.resource(new AIRecordingServer((count, _) => {
        if (count > initiallyCommitted && rebound.compareAndSet(false, true)) {
          session.udf.register("lower", (text: String) => "changed:" + text)
        }
        200 -> reply
      })) { server =>
        val root = path()
        val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), root,
          server.url + "/v1/chat/completions").setPromptTemplate("{lower(text)}")
        val data = Seq.fill(total)("HELLO").toDF("text")
        if (recovering) {
          intercept[IllegalStateException] {
            AIPromptOutput.transformObserved(stage, data, event =>
              if (event == "batch_committed") throw new IllegalStateException("unknown commit acknowledgement"))
          }
          val interrupted = AIPrompt.getOutputStatus(session, root)
          assert(interrupted.committedCount == 2 && interrupted.ownerToken.isDefined)
          AIPrompt.releaseOutputOwner(session, root, interrupted.ownerToken.get, confirmStopped = true)
        }
        val error = intercept[IllegalArgumentException] {
          if (recovering) stage.resume(session) else stage.transform(data)
        }
        assert(error.getMessage.contains("stable built-in expressions"))
        val status = AIPrompt.getOutputStatus(session, root)
        assert(status.pendingCount == 1 && status.ownerToken.isEmpty && server.requests.size() == total - 1)
        intercept[IllegalArgumentException](stage.resume(session))
        assert(AIPrompt.getOutputStatus(session, root).ownerToken.isEmpty && server.requests.size() == total - 1)
        val resumed = stage.resume(session.newSession())
        assert(resumed.count() == total && resumed.schema == stage.transformSchema(data.schema))
        assert(server.requests.size() == total && AIPrompt.getOutputStatus(session, root).allSucceeded)
      }
    }
  }

  Seq("chat_completions", "responses").foreach { api =>
    test(s"$api attachments follow input column order through copied and loaded partial durable resumes") {
      delta()
      val session = spark
      import session.implicits._
      val left = "data:image/png;base64,AQID"
      val right = "data:image/png;base64,BAUG"
      val data = Seq.fill(3)((left, right)).toDF("left", "right").select("right", "left")
      val reply = if (api == "chat_completions") {
        """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}]}"""
      } else {
        """{"status":"completed","output":[{"type":"message","role":"assistant",
          |"content":[{"type":"output_text","text":"answer"}]}]}""".stripMargin
      }
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val root = path()
        val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), root, server.url + "/v1")
          .setApiType(api).setOutputBatchSize(1)
          .setPromptTemplate("Compare {left} and {`RiGhT`}; literal {{LEFT}}")
          .setColumnTypes(ListMap("LEFT" -> "path", "RIGHT" -> "path"))
        val lazyStage = stage.copy(ParamMap.empty)
        lazyStage.clear(lazyStage.outputPath)
        assert(lazyStage.transform(data.limit(1)).count() == 1)
        def stopAfterBatch(event: String): Unit =
          if (event == "batch_committed") throw new IllegalStateException("stop after committed batch")
        intercept[IllegalStateException](AIPromptOutput.transformObserved(stage, data, stopAfterBatch))
        val first = AIPrompt.getOutputStatus(session, root)
        assert(first.state == "running" && first.pendingCount == 2 && server.requests.size() == 2)
        AIPrompt.releaseOutputOwner(session, root, first.ownerToken.get, confirmStopped = true)
        val reordered = stage.copy(ParamMap.empty).setColumnTypes(ListMap("RIGHT" -> "path", "LEFT" -> "path"))
        intercept[IllegalStateException](AIPromptOutput.resumeObserved(reordered, session, stopAfterBatch))
        val second = AIPrompt.getOutputStatus(session, root)
        assert(second.state == "running" && second.pendingCount == 1 && server.requests.size() == 3)
        AIPrompt.releaseOutputOwner(session, root, second.ownerToken.get, confirmStopped = true)
        val saved = Files.createTempDirectory(tmpDir, "aiprompt-ordered-attachments").resolve("model").toString
        reordered.write.save(saved)
        val loaded = AIPrompt.load(saved)
        assert(loaded.resume(session).count() == 3 && server.requests.size() == 4)
        assert(loaded.resume(session).count() == 3 && server.requests.size() == 4)
        assert(AIPrompt.getOutputStatus(session, root).state == "complete")
        server.requests.asScala.foreach { case (_, body) =>
          assert(body.indexOf(right) >= 0 && body.indexOf(right) < body.indexOf(left))
          assert(body.contains("Compare attachment.png and attachment.png; literal {LEFT}"))
        }
      }
    }
  }

  test("durable native questions retain order through persistence and reject order-only changes before ownership") {
    delta()
    val session = spark
    import session.implicits._
    val names = Seq("e_one", "d_two", "c_three", "b_four", "a_five", "z_six")
    def questions(order: Seq[String], criteriaOrder: Seq[String]): JsObject =
      JsObject(ListMap(order.map(name => name -> JsObject("type" -> JsString("choice"),
        "instructions" -> JsString(name),
        "criteria" -> JsObject(ListMap(criteriaOrder.map(label => label -> JsString(label)): _*)))): _*))
    Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.SystemResponse)) { server =>
      val root = path()
      val stage = new AIPrompt().setApiType("systemone").setUrl(server.url).setModel("model")
        .setInputCol("text").setQuestions(questions(names, names)).setOutputPath(root).setOutputBatchSize(1)
      intercept[IllegalStateException] {
        AIPromptOutput.transformObserved(stage, Seq("hello", "hello").toDF("text"), event =>
          if (event == "batch_committed") throw new IllegalStateException("stop after committed batch"))
      }
      val status = AIPrompt.getOutputStatus(session, root)
      assert(status.pendingCount == 1 && server.requests.size() == 1)
      AIPrompt.releaseOutputOwner(session, root, status.ownerToken.get, confirmStopped = true)
      Seq(questions(names.reverse, names), questions(names, names.reverse)).foreach { changed =>
        intercept[IllegalArgumentException](stage.copy(ParamMap.empty).setQuestions(changed).resume(session))
        assert(server.requests.size() == 1 && AIPrompt.getOutputStatus(session, root).ownerToken.isEmpty)
      }
      val saved = Files.createTempDirectory(tmpDir, "aiprompt-ordered-questions").resolve("model").toString
      stage.copy(ParamMap.empty).write.save(saved)
      assert(AIPrompt.load(saved).resume(session).count() == 2 && server.requests.size() == 2)
      server.requests.asScala.foreach { case (_, body) =>
        val sent = AIJson.objectString(body).fields("questions").asJsObject
        assert(sent.fields.keys.toSeq == names)
        sent.fields.values.foreach(question =>
          assert(question.asJsObject.fields("criteria").asJsObject.fields.keys.toSeq == names))
      }
    }
  }

  test("durable templates reject subqueries before writes while snapshotted input expressions remain supported") {
    delta()
    val session = spark
    import session.implicits._
    val view = "aiprompt_template_context"
    Seq("old").toDF("prefix").createOrReplaceTempView(view)
    val data = Seq("hello", "hello", "hello").toDF("text")
    val reply =
      """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}]}"""
    try {
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        def contents: Vector[String] = server.requests.asScala.toVector.map { case (_, body) =>
          AIJson.objectString(body).fields("messages").asInstanceOf[JsArray].elements.last
            .asJsObject.fields("content").asInstanceOf[JsString].value
        }
        Seq(s"{(SELECT prefix FROM $view)} {text}",
          s"{concat((SELECT prefix FROM $view), ' ', text)}",
          s"{(SELECT max(prefix) FROM $view)} {text}").foreach { template =>
          val root = path()
          val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), root,
            server.url + "/v1/chat/completions").setPromptTemplate(template)
          val lazyStage = stage.copy(ParamMap.empty)
          lazyStage.clear(lazyStage.outputPath)
          server.requests.clear()
          assert(lazyStage.transform(data.limit(1)).count() == 1 && contents == Vector("old hello"))
          val error = intercept[IllegalArgumentException](stage.transform(data))
          assert(error.getMessage.contains("cannot contain SQL subqueries") && Option(error.getCause).isEmpty)
          assert(!new File(root).exists() && server.requests.size() == 1)
        }
        server.requests.clear()
        val root = path()
        val materialized = data.withColumn("prefix", expr(s"(SELECT prefix FROM $view)"))
        val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), root,
          server.url + "/v1/chat/completions").setPromptTemplate("{prefix} {text}")
        intercept[IllegalStateException] {
          AIPromptOutput.transformObserved(stage, materialized, event =>
            if (event == "batch_committed") throw new IllegalStateException("stop after committed batch"))
        }
        val status = AIPrompt.getOutputStatus(session, root)
        assert(status.pendingCount == 1 && contents == Vector("old hello", "old hello"))
        AIPrompt.releaseOutputOwner(session, root, status.ownerToken.get, confirmStopped = true)
        Seq("new").toDF("prefix").createOrReplaceTempView(view)
        assert(stage.resume(session).count() == 3 && contents == Vector("old hello", "old hello", "old hello"))
      }
    } finally {
      session.catalog.dropTempView(view)
    }
  }

  test("durable resume preserves ordered schemas in typed options and body extensions") {
    delta()
    val session = spark
    import session.implicits._
    Seq(false, true).foreach { bodyExtension =>
      withServer { server =>
        val root = path()
        val names = Seq("first", "second")
        def configure(stage: AIPrompt, order: Seq[String]): AIPrompt = {
          val schema = Map[String, Any]("type" -> "object",
            "properties" -> ListMap(order.map(_ -> Map("type" -> "string")): _*),
            "required" -> names, "additionalProperties" -> false)
          if (bodyExtension) stage.setExtraBody(Map("response_format" -> Map("type" -> "json_schema",
            "json_schema" -> Map("name" -> "ordered", "strict" -> true, "schema" -> schema))))
          else stage.setResponseSchema(schema)
        }
        val stage = configure(prompt(root, server).setOutputBatchSize(1), names)
        intercept[IllegalStateException] {
          AIPromptOutput.transformObserved(stage, Seq("hello", "hello").toDF("text"), event =>
            if (event == "batch_committed") throw new IllegalStateException("stop after committed batch"))
        }
        val status = AIPrompt.getOutputStatus(session, root)
        assert(status.pendingCount == 1 && server.requests.get() == 1)
        AIPrompt.releaseOutputOwner(session, root, status.ownerToken.get, confirmStopped = true)
        intercept[IllegalArgumentException](configure(stage.copy(ParamMap.empty), names.reverse).resume(session))
        assert(server.requests.get() == 1 && AIPrompt.getOutputStatus(session, root).ownerToken.isEmpty)
        val saved = Files.createTempDirectory(tmpDir, "aiprompt-resume-ordered").resolve("model").toString
        stage.copy(ParamMap.empty).write.save(saved)
        assert(AIPrompt.load(saved).resume(session).count() == 2 && server.requests.get() == 2)
      }
    }
  }

  test("resume rejects changed SQL expression semantics but permits execution tuning") {
    delta()
    val first = spark.newSession()
    first.conf.set("spark.sql.ansi.enabled", "false")
    first.conf.set("spark.sql.legacy.sizeOfNull", "true")
    val resumed = first.newSession()
    resumed.conf.set("spark.sql.ansi.enabled", "true")
    resumed.conf.set("spark.sql.legacy.sizeOfNull", "true")
    val data = first.range(3).withColumn("tags", expr("cast(null as array<string>)"))
    val reply =
      """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}]}"""
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      val root = path()
      val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), root, server.url + "/v1/chat/completions")
        .setPromptTemplate("{coalesce(size(tags), 0)}")
      def contents: Vector[String] = server.requests.asScala.toVector.map { case (_, body) =>
        AIJson.objectString(body).fields("messages").asInstanceOf[JsArray].elements.last
          .asJsObject.fields("content").asInstanceOf[JsString].value
      }
      intercept[IllegalStateException] {
        AIPromptOutput.transformObserved(stage, data, event =>
          if (event == "batch_committed") throw new IllegalStateException("stop after committed batch"))
      }
      val status = AIPrompt.getOutputStatus(first, root)
      assert(status.pendingCount == 1 && contents == Vector("-1", "-1"))
      AIPrompt.releaseOutputOwner(first, root, status.ownerToken.get, confirmStopped = true)
      val mismatch = intercept[IllegalArgumentException](stage.resume(resumed))
      Seq("original SynapseML build", "Spark runtime", "library and SQL defaults").foreach { guidance =>
        assert(mismatch.getMessage.contains(guidance))
      }
      resumed.conf.set("spark.sql.ansi.enabled", "false")
      resumed.conf.set("spark.sql.legacy.sizeOfNull", "false")
      intercept[IllegalArgumentException](stage.resume(resumed))
      assert(contents.size == 2 && AIPrompt.getOutputStatus(first, root).ownerToken.isEmpty)
      resumed.conf.set("spark.sql.legacy.sizeOfNull", "true")
      resumed.conf.set("spark.sql.shuffle.partitions", "1")
      assert(stage.resume(resumed).count() == 3 && contents == Vector("-1", "-1", "-1"))
      assert(stage.resume(resumed).count() == 3 && contents.size == 3)
    }
  }

  test("durable path normalization preserves missing-input errors and self-contained attachments") {
    delta()
    val session = spark
    import session.implicits._
    val data = Seq(None, Some(""), Some(" \t "), Some("data:text/plain;base64,aGVsbG8="),
      Some(" \tDATA:text/plain;BASE64,aGVsbG8= \t")).toDF("asset")
    val reply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"},
      |"finish_reason":"stop"}]}""".stripMargin
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      val root = path()
      val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), root, server.url + "/v1")
        .setPromptTemplate("{asset}").setColumnTypes(Map("asset" -> "path"))
      val lazyStage = stage.copy(ParamMap.empty)
      lazyStage.clear(lazyStage.outputPath)
      def verify(rows: Array[Row]): Unit = {
        assert(rows.length == 5)
        val errors = rows.flatMap(row => Option(row.getAs[Row]("error")))
        assert(errors.length == 3)
        errors.foreach { row =>
          val error = AIJson.objectString(row.getString(0))
          assert(error.fields("code") == JsString("missing_input"))
          assert(error.fields("message") == JsString("attachment input is missing"))
        }
      }
      def verifyPrompts(): Unit = server.requests.asScala.foreach { case (_, body) =>
        val message = AIJson.objectString(body).fields("messages").asInstanceOf[JsArray].elements.last.asJsObject
        val text = message.fields("content").asInstanceOf[JsString].value
        assert(text.takeWhile(_ != '\n') == "attachment.txt")
      }
      verify(lazyStage.transform(data).collect())
      assert(server.requests.size() == 2)
      verifyPrompts()
      val output = stage.transform(data)
      verify(output.collect())
      val status = AIPrompt.getOutputStatus(session, root)
      assert(status.state == "complete" && status.rowErrorCount == 3 && status.committedCount == 5)
      assert(stage.resume(session).count() == 5 && server.requests.size() == 4)
      verifyPrompts()
      val remote = stage.copy(ParamMap.empty).setOutputPath(path())
      intercept[org.apache.spark.SparkException] {
        remote.transform(Seq(Some(" \thttps://example.invalid/media \t")).toDF("asset"))
      }
      assert(server.requests.size() == 4)
    }
  }

  test("local failed control initialization does not poison a fresh public run") {
    delta()
    val session = spark
    import session.implicits._
    withServer { server =>
      val root = path()
      val qualified = AIPromptDelta.qualify(spark, root)
      intercept[IllegalStateException] {
        AIPromptLocalTransaction(qualified) { throw new IllegalStateException("before control write") }
      }
      assert(Files.exists(new File(root, AIPromptLocalTransaction.LockFile).toPath))
      assert(!new File(root, "_control").exists())
      assert(prompt(root, server).transform(Seq("hello").toDF("text")).count() == 1)
      assert(server.requests.get() == 1 && AIPrompt.getOutputStatus(spark, root).state == "complete")
    }
  }

  test("local control transactions serialize same-driver callers and release after failures") {
    val root = new File(path()).toPath
    val entered = new CountDownLatch(1)
    val secondStarted = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val pool = Executors.newFixedThreadPool(2)
    try {
      val first = pool.submit(new Callable[Int] {
        override def call(): Int = AIPromptLocalTransaction(root.toUri.toString) {
          entered.countDown()
          assert(release.await(10, TimeUnit.SECONDS))
          1
        }
      })
      assert(entered.await(10, TimeUnit.SECONDS))
      val second = pool.submit(new Callable[Int] {
        override def call(): Int = {
          secondStarted.countDown()
          AIPromptLocalTransaction(root.resolve(".").toUri.toString) { 2 }
        }
      })
      assert(secondStarted.await(10, TimeUnit.SECONDS))
      intercept[TimeoutException](second.get(1, TimeUnit.SECONDS))
      release.countDown()
      assert(first.get(10, TimeUnit.SECONDS) == 1 && second.get(10, TimeUnit.SECONDS) == 2)
      intercept[IllegalStateException] {
        AIPromptLocalTransaction(root.toUri.toString) { throw new IllegalStateException("transaction failed") }
      }
      assert(AIPromptLocalTransaction(root.toUri.toString) { 3 } == 3)
    } finally {
      release.countDown()
      pool.shutdownNow()
      assert(pool.awaitTermination(10, TimeUnit.SECONDS))
    }
  }

  test("durable wildcard templates preserve public inputs through partial recovery") {
    delta()
    val session = spark
    import session.implicits._
    val data = Seq(("duplicate", "public"), ("duplicate", "public"), ("tail", "public"))
      .toDF("text", "_aiprompt_row_public")
    val reply =
      """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}]}"""
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      val root = path()
      val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), root, server.url + "/v1/chat/completions")
        .setPromptTemplate("{to_json(struct(*))}")
      val lazyStage = stage.copy(org.apache.spark.ml.param.ParamMap.empty)
      lazyStage.clear(lazyStage.outputPath)
      val lazyOutput = lazyStage.transform(data)
      assert(lazyOutput.collect().length == 3)
      def contents: Seq[String] = server.requests.asScala.toSeq.map { case (_, body) =>
        AIJson.objectString(body).fields("messages").asInstanceOf[JsArray].elements.last
          .asJsObject.fields("content").asInstanceOf[JsString].value
      }.sorted
      val expected = contents
      assert(expected.count(_.contains("duplicate")) == 2)
      expected.foreach(text => assert(AIJson.objectString(text).fields.keySet == data.columns.toSet))
      server.requests.clear()
      intercept[IllegalStateException] {
        AIPromptOutput.transformObserved(stage, data, event =>
          if (event == "batch_committed") throw new IllegalStateException("stop after committed batch"))
      }
      val status = AIPrompt.getOutputStatus(spark, root)
      assert(status.pendingCount == 1 && server.requests.size() == 2)
      AIPrompt.releaseOutputOwner(spark, root, status.ownerToken.get, confirmStopped = true)
      val result = stage.resume(spark)
      assert(result.schema == AIPromptOutput.storageSchema(lazyOutput.schema))
      assert(result.count() == 3 && result.filter(col("text") === "duplicate").count() == 2)
      assert(contents == expected && server.requests.size() == 3)
      assert(stage.resume(spark).count() == 3 && server.requests.size() == 3)
    }
  }

  test("durable native questions reject external dependencies before inference") {
    delta()
    val session = spark
    import session.implicits._
    val data = Seq("hello").toDF("text")
    withServer { server =>
      Seq("systemone", "decisions").foreach { api =>
        def stage(root: String): AIPrompt = new AIPrompt().setApiType(api)
          .setUrl(if (api == "systemone") server.url.stripSuffix("/v1/chat/completions")
            else server.url.stripSuffix("/chat/completions"))
          .setModel("model").setInputCol("text").setOutputPath(root)
        Seq("file_id" -> JsString("file-example"), "tools" -> JsArray(JsObject("type" -> JsString("web_search"))))
          .foreach { case (binding, value) =>
            val question = JsObject("type" -> JsString(if (api == "systemone") "noul" else "predicate"),
              "instructions" -> JsString("Is this urgent?"), binding -> value)
            val questions = if (api == "systemone") JsObject("urgent" -> question) else JsArray(question)
            val scalarPath = path()
            intercept[IllegalArgumentException](stage(scalarPath).setQuestions(questions.compactPrint).transform(data))
            assert(!new File(scalarPath).exists())
            intercept[org.apache.spark.SparkException] {
              stage(path()).setQuestionsCol("questions")
                .transform(data.withColumn("questions", lit(questions.compactPrint)))
            }
          }
        val safe = if (api == "systemone") {
          """{"file_id":{"type":"choice","instructions":"Classify this text",
            |"criteria":{"tools":"Tool-related text","conversation":"Conversation-related text",
            |"image_url":"https://example.invalid/inert-description"}}}""".stripMargin
        } else {
          """[{"type":"predicate","name":"file_id","instructions":"Does the text mention tools?"}]"""
        }
        val scalar = stage(path()).setQuestions(safe)
        scalar.validateDurableInput(data.schema, session)
        val perRow = stage(path()).setQuestionsCol("questions")
        perRow.validateDurableRows(data.withColumn("questions", lit(safe)))
      }
      assert(server.requests.get() == 0)
    }
  }

  test("durable JSON postprocessing commits only actual parsing failures as row errors") {
    delta()
    val session = spark
    import session.implicits._
    Seq("chat_completions", "responses").foreach { api =>
      Using.resource(new AIRecordingServer((_, body) => {
        val text = if (body.contains("malformed")) "not json"
          else if (body.contains("incompatible")) """{"value":"wrong"}"""
          else if (body.contains("fragment")) """{"value":1},"extra":2"""
          else if (body.contains("trailing")) """{"value":1} {"value":2}"""
          else if (body.contains("singlequotes")) "{'value':1.0}"
          else if (body.contains("nonnumeric")) """{"value":NaN}"""
          else if (body.contains("duplicatekeys")) """{"value":1.0,"value":2.0}"""
          else """{"value":null}"""
        val quoted = JsString(text).compactPrint
        val reply = if (api == "chat_completions") {
          s"""{"choices":[{"index":0,"message":{"role":"assistant","content":$quoted},"finish_reason":"stop"}]}"""
        } else {
          s"""{"status":"completed","output":[{"type":"message","role":"assistant",
             |"content":[{"type":"output_text","text":$quoted}]}]}""".stripMargin
        }
        200 -> reply
      })) { server =>
        val root = path()
        val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), root, server.url + "/v1")
          .setApiType(api).setPostProcessingOptions(Map("jsonSchema" -> "value DOUBLE"))
        val invalid = Set("malformed", "incompatible", "fragment", "trailing")
        val rows = stage.transform((invalid.toSeq ++ Seq("singlequotes", "nonnumeric", "duplicatekeys", "valid"))
          .toDF("text")).collect()
        assert(rows.length == 8 && rows.count(_.getAs[Row]("error") != null) == 4)
        assert(rows.filter(row => invalid(row.getAs[String]("text")))
          .forall(_.getAs[Row]("error").getString(0).contains("parse_error")))
        assert(rows.find(_.getAs[String]("text") == "valid").get.getAs[Row]("answer").isNullAt(0))
        assert(rows.find(_.getAs[String]("text") == "singlequotes").get.getAs[Row]("answer").getDouble(0) == 1.0)
        assert(rows.find(_.getAs[String]("text") == "nonnumeric").get.getAs[Row]("answer").getDouble(0).isNaN)
        assert(rows.find(_.getAs[String]("text") == "duplicatekeys").get.getAs[Row]("answer").getDouble(0) == 2.0)
        assert(rows.forall(_.getAs[String]("raw").contains("assistant")))
        val status = AIPrompt.getOutputStatus(spark, root)
        assert(status.rowErrorCount == 4 && !status.allSucceeded && status.pendingCount == 0)
        assert(stage.resume(spark).count() == 8 && server.requests.size() == 8)
      }
    }
  }

  test("sub-nanosecond concurrent timeouts fail before durable storage or HTTP") {
    delta()
    import spark.implicits._
    withServer { server =>
      Seq(Double.MinPositiveValue, 0.5e-9, 0.999e-9).foreach { timeout =>
        val root = path()
        val error = intercept[IllegalArgumentException] {
          prompt(root, server).setConcurrentTimeout(timeout).transform(Seq("hello").toDF("text"))
        }
        assert(error.getMessage.contains("concurrentTimeout"))
        assert(!new File(root).exists() && server.requests.get() == 0)
      }
    }
  }

  test("invalid scalar questions and sub-millisecond timeouts fail before durable storage") {
    delta()
    import spark.implicits._
    Using.resource(new AIRecordingServer((_, _) => 200 -> "{}")) { server =>
      val stages = Seq(
        new AIPrompt().setApiType("systemone").setQuestions("null"),
        new AIPrompt().setApiType("decisions").setQuestions("null"),
        new AIPrompt().setTimeout(0.0005))
      stages.foreach { stage =>
        val root = path()
        stage.setUrl(if (stage.getApiType == "systemone") server.url else server.url + "/v1")
          .setModel("model").setInputCol("text").setOutputPath(root)
        intercept[IllegalArgumentException](stage.transform(Seq("hello").toDF("text")))
        assert(!new File(root).exists())
      }
      assert(server.requests.isEmpty)
    }
  }

  test("durable decisions templates reject provider bindings on the committed snapshot before HTTP") {
    delta()
    import spark.implicits._
    Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
      def stage(root: String): AIPrompt = new AIPrompt().setApiType("decisions")
        .setUrl(server.url + "/v1").setModel("native-model").setPromptTemplate("{payload}")
        .setInputMode("json").setQuestions(NativeRequestFixtures.DecisionQuestions)
        .setOutputPath(root).setOutputCol("answer").setErrorCol("error")
      val external =
        """[{"role":"user","content":[{"type":"input_text","text":"hello","file_id":"provider-file"}]}]"""
      intercept[org.apache.spark.SparkException] {
        stage(path()).transform(Seq(external).toDF("payload"))
      }
      assert(server.requests.isEmpty)
      val root = path()
      val valid = stage(root)
      val rows = valid.transform(Seq("\"ordinary https://example.invalid text\"", "not json")
        .toDF("payload")).collect()
      assert(rows.count(_.getAs[Row]("error") == null) == 1)
      assert(rows.count(row => Option(row.getAs[Row]("error"))
        .exists(_.getString(0).contains("invalid_request"))) == 1)
      assert(AIPrompt.getOutputStatus(spark, root).rowErrorCount == 1)
      assert(valid.resume(spark).count() == 2 && server.requests.size() == 1)
    }
  }

  test("durable long Retry-After terminates with the original HTTP error and never replays a committed outcome") {
    delta()
    val session = spark
    import session.implicits._
    val reply = """{"error":{"code":"rate_limit","message":"retry later"}}"""
    Using.resource(new AIRecordingServer((_, _) => 429 -> reply, retryAfter = "120")) { server =>
      val root = path()
      val stage = AIPromptStorageTestConfig.configure(new AIPrompt(), root, server.url + "/v1/chat/completions")
      val row = stage.transform(Seq("one").toDF("text")).head()
      assert(row.getAs[Row]("error").getString(0).contains("http_error"))
      assert(row.getAs[String]("raw") == reply)
      val status = AIPrompt.getOutputStatus(spark, root)
      assert(status.rowErrorCount == 1 && !status.allSucceeded && status.pendingCount == 0)
      assert(stage.resume(spark).count() == 1 && server.requests.size() == 1)
    }
  }

  test("durable map-backed chat content commits terminal errors without sending or replaying") {
    delta()
    withServer { server =>
      val root = path()
      val stage = prompt(root, server)
      stage.clear(stage.promptTemplate)
      stage.setMessagesCol("messages")
      val rows = stage.transform(AIChatContentFixtures.mixed(spark)).collect()
      val errors = rows.filter(_.getAs[Row]("error") != null)
      assert(rows.length == 6 && errors.length == 5)
      assert(errors.forall(_.getAs[Row]("error").getString(0).contains("invalid_request")))
      assert(rows.count(_.getAs[Row]("error") == null) == 1)
      assert(AIPrompt.getOutputStatus(spark, root).rowErrorCount == 5)
      assert(stage.resume(spark).count() == 6 && server.requests.get() == 1)
    }
  }

  test("durable body extensions reject known remote media but permit inert URL text and schemas") {
    delta()
    withServer { server =>
      val session = spark
      import session.implicits._
      val input = Seq("one").toDF("text")
      val remote = "https://example.invalid/mutable"
      Seq(Map("image_url" -> remote), Map("image_url" -> Map("url" -> remote)),
        Map("file_url" -> remote),
        Map("file_url" -> Map("url" -> remote)), Map("file_data" -> Map("url" -> remote))).foreach { media =>
        val root = path()
        val stage = prompt(root, server).setExtraBody(Map("extension" -> media))
        intercept[IllegalArgumentException](stage.transform(input))
        assert(!new File(root).exists())
      }
      Seq("file_url", "file_data").foreach { field =>
        val stage = prompt(path(), server).setExtraBodyCol("body")
        intercept[org.apache.spark.SparkException] {
          stage.transform(input.withColumn("body", lit(s"""{"extension":{"$field":{"url":"$remote"}}}""")))
        }
      }
      assert(server.requests.get() == 0)
      val inline = "data:image/png;base64,AQID"
      val schema = Map("type" -> "object", "properties" -> Map("file_url" -> Map("type" -> "string")))
      val safe = prompt(path(), server).setExtraBody(Map(
        "extension" -> Map("image_url" -> inline, "file_url" -> Map("url" -> inline),
          "file_data" -> Map("url" -> inline), "text" -> remote),
        "response_format" -> Map("type" -> "json_schema", "json_schema" ->
          Map("name" -> "result", "schema" -> schema))))
      assert(safe.transform(input).count() == 1 && server.requests.get() == 1)
    }
  }

  test("resume fingerprints the executing session rather than another active session") {
    delta()
    withServer { server =>
      val session = spark.newSession()
      import session.implicits._
      val root = path()
      val stage = prompt(root, server)
      val previous = SparkSession.getActiveSession
      val previousZone = spark.conf.get("spark.sql.session.timeZone")
      session.conf.set("spark.sql.session.timeZone", "UTC")
      spark.conf.set("spark.sql.session.timeZone", "UTC")
      SparkSession.setActiveSession(spark)
      try {
        val input = Seq(1, 2, 3).toDF("id").withColumn("text", lit("2026-01-01 12:00:00").cast("timestamp"))
        intercept[IllegalStateException] {
          AIPromptOutput.transformObserved(stage, input, event =>
            if (event == "batch_committed") throw new IllegalStateException("stop after committed batch"))
        }
        val status = AIPrompt.getOutputStatus(session, root)
        assert(status.pendingCount == 1 && server.requests.get() == 2)
        AIPrompt.releaseOutputOwner(session, root, status.ownerToken.get, confirmStopped = true)
        session.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
        intercept[IllegalArgumentException](stage.resume(session))
        assert(server.requests.get() == 2)
        session.conf.set("spark.sql.session.timeZone", "UTC")
        spark.conf.set("spark.sql.session.timeZone", "Asia/Tokyo")
        assert(stage.resume(session).count() == 3 && server.requests.get() == 3)
        assert(SparkSession.getActiveSession.contains(spark))
      } finally {
        spark.conf.set("spark.sql.session.timeZone", previousZone)
        previous match {
          case Some(active) => SparkSession.setActiveSession(active)
          case None => SparkSession.clearActiveSession()
        }
      }
    }
  }
}
