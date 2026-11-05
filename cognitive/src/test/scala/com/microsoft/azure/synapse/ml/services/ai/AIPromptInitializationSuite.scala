// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.services.openai.OpenAIDecisions
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import spray.json._

import java.io.File
import scala.collection.JavaConverters._
import scala.util.{Try, Using}

class AIPromptInitializationSuite extends AIPromptOutputTestBase {
  private val validImage = "data:image/png;base64,AQID"
  private val malformedImages = Seq("", "data:image/png,AQID", "data:image/png")
  private val chatReply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
  private val responsesReply = """{"status":"completed","output":[{"type":"message","role":"assistant",
    |"content":[{"type":"output_text","text":"answer"}]}]}""".stripMargin

  test("missing run paths report no run without preventing a fresh transform") {
    delta()
    import spark.implicits._
    withServer { server =>
      val root = path()
      val stage = prompt(root, server)
      val calls = Seq[() => Any](
        () => AIPrompt.getOutputStatus(spark, root),
        () => AIPrompt.loadOutput(spark, root),
        () => stage.resume(spark),
        () => AIPrompt.releaseOutputOwner(spark, root, "stopped-owner", confirmStopped = true))
      calls.foreach { call =>
        val error = intercept[IllegalArgumentException](call())
        assert(error.getMessage.contains("No AIPrompt run exists"))
        assert(!error.getMessage.contains("incomplete") && !error.getMessage.contains(root))
        assert(!new File(root).exists() && server.requests.get() == 0)
      }
      assert(stage.transform(Seq("hello").toDF("text")).count() == 1)
      assert(AIPrompt.getOutputStatus(spark, root).isComplete && server.requests.get() == 1)
    }
  }

  test("unrelated output directories are not described as incomplete runs or modified") {
    delta()
    import spark.implicits._
    withServer { server =>
      val root = path()
      val input = Seq("unrelated data").toDF("text")
      input.coalesce(1).write.parquet(root)
      val files = new File(root).list().toSet
      val stage = prompt(root, server)
      val calls = Seq[() => Any](
        () => AIPrompt.getOutputStatus(spark, root),
        () => AIPrompt.loadOutput(spark, root),
        () => stage.resume(spark),
        () => AIPrompt.releaseOutputOwner(spark, root, "stopped-owner", confirmStopped = true),
        () => stage.transform(input))
      calls.foreach { call =>
        val error = intercept[IllegalArgumentException](call())
        assert(error.getMessage.contains("not an AIPrompt run directory"))
        assert(!error.getMessage.contains("incomplete") && !error.getMessage.contains("cleaning up"))
        assert(!error.getMessage.contains(root))
        assert(new File(root).list().toSet == files && server.requests.get() == 0)
      }
      assert(spark.read.parquet(root).as[String].collect().toSeq == Seq("unrelated data"))
    }
  }

  test("uncommitted control data reports incomplete initialization without changing storage") {
    delta()
    import spark.implicits._
    withServer { server =>
      val root = path()
      val control = new File(root, "_control")
      val input = Seq("hello").toDF("text")
      input.coalesce(1).write.parquet(control.getPath)
      val files = control.list().toSet
      val stage = prompt(root, server)
      val calls = Seq[() => Any](
        () => AIPrompt.getOutputStatus(spark, root),
        () => AIPrompt.loadOutput(spark, root),
        () => stage.resume(spark),
        () => AIPrompt.releaseOutputOwner(spark, root, "stopped-owner", confirmStopped = true),
        () => stage.transform(input))
      calls.foreach { call =>
        val result = Try(call())
        assert(result.isFailure)
        val error = result.failed.get
        assert(error.isInstanceOf[IllegalArgumentException], error.toString)
        assert(error.getMessage.contains("initialization is incomplete") && error.getMessage.contains("new outputPath"))
        assert(error.getMessage.contains("may still be running"))
        assert(error.getMessage.contains("Wait for initialization to finish"))
        assert(error.getMessage.contains("confirm the creating application has stopped"))
        assert(!error.getMessage.contains(root))
        assert(control.list().toSet == files && server.requests.get() == 0)
      }
    }
  }

  test("active initialization requires waiting or confirmed stopped execution before abandoning the run") {
    delta()
    import spark.implicits._
    withServer { server =>
      val root = path()
      val qualified = AIPromptDelta.qualify(spark, root)
      val stage = prompt(root, server)
      val input = Seq("hello").toDF("text")
      var observed = Set.empty[String]
      val output = AIPromptOutput.transformObserved(stage, input, event =>
        if (Set("input_committed", "input_validated").contains(event)) {
          val before = AIPromptDelta.control(spark, qualified)
          assert(before.state == "initializing" && before.owner.isDefined)
          val calls = Seq[() => Any](
            () => AIPrompt.getOutputStatus(spark, root),
            () => AIPrompt.loadOutput(spark, root),
            () => stage.resume(spark),
            () => stage.copy(ParamMap.empty).transform(input))
          calls.foreach { call =>
            val error = intercept[IllegalArgumentException](call())
            assert(error.getMessage.contains("may still be running"))
            assert(error.getMessage.contains("Wait for initialization to finish"))
            assert(error.getMessage.contains("confirm the creating application has stopped"))
            assert(!error.getMessage.contains(root))
          }
          assert(AIPromptDelta.control(spark, qualified) == before && server.requests.get() == 0)
          observed += event
        })
      assert(observed == Set("input_committed", "input_validated"))
      assert(output.count() == 1 && server.requests.get() == 1)
      assert(AIPrompt.getOutputStatus(spark, root).isComplete)
    }
  }

  test("SQL variable substitution output paths reject before durable storage or inference") {
    delta()
    Seq(true, false).foreach { enabled =>
      Seq(false, true).foreach { defined =>
        val session = spark.newSession()
        session.conf.set("spark.sql.variable.substitute", enabled.toString)
        if (defined) session.conf.set("aiprompt_test_segment", "substituted")
        import session.implicits._
        withServer { server =>
          val parent = path()
          val root = new File(parent, "${aiprompt_test_segment}").toString
          val stage = prompt(root, server)
          val input = Seq("hello").toDF("text")
          val calls = Seq[() => Any](
            () => stage.transform(input),
            () => stage.transformSchema(input.schema),
            () => stage.resume(session),
            () => AIPrompt.getOutputStatus(session, root),
            () => AIPrompt.loadOutput(session, root),
            () => AIPrompt.releaseOutputOwner(session, root, "stopped-owner", confirmStopped = true))
          calls.foreach { call =>
            val error = intercept[IllegalArgumentException](call())
            assert(error.getMessage.contains("outputPath must not contain SQL variable substitutions"))
            assert(!error.getMessage.contains(parent))
          }
          assert(!new File(parent).exists() && server.requests.get() == 0)
        }
      }
    }
  }

  private def mediaInput: DataFrame = {
    val session = spark
    import session.implicits._
    (malformedImages :+ validImage).toDF("asset").withColumn("text", lit("describe"))
  }

  private def messageInput(api: String): DataFrame = {
    val partType = if (api == "chat_completions") "image_url" else "input_image"
    mediaInput.withColumn("messages", array(struct(lit("user").as("role"),
      array(map(lit("type"), lit(partType), lit("image_url"), col("asset"))).as("content"))))
  }

  private def verifyMedia(api: String, input: DataFrame, expectedCodes: Seq[String], sends: Int)
                         (configure: AIPrompt => AIPrompt): Unit = {
    delta()
    val reply = api match {
      case "decisions" => NativeRequestFixtures.DecisionResponse
      case "responses" => responsesReply
      case _ => chatReply
    }
    Using.resource(new AIRecordingServer((_, body) => {
      if (body.contains(validImage)) 200 -> reply
      else 400 -> """{"error":{"code":"invalid_media","message":"synthetic malformed media"}}"""
    })) { server =>
      val root = path()
      val stage = configure(new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
        .setOutputCol("answer").setErrorCol("error").setOutputPath(root).setOutputBatchSize(4))
      def verify(rows: Array[Row]): Unit = {
        val byAsset = rows.map(row => row.getAs[String]("asset") -> row).toMap
        assert(rows.length == 4 && byAsset.size == 4)
        malformedImages.zip(expectedCodes).foreach { case (asset, code) =>
          val error = byAsset(asset).getAs[Row]("error")
          assert(error != null && AIJson.objectString(error.getString(0)).fields("code") == JsString(code))
        }
        assert(byAsset(validImage).getAs[Row]("error") == null && byAsset(validImage).getAs[Any]("answer") != null)
      }
      val lazyStage = stage.copy(ParamMap.empty)
      lazyStage.clear(lazyStage.outputPath)
      verify(lazyStage.transform(input).collect())
      assert(server.requests.size() == sends)
      val output = stage.transform(input)
      assert(output.schema == stage.transformSchema(input.schema))
      verify(output.collect())
      val status = AIPrompt.getOutputStatus(spark, root)
      assert(status.isComplete && status.rowErrorCount == 3 && status.committedCount == 4)
      verify(stage.resume(spark).collect())
      assert(server.requests.size() == 2 * sends && status.ownerToken.isEmpty)
    }
  }

  Seq("chat_completions", "responses").foreach { api =>
    test(s"durable malformed inline $api paths retain lazy terminal row errors") {
      verifyMedia(api, mediaInput, Seq("missing_input", "invalid_request", "invalid_request"), sends = 1) {
        _.setPromptTemplate("{asset}").setColumnTypes(Map("asset" -> "path"))
      }
    }

    test(s"durable malformed inline $api messages retain local and provider terminal errors") {
      verifyMedia(api, messageInput(api), Seq("invalid_request", "http_error", "http_error"), sends = 3) {
        _.setMessagesCol("messages")
      }
    }
  }

  test("durable malformed inline Decisions JSON remains terminal without sending invalid rows") {
    val input = messageInput("decisions").withColumn("input", to_json(col("messages"))).drop("messages")
    verifyMedia("decisions", input, Seq.fill(3)("invalid_request"), sends = 1) {
      _.setInputCol("input").setInputMode("json").setQuestions(NativeRequestFixtures.DecisionQuestions)
    }
  }

  Seq(true).foreach { durable =>
    test(s"Decisions image URL casing and MIME parameters preserve payloads with durable=$durable") {
      if (durable) delta()
      import spark.implicits._
      val accepted = Seq(validImage, "DATA:image/png;base64,AQID", "data:IMAGE/PNG;base64,AQID",
        "data:image/png;BASE64,AQID", "data:image/png;name=a.png;base64,AQID",
        "DATA:IMAGE/PNG;NAME=a%20b.png;charset=UTF-8;BASE64,AQID")
      val rejected = Seq("data:text/plain;base64,AQID", "data:image/png;name;base64,AQID",
        "data:image/png;=a.png;base64,AQID", "data:image/png;name=bad%xy;base64,AQID",
        "data:image/png;base64;name=a.png,AQID", "data:image/png;name=a.png;base64,@@@",
        "data:image/png;name=a.png;base64,", "data:image/png;name=a.png,AQID")
      val messages = (accepted ++ rejected).toDF("asset").withColumn("messages", array(struct(
        lit("user").as("role"), array(map(lit("type"), lit("input_image"),
          lit("image_url"), col("asset"))).as("content"))))
      Seq(false, true).foreach { json =>
        val input = if (json) messages.withColumn("input", to_json(col("messages"))).drop("messages") else messages
        Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions()).foreach { stage =>
          Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
            stage.setUrl(server.url + "/v1").setModel("model").setQuestions(NativeRequestFixtures.DecisionQuestions)
              .setOutputCol("answer").setErrorCol("error")
            if (json) stage.setInputCol("input").setInputMode("json") else stage.setMessagesCol("messages")
            if (durable) stage.setOutputPath(path()).setOutputBatchSize(accepted.size + rejected.size)
            def verify(rows: Array[Row]): Unit = {
              val byAsset = rows.map(row => row.getAs[String]("asset") -> row).toMap
              assert(rows.length == accepted.size + rejected.size && byAsset.size == rows.length)
              accepted.foreach { asset =>
                assert(byAsset(asset).getAs[Row]("error") == null, asset)
                assert(byAsset(asset).getAs[Row]("answer") != null)
              }
              rejected.foreach { asset =>
                assert(byAsset(asset).getAs[Row]("error").getString(0).contains("invalid_request"), asset)
                assert(byAsset(asset).getAs[Row]("answer") == null)
              }
            }
            val output = stage.transform(input)
            assert(output.schema == stage.transformSchema(input.schema))
            verify(output.collect())
            assert(server.requests.size() == accepted.size)
            if (durable) {
              verify(stage.resume(spark).collect())
              val status = AIPrompt.getOutputStatus(spark, stage.getOutputPath)
              assert(status.isComplete && status.rowErrorCount == rejected.size)
              assert(server.requests.size() == accepted.size)
            }
            val sent = Iterator.continually(server.requests.poll()).takeWhile(_ != null).map { case (_, body) =>
              body.parseJson.asJsObject.fields("input").asInstanceOf[JsArray].elements.head.asJsObject
                .fields("content").asInstanceOf[JsArray].elements.head.asJsObject.fields("image_url")
            }.toSet
            assert(sent == accepted.map(JsString(_)).toSet)
          }
        }
      }
    }
  }

  test("durable malformed inline body media retains provider terminal errors") {
    val input = mediaInput.withColumn("body",
      to_json(struct(struct(struct(col("asset").as("url")).as("image_url")).as("extension"))))
    verifyMedia("chat_completions", input, Seq.fill(3)("http_error"), sends = 4) {
      _.setInputCol("text").setExtraBodyCol("body")
    }
  }

  private def underOutputSchema(value: JsValue): Seq[JsObject] = Seq(
    JsObject("response_format" -> JsObject("json_schema" -> JsObject("schema" -> value))),
    JsObject("response_format" -> JsObject("schema" -> value)),
    JsObject("text" -> JsObject("format" -> JsObject("schema" -> value))))

  private def underArrayOutputSchema(value: JsValue): Seq[JsObject] = Seq(
    JsObject("response_format" -> JsArray(JsObject("json_schema" -> JsObject("schema" -> value)))),
    JsObject("response_format" -> JsObject("json_schema" -> JsArray(JsObject("schema" -> value)))),
    JsObject("text" -> JsArray(JsObject("format" -> JsObject("schema" -> value)))),
    JsObject("text" -> JsObject("format" -> JsArray(JsObject("schema" -> value)))))

  private def bodyStage(api: String, url: String): AIPrompt = {
    val stage = new AIPrompt().setApiType(api).setUrl(if (api == "systemone") url else url + "/v1")
      .setModel("model").setInputCol("text")
      .setOutputCol("answer").setErrorCol("error").setOutputPath(path())
    if (api == "systemone") stage.setQuestions(NativeRequestFixtures.SystemQuestions)
    else if (api == "decisions") stage.setQuestions(NativeRequestFixtures.DecisionQuestions)
    stage
  }

  Seq("inputCol", "promptTemplate", "questionsCol").foreach { binding =>
    test(s"durable native malformed shapes reject resources before sibling inference with $binding") {
      delta()
      import spark.implicits._
      val systemone = binding == "questionsCol"
      val reply = if (systemone) NativeRequestFixtures.SystemResponse else NativeRequestFixtures.DecisionResponse
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val stage = new AIPrompt().setApiType(if (systemone) "systemone" else "decisions")
          .setUrl(if (systemone) server.url else server.url + "/v1").setModel("model")
          .setOutputCol("answer").setErrorCol("error")
        if (systemone) stage.setInputCol("text").setQuestionsCol("payload")
        else {
          stage.setInputMode("json").setQuestions(NativeRequestFixtures.DecisionQuestions)
          binding match {
            case "inputCol" => stage.setInputCol("payload")
            case "promptTemplate" => stage.setPromptTemplate("{payload}")
          }
        }
        val resources = Seq("""{"file_id":"provider-file"}""",
          """{"image_url":"https://example.invalid/media"}""")
        resources.foreach { resource =>
          val invalids = if (systemone) Seq(s"""{"urgent":[$resource]}""",
            s"""{"urgent":{"type":"choice","instructions":"Classify","criteria":{"label":[$resource]}}}""",
            s"""{"urgent":{"type":"score","instructions":"Score","criteria":[$resource]}}""")
          else Seq(resource)
          val safe = if (systemone) NativeRequestFixtures.SystemQuestions else "\"hello\""
          invalids.foreach { invalid =>
            val root = path()
            val data = Seq(("hello", safe), ("hello", invalid)).toDF("text", "payload")
            val failure = intercept[org.apache.spark.SparkException](stage.setOutputPath(root).transform(data))
            assert(Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null)
              .exists(_.isInstanceOf[IllegalArgumentException]))
            assert(server.requests.isEmpty)
            assert(AIPromptDelta.control(spark, root).state == "initializing")
          }
        }
      }
    }
  }

  test("durable native binding diagnostics identify restricted names without state values") {
    import spark.implicits._
    val stage = new AISystemOne().setStateCol("state").setStateMode("json")
    Seq("conversation", "prompt").foreach { name =>
      val value = JsObject(name -> JsString("private-state-value")).compactPrint
      val error = intercept[org.apache.spark.SparkException](stage.validateDurableRows(Seq(value).toDF("state")))
      val cause = Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).toVector.last
      assert(cause.getMessage.contains(s"restricted fields: $name"))
      assert(!cause.getMessage.contains("private-state-value"))
    }
    stage.validateDurableRows(Seq("""{"conversation":null,"prompt":null,"text":"private-state-value"}""").toDF("state"))
  }

  test("durable output schema exemptions apply only to request bodies without a storage dependency") {
    import spark.implicits._
    val resources = Seq(JsObject("file_id" -> JsString("provider-file")),
      JsObject("image_url" -> JsString("https://example.invalid/media")))
    val stage = new AISystemOne().setStateCol("payload").setStateMode("json")
    resources.flatMap(underOutputSchema).foreach { value =>
      val error = intercept[org.apache.spark.SparkException] {
        stage.validateDurableRows(Seq(value.compactPrint).toDF("payload"))
      }
      assert(Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
        .exists(_.isInstanceOf[IllegalArgumentException]))
    }
    val schema = JsObject("type" -> JsString("object"), "properties" -> JsObject(
      "file_id" -> JsObject("type" -> JsString("string")),
      "image_url" -> JsObject("type" -> JsString("string"))))
    Seq("chat_completions" -> 0, "responses" -> 2).foreach { case (api, index) =>
      val value = underOutputSchema(schema)(index)
      val data = Seq(("hello", value.compactPrint)).toDF("payload", "body")
      val scalar = new AIPrompt().setApiType(api).setInputCol("payload").setExtraBody(value.compactPrint)
      AIDurableInput.validate(scalar, data.schema)
      new AIPrompt().setApiType(api).setInputCol("payload").setExtraBodyCol("body").validateDurableRows(data)
    }
  }

  test("durable output schema exemptions reject wrong APIs and array carriers before storage") {
    val schema = new StructType().add("text", StringType)
    val resources = Seq(JsObject("file_id" -> JsString("provider-file")),
      JsObject("image_url" -> JsString("https://example.invalid/media")))
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      Seq("chat_completions", "responses", "systemone", "decisions", "embeddings").foreach { api =>
        val stage = bodyStage(api, server.url)
        resources.foreach { resource =>
          val bodies = underOutputSchema(resource).zipWithIndex.collect {
            case (body, index) if !(api == "chat_completions" && index == 0 ||
              api == "responses" && index == 2) => body
          } ++ underArrayOutputSchema(resource)
          bodies.foreach { body =>
            withClue(s"$api: ${body.compactPrint}: ") {
              val error = intercept[IllegalArgumentException] {
                stage.setExtraBody(body.compactPrint).transformSchema(schema)
              }
              assert(error.getMessage.contains("Durable"))
              assert(!new File(stage.getOutputPath).exists() && server.requests.isEmpty)
            }
          }
        }
      }
    }
  }

  test("durable output schema exemptions validate body columns before sibling inference") {
    delta()
    import spark.implicits._
    Seq("chat_completions", "responses", "systemone", "decisions", "embeddings").foreach { api =>
      val reply = if (api == "responses") responsesReply else chatReply
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val crossApi = underOutputSchema(JsObject("file_id" -> JsString("provider-file")))(
          if (api == "responses") 0 else 2)
        val arrayBody = underArrayOutputSchema(
          JsObject("image_url" -> JsString("https://example.invalid/media")))(
          if (api == "responses") 3 else 1)
        Seq(crossApi, arrayBody).foreach { body =>
          val stage = bodyStage(api, server.url).setExtraBodyCol("body")
          val data = Seq(("plain", "{}"), ("invalid", body.compactPrint)).toDF("text", "body")
          val error = withClue(s"$api: ${body.compactPrint}: ") {
            intercept[org.apache.spark.SparkException](stage.transform(data))
          }
          assert(Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
            .exists(cause => cause.isInstanceOf[IllegalArgumentException] && cause.getMessage.contains("Durable")))
          assert(server.requests.isEmpty && AIPromptDelta.control(spark, stage.getOutputPath).state == "initializing")
        }
        if (api == "chat_completions" || api == "responses") {
          val schema = JsObject("type" -> JsString("object"), "properties" -> JsObject(
            "file_id" -> JsObject("type" -> JsString("string")),
            "image_url" -> JsObject("type" -> JsString("string"))))
          val body = underOutputSchema(schema)(if (api == "responses") 2 else 0)
          val stage = bodyStage(api, server.url).setExtraBodyCol("body")
          val data = Seq(("plain", "{}"), ("schema", body.compactPrint)).toDF("text", "body")
          val output = stage.transform(data)
          val rows = output.collect()
          assert(rows.length == 2 && rows.forall(_.getAs[Row]("error") == null))
          assert(stage.resume(spark).collect().toSet == rows.toSet && server.requests.size() == 2)
          val key = if (api == "responses") "text" else "response_format"
          assert(server.requests.asScala.exists { case (_, sent) =>
            AIJson.objectString(sent).fields.get(key).contains(body.fields(key))
          })
        }
      }
    }
  }

  private def arrayMedia(url: String): Seq[Map[String, Any]] = Seq(
    Map("image_url" -> Seq(url)), Map("file_url" -> Seq(Seq(url))),
    Map("image_url" -> Map("url" -> Seq(url), "detail" -> "auto")),
    Map("file_data" -> Map("url" -> Seq(url))),
    Map("file_data" -> Seq(Map("url" -> Seq(url)), url)),
    Map("file_data" -> Seq(url)), Map("file_data" -> Seq(Seq(url))))

  test("durable media arrays reject external locations without a storage dependency") {
    import spark.implicits._
    arrayMedia("https://example.invalid/media").foreach { media =>
      val value = AIJson.fromAny(media).compactPrint
      val stage = new AISystemOne().setStateCol("state").setStateMode("json")
      intercept[org.apache.spark.SparkException](stage.validateDurableRows(Seq(value).toDF("state")))
      val scalar = new AIPrompt().setInputCol("state").setExtraBody(Map("extension" -> media))
      intercept[IllegalArgumentException](AIDurableInput.validate(scalar, Seq("hello").toDF("state").schema))
    }
    val inline = arrayMedia(validImage).map(AIJson.fromAny(_).compactPrint)
    new AISystemOne().setStateCol("state").setStateMode("json")
      .validateDurableRows((inline :+ """{"file_data":"https://example.invalid/literal"}""").toDF("state"))
  }

  test("durable media arrays in native state and body columns abort before inference") {
    delta()
    import spark.implicits._
    Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.SystemResponse)) { server =>
      Seq(false, true).foreach { bodyColumn =>
        val stage = new AISystemOne().setUrl(server.url).setModel("model")
          .setQuestions(NativeRequestFixtures.SystemQuestions).setOutputCol("answer").setErrorCol("error")
        if (bodyColumn) stage.setStateCol("text").setExtraBodyCol("payload")
        else stage.setStateCol("payload").setStateMode("json")
        val media = arrayMedia("https://example.invalid/media")
        (Seq(media(if (bodyColumn) 3 else 0)) ++ media.takeRight(2)).foreach { value =>
          val root = path()
          val payload = AIJson.fromAny(value)
          val data = Seq(("hello", "{}"), ("hello", payload.compactPrint)).toDF("text", "payload")
          intercept[org.apache.spark.SparkException](stage.setOutputPath(root).transform(data))
          assert(server.requests.isEmpty && AIPromptDelta.control(spark, root).state == "initializing")
        }
      }
    }
  }

  Seq("stateCol", "inputCol", "promptTemplate").zipWithIndex.foreach { case (binding, index) =>
    test(s"durable System One JSON $binding rejects resources and commits malformed state without replay") {
      delta()
      import spark.implicits._
      Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.SystemResponse)) { server =>
        Seq(new AISystemOne(), new AIPrompt().setApiType("systemone")).foreach { configured =>
          configured.setUrl(server.url).setModel("model").setInputMode("json")
            .setQuestions(NativeRequestFixtures.SystemQuestions).setOutputCol("answer").setErrorCol("error")
          binding match {
            case "stateCol" => configured.setStateCol("payload")
            case "inputCol" => configured.setInputCol("payload")
            case "promptTemplate" => configured.setPromptTemplate("{payload}")
          }
          val before = server.requests.size()
          val resources = Seq(JsObject("file_id" -> JsString("provider-file")),
            JsObject("image_url" -> JsString("https://example.invalid/media")))
          val states = Seq("""{"nested":[{"file_id":"provider-file"}]}""") ++
            resources.map(value => underOutputSchema(value)(index).compactPrint)
          states.foreach { state =>
            intercept[org.apache.spark.SparkException] {
              configured.setOutputPath(path()).transform(Seq(state).toDF("payload"))
            }
            assert(server.requests.size() == before)
          }
          val root = path()
          val stage = configured.copy(ParamMap.empty).setOutputPath(root)
          val safe = """{"text":"https://example.invalid/inert","image_url":"data:image/png;base64,AQID"}"""
          val data = Seq(safe, "not json").toDF("payload")
          val result = stage.transform(data)
          assert(result.schema == stage.transformSchema(data.schema))
          val rows = result.collect().map(row => row.getAs[String]("payload") -> row).toMap
          assert(rows(safe).getAs[Row]("error") == null)
          assert(rows("not json").getAs[Row]("error").getString(0).contains("invalid_request"))
          assert(server.requests.size() == before + 1)
          assert(server.requests.asScala.last._2.parseJson.asJsObject.fields("state") == safe.parseJson)
          val status = AIPrompt.getOutputStatus(spark, root)
          assert(status.state == "complete" && status.rowErrorCount == 1 && status.pendingCount == 0)
          assert(stage.resume(spark).count() == 2 && server.requests.size() == before + 1)
        }
      }
    }
  }

  test("undefined durable lifecycle states fail status reads output reads and resume without inference") {
    delta()
    val session = spark
    import session.implicits._
    withServer { server =>
      val root = path()
      val stage = prompt(root, server)
      stage.transform(Seq("original").toDF("text"))
      val sends = server.requests.get()
      val qualified = AIPromptDelta.qualify(session, root)
      val control = AIPromptDelta.table(qualified, "_control")
      Seq("ready", "unknown").foreach { state =>
        session.sql(s"UPDATE $control SET state = ${AIPromptDelta.literal(state)}").collect()
        val status = intercept[IllegalArgumentException](AIPrompt.getOutputStatus(session, root))
        val output = intercept[IllegalArgumentException](AIPrompt.loadOutput(session, root))
        val resume = intercept[IllegalArgumentException](stage.resume(session))
        Seq(status, output, resume).foreach(error => assert(error.getMessage.contains("Corrupt AIPrompt run state")))
        assert(server.requests.get() == sends)
      }
      session.sql(s"UPDATE $control SET state = 'complete'").collect()
      assert(AIPrompt.getOutputStatus(session, root).isComplete)
      assert(AIPrompt.loadOutput(session, root).count() == 1)
      assert(stage.resume(session).count() == 1 && server.requests.get() == sends)
    }
  }

  test("owned resume refuses before saved template validation and retains validation after release") {
    delta()
    val session = spark.newSession()
    import session.implicits._
    withServer { server =>
      val root = path()
      val stage = prompt(root, server).setPromptTemplate("{lower(text)}")
      intercept[IllegalStateException] {
        AIPromptOutput.transformObserved(stage, Seq("HELLO").toDF("text"), event =>
          if (event == "ready") throw new IllegalStateException("synthetic stopped owner"))
      }
      val qualified = AIPromptDelta.qualify(session, root)
      val before = AIPromptDelta.control(session, qualified)
      assert(before.state == "running" && before.owner.isDefined && server.requests.get() == 0)
      session.udf.register("lower", (text: String) => Seq("changed:" + text))
      val owned = intercept[IllegalArgumentException](stage.resume(session))
      assert(owned.getMessage.contains("already has an owner"))
      assert(AIPromptDelta.control(session, qualified) == before && server.requests.get() == 0)
      AIPrompt.releaseOutputOwner(session, root, before.owner.get, confirmStopped = true)
      val invalid = intercept[IllegalArgumentException](stage.resume(session))
      assert(invalid.getMessage.contains("Prompt templates must produce strings"))
      assert(AIPromptDelta.control(session, qualified).owner.isEmpty && server.requests.get() == 0)
      assert(stage.resume(session.newSession()).count() == 1 && server.requests.get() == 1)
      assert(AIPrompt.getOutputStatus(session, root).isComplete)
    }
  }

  test("completed resume reads committed rows without resolving saved template functions again") {
    delta()
    val session = spark.newSession()
    import session.implicits._
    withServer { server =>
      val root = path()
      val stage = prompt(root, server).setPromptTemplate("{lower(text)}")
      val original = stage.transform(Seq("HELLO", "WORLD").toDF("text"))
      val rows = original.collect().toSet
      val before = AIPrompt.getOutputStatus(session, root)
      assert(before.isComplete && before.ownerToken.isEmpty && server.requests.get() == 2)
      session.udf.register("lower", (text: String) => Seq("changed:" + text))
      val resumed = stage.resume(session)
      assert(resumed.schema == original.schema && resumed.collect().toSet == rows)
      assert(AIPrompt.getOutputStatus(session, root) == before && server.requests.get() == 2)
      val mismatch = intercept[IllegalArgumentException] {
        stage.copy(ParamMap.empty).setPromptTemplate("changed {lower(text)}").resume(session)
      }
      assert(mismatch.getMessage.contains("do not match the saved run"))
      assert(server.requests.get() == 2)
    }
  }

  test("durable initialization rejects schema evolution before validating the committed input") {
    delta()
    import spark.implicits._
    withServer { server =>
      val root = path()
      val qualified = AIPromptDelta.qualify(spark, root)
      val stage = prompt(root, server)
      val error = intercept[IllegalArgumentException] {
        AIPromptOutput.transformObserved(stage, Seq("original").toDF("text"), event =>
          if (event == "input_committed") {
            val manifest = AIPromptDelta.control(spark, qualified)
            val input = AIPromptDelta.latest(spark, qualified, "_input")._2
            input.withColumn(manifest.keyColumn, AIPromptDelta.column(manifest.keyColumn) + lit(1L))
              .withColumn("foreign", lit("not supplied")).write.format("delta").mode("append")
              .option("mergeSchema", "true").save(AIPromptDelta.child(qualified, "_input"))
          })
      }
      assert(error.getMessage.contains("schema evolution is not supported"))
      val manifest = AIPromptDelta.control(spark, qualified)
      assert(manifest.state == "initializing" && manifest.inputVersion.isEmpty && manifest.owner.isDefined)
      assert(!new File(root, "results").exists() && server.requests.get() == 0)
      intercept[IllegalArgumentException](stage.resume(spark))
      assert(server.requests.get() == 0)
    }
  }

  test("durable initialization allows compaction of the committed input without changing its rows") {
    delta()
    import spark.implicits._
    withServer { server =>
      val root = path()
      val qualified = AIPromptDelta.qualify(spark, root)
      val stage = prompt(root, server).setOutputBatchSize(4)
      val input = Seq("one", "two", "three", "four").toDF("text").repartition(2)
      var compacted = Option.empty[Long]
      val output = AIPromptOutput.transformObserved(stage, input, event =>
        if (event == "input_committed") {
          assert(AIPromptDelta.version(spark, qualified, "_input") == 0L)
          spark.sql(s"OPTIMIZE ${AIPromptDelta.table(qualified, "_input")}").collect()
          compacted = Some(AIPromptDelta.version(spark, qualified, "_input"))
          assert(compacted.get > 0L)
        })
      assert(output.select("text").as[String].collect().toSet == Set("one", "two", "three", "four"))
      assert(AIPromptDelta.control(spark, qualified).inputVersion == compacted)
      assert(AIPrompt.getOutputStatus(spark, root).allSucceeded && server.requests.get() == 4)
      assert(stage.resume(spark).count() == 4 && server.requests.get() == 4)
    }
  }

  Seq(false, true).foreach { complete =>
    test(s"durable input maintenance after initialization is rejected with guidance for complete=$complete") {
      delta()
      import spark.implicits._
      withServer { server =>
        val root = path()
        val qualified = AIPromptDelta.qualify(spark, root)
        val stage = prompt(root, server).setOutputBatchSize(2)
        val input = Seq("one", "two", "three", "four").toDF("text").repartition(2)
        if (complete) {
          assert(stage.transform(input).count() == 4)
        } else {
          intercept[IllegalStateException] {
            AIPromptOutput.transformObserved(stage, input, event =>
              if (event == "batch_committed") throw new IllegalStateException("stop after committed batch"))
          }
          val status = AIPrompt.getOutputStatus(spark, root)
          assert(status.committedCount == 2 && status.pendingCount == 2)
          AIPrompt.releaseOutputOwner(spark, root, status.ownerToken.get, confirmStopped = true)
        }
        val before = AIPromptDelta.control(spark, qualified)
        val resultsVersion = AIPromptDelta.version(spark, qualified, "results")
        val sends = server.requests.get()
        spark.sql(s"OPTIMIZE ${AIPromptDelta.table(qualified, "_input")}").collect()
        assert(AIPromptDelta.version(spark, qualified, "_input") > before.inputVersion.get)
        assert(AIPromptDelta.read(spark, qualified, "_input", before.inputVersion.get).count() == 4)
        val operations = Seq[() => Any](
          () => AIPrompt.loadOutput(spark, root).collect(),
          () => AIPrompt.getOutputStatus(spark, root),
          () => stage.resume(spark).collect())
        operations.foreach { operation =>
          val error = intercept[IllegalArgumentException](operation())
          assert(error.getMessage.contains("immutable AIPrompt input snapshot"))
          assert(error.getMessage.contains("do not OPTIMIZE or VACUUM"))
        }
        assert(AIPromptDelta.control(spark, qualified) == before)
        assert(AIPromptDelta.version(spark, qualified, "results") == resultsVersion)
        assert(server.requests.get() == sends && sends == (if (complete) 4 else 2))
      }
    }
  }

  Seq("input_committed", "input_validated", "results_created").foreach { boundary =>
    test(s"durable initialization retains the validated snapshot when input changes after $boundary") {
      delta()
      val session = spark
      import session.implicits._
      withServer { server =>
        val root = path()
        val qualified = AIPromptDelta.qualify(session, root)
        val stage = prompt(root, server)
        val failure = Try(AIPromptOutput.transformObserved(stage, Seq("original").toDF("text"), event =>
          if (event == boundary) {
            val manifest = AIPromptDelta.control(session, qualified)
            val input = AIPromptDelta.latest(session, qualified, "_input")._2
            input.withColumn(manifest.keyColumn, AIPromptDelta.column(manifest.keyColumn) + lit(1L))
              .withColumn("text", lit("not validated")).write.format("delta").mode("append")
              .save(AIPromptDelta.child(qualified, "_input"))
          })).failed.toOption
        assert(server.requests.get() == 0, "Unvalidated input must not reach inference")
        assert(failure.exists(error => error.isInstanceOf[IllegalArgumentException] &&
          error.getMessage.contains("immutable AIPrompt input snapshot")))
        val manifest = AIPromptDelta.control(session, qualified)
        assert(manifest.state == "initializing" && manifest.inputVersion.isEmpty && manifest.owner.isDefined)
        intercept[IllegalArgumentException](stage.resume(session))
        assert(server.requests.get() == 0)
      }
    }
  }
}
