// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.services.openai.OpenAIDecisions
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spray.json._

import java.util.Collections
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.util.Using

class AIPromptMediaInputSuite extends AIPromptTestBase {
  private val validImage = "data:image/png;base64,AQID"
  private val malformedImages = Seq("", "data:image/png,AQID", "data:image/png")
  private val chatReply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
  private val responsesReply = """{"status":"completed","output":[{"type":"message","role":"assistant",
    |"content":[{"type":"output_text","text":"answer"}]}]}""".stripMargin

  private def mediaInput: DataFrame = {
    import spark.implicits._
    (malformedImages :+ validImage).toDF("asset").withColumn("text", lit("describe"))
  }

  private def messageInput(api: String): DataFrame = {
    val partType = if (api == "chat_completions") "image_url" else "input_image"
    mediaInput.withColumn("messages", array(struct(lit("user").as("role"),
      array(map(lit("type"), lit(partType), lit("image_url"), col("asset"))).as("content"))))
  }

  private def verifyMedia(api: String, input: DataFrame, expectedCodes: Seq[String], sends: Int)
                         (configure: AIPrompt => AIPrompt): Unit = {
    val reply = api match {
      case "decisions" => NativeRequestFixtures.DecisionResponse
      case "responses" => responsesReply
      case _ => chatReply
    }
    Using.resource(new AIRecordingServer((_, body) => {
      if (body.contains(validImage)) 200 -> reply
      else 400 -> """{"error":{"code":"invalid_media","message":"synthetic malformed media"}}"""
    })) { server =>
      val stage = configure(new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
        .setOutputCol("answer").setErrorCol("error"))
      val output = stage.transform(input)
      assert(output.schema == stage.transformSchema(input.schema))
      val rows = output.collect()
      val byAsset = rows.map(row => row.getAs[String]("asset") -> row).toMap
      assert(rows.length == 4 && byAsset.size == 4)
      malformedImages.zip(expectedCodes).foreach { case (asset, code) =>
        val error = byAsset(asset).getAs[Row]("error")
        assert(error != null && AIJson.objectString(error.getString(0)).fields("code") == JsString(code))
      }
      assert(byAsset(validImage).getAs[Row]("error") == null && byAsset(validImage).getAs[Any]("answer") != null)
      assert(server.requests.size() == sends)
    }
  }

  Seq("chat_completions", "responses").foreach { api =>
    test(s"malformed inline $api paths retain local row errors") {
      verifyMedia(api, mediaInput, Seq("missing_input", "invalid_request", "invalid_request"), sends = 1) {
        _.setPromptTemplate("{asset}").setColumnTypes(Map("asset" -> "path"))
      }
    }

    test(s"malformed inline $api messages retain local and provider row errors") {
      verifyMedia(api, messageInput(api), Seq("invalid_request", "http_error", "http_error"), sends = 3) {
        _.setMessagesCol("messages")
      }
    }

    test(s"$api attachments follow input column order through copies and persistence") {
      import spark.implicits._
      val left = validImage
      val right = "data:image/png;base64,BAUG"
      val data = Seq((left, right)).toDF("left", "right").select("right", "left")
      val reply = if (api == "chat_completions") chatReply else responsesReply
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setApiType(api)
          .setOutputCol("answer").setErrorCol("error")
          .setPromptTemplate("Compare {left} and {`RiGhT`}; literal {{LEFT}}")
          .setColumnTypes(ListMap("LEFT" -> "path", "RIGHT" -> "path"))
        val reordered = stage.copy(ParamMap.empty).setColumnTypes(ListMap("RIGHT" -> "path", "LEFT" -> "path"))
        val saved = path()
        reordered.write.session(spark).save(saved)
        Seq(stage, reordered, AIPrompt.load(saved)).foreach { configured =>
          assert(configured.transform(data).head().getAs[String]("answer") == "answer")
        }
        assert(server.requests.size() == 3)
        server.requests.asScala.foreach { case (_, body) =>
          assert(body.indexOf(right) >= 0 && body.indexOf(right) < body.indexOf(left))
          assert(body.contains("Compare attachment.png and attachment.png; literal {LEFT}"))
        }
      }
    }
  }

  test("malformed inline Decisions JSON rejects invalid rows without sending") {
    val input = messageInput("decisions").withColumn("input", to_json(col("messages"))).drop("messages")
    verifyMedia("decisions", input, Seq.fill(3)("invalid_request"), sends = 1) {
      _.setInputCol("input").setInputMode("json").setQuestions(NativeRequestFixtures.DecisionQuestions)
    }
  }

  test("Decisions image URL casing and MIME parameters preserve payloads") {
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
    for (json <- Seq(false, true); stage <- Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions())) {
      val input = if (json) messages.withColumn("input", to_json(col("messages"))).drop("messages") else messages
      Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
        stage.setUrl(server.url + "/v1").setModel("model").setQuestions(NativeRequestFixtures.DecisionQuestions)
          .setOutputCol("answer").setErrorCol("error")
        if (json) stage.setInputCol("input").setInputMode("json") else stage.setMessagesCol("messages")
        val output = stage.transform(input)
        assert(output.schema == stage.transformSchema(input.schema))
        val rows = output.collect()
        val byAsset = rows.map(row => row.getAs[String]("asset") -> row).toMap
        assert(rows.length == accepted.size + rejected.size && byAsset.size == rows.length)
        accepted.foreach { asset =>
          assert(byAsset(asset).getAs[Row]("error") == null && byAsset(asset).getAs[Row]("answer") != null, asset)
        }
        rejected.foreach { asset =>
          assert(byAsset(asset).getAs[Row]("error").getString(0).contains("invalid_request"), asset)
          assert(byAsset(asset).getAs[Row]("answer") == null)
        }
        assert(server.requests.size() == accepted.size)
      }
    }
  }

  test("Decisions null image options normalize through direct and facade stages") {
    import spark.implicits._
    Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions()).foreach { stage =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
        val image = Map("type" -> JsString("input_image"), "image_url" -> JsString(validImage),
          "provider_extension" -> JsNull)
        val variants = Seq(Map("file_id" -> JsNull), Map("detail" -> JsNull),
          Map("file_id" -> JsNull, "detail" -> JsNull))
        val data = variants.map { fields =>
          JsArray(JsObject("role" -> JsString("user"),
            "content" -> JsArray(JsObject(image ++ fields)))).compactPrint
        }.toDF("text")
        stage.setUrl(server.url + "/v1").setModel("model").setInputCol("text").setInputMode("json")
          .setQuestions(NativeRequestFixtures.DecisionQuestions).setOutputCol("answer").setErrorCol("error")
        val output = stage.transform(data)
        assert(output.schema == stage.transformSchema(data.schema))
        val rows = output.collect()
        assert(rows.length == 3 && rows.forall(row =>
          row.getAs[Row]("error") == null && row.getAs[Row]("answer") != null))
        assert(server.requests.size() == 3)
        server.requests.asScala.foreach { case (_, body) =>
          val message = AIJson.objectString(body).fields("input").asInstanceOf[JsArray].elements.head.asJsObject
          val part = message.fields("content").asInstanceOf[JsArray].elements.head.asJsObject
          assert(!part.fields.contains("file_id") && !part.fields.contains("detail"))
          assert(part.fields("provider_extension") == JsNull)
        }
      }
    }
  }

  test("attachment wildcard templates use only public columns") {
    val data = spark.range(3).select(col("id").cast(StringType).as("ticket"),
      lit(validImage).as("asset"), lit("user-owned").as("asset_filename"))
    val expected = data.toJSON.collect().toVector.sorted
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model")
        .setPromptTemplate("{asset};{to_json(struct(*))}").setColumnTypes(Map("asset" -> "path"))
        .setOutputCol("answer").setErrorCol("error")
      val result = stage.transform(data)
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
    }
  }

  test("path normalization preserves missing-input errors and inline attachments") {
    import spark.implicits._
    val data = Seq(None, Some(""), Some(" \t "), Some("data:text/plain;base64,aGVsbG8="),
      Some(" \tDATA:text/plain;BASE64,aGVsbG8= \t")).toDF("asset")
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model")
        .setPromptTemplate("{asset}").setColumnTypes(Map("asset" -> "path"))
        .setOutputCol("answer").setErrorCol("error")
      val rows = stage.transform(data).collect()
      assert(rows.length == 5)
      val errors = rows.flatMap(row => Option(row.getAs[Row]("error")))
      assert(errors.length == 3)
      errors.foreach { row =>
        val error = AIJson.objectString(row.getString(0))
        assert(error.fields("code") == JsString("missing_input"))
        assert(error.fields("message") == JsString("attachment input is missing"))
      }
      assert(server.requests.size() == 2)
      server.requests.asScala.foreach { case (_, body) =>
        val message = AIJson.objectString(body).fields("messages").asInstanceOf[JsArray].elements.last.asJsObject
        assert(message.fields("content").asInstanceOf[JsString].value.takeWhile(_ != '\n') == "attachment.txt")
      }
    }
  }

  test("large inline messages remain valid") {
    val inline = "data:image/png;base64," + ("A" * 20000004)
    val message = new StructType().add("role", StringType)
      .add("content", ArrayType(MapType(StringType, StringType)))
    val data = spark.createDataFrame(Collections.singletonList(
      Row(Seq(Row("user", Seq(Map("type" -> "input_image", "image_url" -> inline)))))),
      new StructType().add("messages", ArrayType(message)))
    Using.resource(new AIRecordingServer((_, _) => 200 -> responsesReply)) { server =>
      val stage = new AIPrompt().setApiType("responses").setUrl(server.url + "/v1").setModel("model")
        .setMessagesCol("messages").setOutputCol("answer").setErrorCol("error")
      val row = stage.transform(data).head()
      assert(row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null)
      assert(server.requests.size() == 1 && server.requests.peek()._2.contains(inline))
    }
  }

  test("Responses forwards literal file_data and preserves provider validation failures") {
    val raw = "JVBERi0xLjQK"
    val inline = "data:application/pdf;base64," + raw
    val contents = Seq(raw, inline, "not base64", "https://example.invalid/not-a-location")
    val messageType = new StructType().add("role", StringType)
      .add("content", ArrayType(MapType(StringType, StringType)))
    val input = spark.createDataFrame(contents.map { text =>
      Row(text, Seq(Row("user", Seq(Map("type" -> "input_file", "filename" -> "note.pdf", "file_data" -> text)))))
    }.asJava, new StructType().add("text", StringType).add("messages", ArrayType(messageType))).coalesce(1)
    def fileData(body: String): String = {
      val message = AIJson.objectString(body).fields("input").asInstanceOf[JsArray].elements.head.asJsObject
      message.fields("content").asInstanceOf[JsArray].elements.head.asJsObject
        .fields("file_data").asInstanceOf[JsString].value
    }
    Using.resource(new AIRecordingServer((_, body) => {
      if (Set(raw, inline)(fileData(body))) 200 -> responsesReply
      else 400 -> """{"error":{"code":"invalid_file","message":"Unsupported inline file content"}}"""
    })) { server =>
      val stage = new AIPrompt().setApiType("responses").setUrl(server.url + "/v1").setModel("model")
        .setMessagesCol("messages").setOutputCol("answer").setErrorCol("error")
      val rows = stage.transform(input).collect()
      assert(rows.length == 4 && rows.count(_.getAs[Row]("error") != null) == 2)
      assert(server.requests.asScala.map(entry => fileData(entry._2)).toSeq.sorted == contents.sorted)
    }
  }
}
