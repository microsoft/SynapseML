// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.services.openai.OpenAIDecisions
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spray.json._

import java.awt.Color
import java.awt.image.BufferedImage
import java.io.ByteArrayOutputStream
import java.nio.file.{Files, Paths}
import java.util.{Base64, Collections}
import javax.imageio.ImageIO
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.util.Using

class AIPromptMediaInputSuite extends AIPromptTestBase {
  private val validImage = "data:image/png;base64,AQID"
  private val malformedImages = Seq("", "data:image/png,AQID", "data:image/png")
  private val chatReply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
  private val responsesReply = """{"status":"completed","output":[{"type":"message","role":"assistant",
    |"content":[{"type":"output_text","text":"answer"}]}]}""".stripMargin
  private def imageBytes(format: String): Array[Byte] = Using.resource(new ByteArrayOutputStream()) { output =>
    val image = new BufferedImage(2, 2, BufferedImage.TYPE_INT_RGB)
    image.setRGB(0, 0, Color.RED.getRGB)
    image.setRGB(1, 1, Color.BLUE.getRGB)
    assert(ImageIO.write(image, format, output))
    output.toByteArray
  }
  private lazy val pngBytes = imageBytes("png")
  private lazy val pngDataUrl = "data:image/png;base64," + Base64.getEncoder.encodeToString(pngBytes)

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

  test("Decisions hosted URLs and inline image casing and MIME parameters preserve payloads") {
    import spark.implicits._
    val accepted = Seq(validImage, "DATA:image/png;base64,AQID", "data:IMAGE/PNG;base64,AQID",
      "data:image/png;BASE64,AQID", "data:image/png;name=a.png;base64,AQID",
      "DATA:IMAGE/PNG;NAME=a%20b.png;charset=UTF-8;BASE64,AQID",
      "https://example.invalid/photo.png?signature=a%2Bb&version=2", "http://example.invalid:8080/photo.jpg",
      "https://img_cdn.example.invalid/photo.png", "https://b\u00fccher.example.invalid/photo.png")
    val rejected = Seq("data:text/plain;base64,AQID", "data:image/png;name;base64,AQID",
      "data:image/png;=a.png;base64,AQID", "data:image/png;name=bad%xy;base64,AQID",
      "data:image/png;base64;name=a.png,AQID", "data:image/png;name=a.png;base64,@@@",
      "data:image/png;name=a.png;base64,", "data:image/png;name=a.png,AQID", "https:///image.png")
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
          val error = AIJson.objectString(byAsset(asset).getAs[Row]("error").getString(0))
          assert(error.fields("code") == JsString("invalid_request"), asset)
          if (asset == "https:///image.png") {
            assert(error.fields("message") ==
              JsString("requirement failed: Decisions images must use HTTP(S) URLs or inline base64 image data URLs"))
          }
          assert(byAsset(asset).getAs[Row]("answer") == null)
        }
        assert(server.requests.size() == accepted.size)
        val forwarded = server.requests.asScala.map { case (_, body) =>
          AIJson.objectString(body).fields("input").asInstanceOf[JsArray].elements.head.asJsObject
            .fields("content").asInstanceOf[JsArray].elements.head.asJsObject.fields("image_url")
        }.toSeq
        assert(forwarded.toSet == accepted.map(JsString(_)).toSet)
      }
    }
  }

  test("Decisions question images survive column bindings copies and persistence") {
    import spark.implicits._
    val text = JsObject("type" -> JsString("input_text"), "text" -> JsString("Compare with the shared image."))
    val image = JsObject("type" -> JsString("input_image"),
      "image_url" -> JsString("https://example.invalid/candidate.png?signature=a%2Bb"),
      "provider_extension" -> JsNull)
    val suppliedImage = JsObject(image.fields ++ Map("file_id" -> JsNull, "detail" -> JsNull))
    val question = JsObject("type" -> JsString("predicate"), "name" -> JsString("matches"),
      "instructions" -> JsArray(text, suppliedImage))
    val questions = JsArray(question)
    val expectedQuestions = JsArray(JsObject(question.fields.updated("instructions", JsArray(text, image))))
    val evidence = JsArray(JsObject("role" -> JsString("user"), "content" -> JsArray(
      JsObject("type" -> JsString("input_image"), "image_url" -> JsString(validImage)))))
    val data = Seq((evidence.compactPrint, questions.compactPrint)).toDF("evidence", "questions")
    for (fromColumn <- Seq(false, true)) {
      Seq(
        (new AIPrompt().setApiType("decisions"), AIPrompt.load _),
        (new OpenAIDecisions(), OpenAIDecisions.load _)
      ).foreach { case (stage, load) =>
        Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
          stage.setUrl(server.url + "/v1").setModel("model").setInputCol("evidence").setInputMode("json")
            .setOutputCol("answer").setErrorCol("error")
          if (fromColumn) stage.setQuestionsCol("questions") else stage.setQuestions(questions.compactPrint)
          val saved = path()
          stage.write.session(spark).save(saved)
          Seq(stage, stage.copy(ParamMap.empty), load(saved)).foreach { configured =>
            val output = configured.transform(data)
            assert(output.schema == configured.transformSchema(data.schema))
            val row = output.head()
            assert(row.getAs[Row]("error") == null && row.getAs[Row]("answer") != null)
          }
          assert(server.requests.size() == 3)
          server.requests.asScala.foreach { case (route, body) =>
            assert(route == "/v1/decisions")
            val sent = AIJson.objectString(body)
            assert(sent.fields("input") == evidence && sent.fields("questions") == expectedQuestions)
          }
        }
      }
    }
  }

  test("Decisions path columns load real images through templates input columns copies and persistence") {
    import spark.implicits._
    val imageFile = Files.write(Paths.get(path() + ".png"), pngBytes)
    Using.resource(new AIRecordingServer((_, _) => 200 -> "", encodeResponse = _ => pngBytes)) { imageServer =>
      val sources = Seq(
        ("local", imageFile.toUri.toString, imageFile.getFileName.toString),
        ("http", imageServer.url + "/photo.png", "photo.png"),
        ("http-extensionless", imageServer.url + "/photo", "photo"),
        ("inline", pngDataUrl, "attachment.png")).map { case (kind, image, name) =>
        (kind, image, name, pngDataUrl)
      } ++ Seq("jpg" -> "jpeg", "jpeg" -> "jpeg", "gif" -> "gif").flatMap { case (extension, mime) =>
        val bytes = imageBytes(if (mime == "jpeg") "jpg" else "gif")
        val file = Files.write(Paths.get(path() + "." + extension), bytes)
        val inline = s"data:image/$mime;base64," + Base64.getEncoder.encodeToString(bytes)
        Seq((s"local-$extension", file.toUri.toString, file.getFileName.toString, inline),
          (s"inline-$extension", inline, if (mime == "jpeg") "attachment.jpg" else "attachment.gif", inline))
      }
      val data = sources.map { case (kind, image, _, _) => (s"Inspect $kind", image) }.toDF("text", "asset")
      for (fromColumn <- Seq(false, true)) {
        Seq(
          (new AIPrompt().setApiType("decisions"), AIPrompt.load _),
          (new OpenAIDecisions(), OpenAIDecisions.load _)
        ).foreach { case (stage, load) =>
          Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
            stage.setUrl(server.url + "/v1").setModel("model").setQuestions(NativeRequestFixtures.DecisionQuestions)
              .setColumnTypes(Map("text" -> "text", "asset" -> "path")).setFileSizeLimitMB(1)
              .setOutputCol("answer").setErrorCol("error")
            if (fromColumn) stage.setInputCol("text") else stage.setPromptTemplate("{text}: {asset}")
            val saved = path()
            stage.write.session(spark).save(saved)
            Seq(stage, stage.copy(ParamMap.empty), load(saved)).foreach { configured =>
              val downloads = imageServer.requests.size()
              val result = configured.transform(data)
              assert(result.schema == configured.transformSchema(data.schema))
              assert(imageServer.requests.size() == downloads)
              val rows = result.collect()
              assert(rows.length == sources.size)
              assert(rows.forall(row => row.getAs[Row]("error") == null && row.getAs[Row]("answer") != null))
              assert(imageServer.requests.size() == downloads + 2)
              assert(imageServer.requests.asScala.forall(request => Set("/photo.png", "/photo")(request._1)))
            }
            val expected = sources.map { case (kind, _, filename, inline) =>
              (if (fromColumn) s"Inspect $kind" else s"Inspect $kind: $filename") -> inline
            }.toMap
            assert(server.requests.size() == sources.size * 3)
            val actualText = server.requests.asScala.map { case (route, body) =>
              assert(route == "/v1/decisions")
              val sent = AIJson.objectString(body)
              assert(sent.fields("questions") == NativeRequestFixtures.DecisionQuestions.parseJson)
              val input = sent.fields("input").asInstanceOf[JsArray].elements
              assert(input.size == 1 && input.head.asJsObject.fields("role") == JsString("user"))
              val content = input.head.asJsObject.fields("content").asInstanceOf[JsArray].elements
              assert(content.size == 2)
              assert(content.head.asJsObject.fields("type") == JsString("input_text"))
              val text = content.head.asJsObject.fields("text").asInstanceOf[JsString].value
              assert(content(1) == JsObject("type" -> JsString("input_image"), "image_url" -> JsString(expected(text))))
              text
            }.toSet
            assert(actualText == expected.keySet)
          }
        }
      }
    }
  }

  test("Decisions invalid prompt text rejects path rows before image IO") {
    import spark.implicits._
    Using.resource(new AIRecordingServer((_, _) => 200 -> "", encodeResponse = _ => pngBytes)) { imageServer =>
      val cases = Seq(
        (Option.empty[String], "missing_input", "input is null or empty"),
        (Some(""), "missing_input", "input is null or empty"),
        (Some(" \t "), "invalid_request", "Native question or input text must be nonempty"))
      val data = cases.toDF("text", "expectedCode", "expectedMessage")
        .withColumn("asset", lit(imageServer.url + "/photo.png"))
      for (fromColumn <- Seq(false, true);
           stage <- Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions())) {
        Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
          stage.setUrl(server.url + "/v1").setModel("model").setQuestions(NativeRequestFixtures.DecisionQuestions)
            .setColumnTypes(Map("asset" -> "path")).setOutputCol("answer").setErrorCol("error")
          if (fromColumn) stage.setInputCol("text") else stage.setPromptTemplate("{text}")
          val result = stage.transform(data)
          assert(result.schema == stage.transformSchema(data.schema))
          val rows = result.collect()
          assert(rows.length == cases.size)
          rows.foreach { row =>
            val error = AIJson.objectString(row.getAs[Row]("error").getString(0))
            assert(error.fields("code") == JsString(row.getAs[String]("expectedCode")))
            assert(error.fields("message") == JsString(row.getAs[String]("expectedMessage")))
            assert(row.getAs[Row]("answer") == null)
          }
          assert(server.requests.isEmpty && imageServer.requests.isEmpty)
        }
      }
    }
  }

  test("WEBP path images retain their MIME type across Decisions Chat and Responses") {
    import spark.implicits._
    val encoded = "UklGRhwAAABXRUJQVlA4TA8AAAAvAUAAAAcQ/Y/+ByKi/wEA"
    val bytes = Base64.getDecoder.decode(encoded)
    val dataUrl = "data:image/webp;base64," + encoded
    val file = Files.write(Paths.get(path() + ".webp"), bytes)
    val extensionlessFile = Files.write(Paths.get(path()), bytes)
    Using.resource(new AIRecordingServer((_, _) => 200 -> "", encodeResponse = _ => bytes)) { imageServer =>
      val data = Seq(file.toUri.toString, extensionlessFile.toUri.toString,
        imageServer.url + "/photo.webp", imageServer.url + "/photo", dataUrl).toDF("asset")
      val stages = Seq(new OpenAIDecisions(), new AIPrompt().setApiType("decisions"),
        new AIPrompt().setApiType("responses"), new AIPrompt().setApiType("chat_completions"))
      stages.foreach { stage =>
        val reply = stage.getApiType match {
          case "decisions" => NativeRequestFixtures.DecisionResponse
          case "responses" => responsesReply
          case _ => chatReply
        }
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          stage.setUrl(server.url + "/v1").setModel("model").setPromptTemplate("Inspect {asset}")
            .setColumnTypes(Map("asset" -> "path")).setOutputCol("answer").setErrorCol("error")
          if (stage.getApiType == "decisions") stage.setQuestions(NativeRequestFixtures.DecisionQuestions)
          val result = stage.transform(data)
          assert(result.schema == stage.transformSchema(data.schema))
          val rows = result.collect()
          assert(rows.length == 5 && rows.forall(row =>
            row.getAs[Row]("error") == null && row.getAs[Any]("answer") != null))
          assert(server.requests.size() == 5)
          assert(server.requests.asScala.forall(_._2.contains(dataUrl)))
        }
      }
      assert(imageServer.requests.size() == stages.size * 2)
    }
  }

  test("extensionless WAV and truncated RIFF paths remain audio across prompting APIs") {
    import spark.implicits._
    val wav = Base64.getDecoder.decode("UklGRiQAAABXQVZFZm10IBAAAAABAAEARKwAAIhYAQACABAAZGF0YQAAAAA=")
    val shortRiff = "RIFF0000WEB".getBytes("US-ASCII")
    val file = Files.write(Paths.get(path()), wav)
    val shortFile = Files.write(Paths.get(path()), shortRiff)
    Using.resources(
      new AIRecordingServer((_, _) => 200 -> "", encodeResponse = _ => wav),
      new AIRecordingServer((_, _) => 200 -> "", encodeResponse = _ => shortRiff)) { (imageServer, shortServer) =>
      val data = Seq(file.toUri.toString, imageServer.url + "/audio",
        shortFile.toUri.toString, shortServer.url + "/short").toDF("asset")
      val stages = Seq(new OpenAIDecisions(), new AIPrompt().setApiType("decisions"),
        new AIPrompt().setApiType("responses"), new AIPrompt().setApiType("chat_completions"))
      stages.foreach { stage =>
        Using.resource(new AIRecordingServer((_, _) => 400 -> """{"error":{"message":"unexpected inference"}}""")) {
          server =>
            stage.setUrl(server.url + "/v1").setModel("model").setPromptTemplate("Inspect {asset}")
              .setColumnTypes(Map("asset" -> "path")).setOutputCol("answer").setErrorCol("error")
            if (stage.getApiType == "decisions") stage.setQuestions(NativeRequestFixtures.DecisionQuestions)
            val rows = stage.transform(data).collect()
            assert(rows.length == 4)
            rows.foreach { row =>
              val error = AIJson.objectString(row.getAs[Row]("error").getString(0))
              assert(error.fields("code") == JsString("invalid_request"))
              val message = error.fields("message").asInstanceOf[JsString].value
              stage.getApiType match {
                case "decisions" =>
                  assert(message == "Decisions path columns only support JPEG, PNG, GIF, and WebP images")
                case "responses" => assert(message == "Audio input is not supported in the current API version.")
                case _ => assert(message.contains("File type 'audio' with MIME type 'audio/x-wav'"))
              }
              assert(row.getAs[Any]("answer") == null)
            }
            assert(server.requests.isEmpty)
        }
      }
      assert(imageServer.requests.size() == stages.size && shortServer.requests.size() == stages.size)
    }
  }

  test("Decisions invalid request metadata rejects path rows before image IO") {
    import spark.implicits._
    val questions = NativeRequestFixtures.DecisionQuestions
    val missing = Option.empty[String].orNull
    val cases = Seq(
      (missing, "model", "v1", "{}", "Inspect", "missing_input", "questions is null"),
      ("{}", "model", "v1", "{}", "Inspect", "invalid_request", "array must be nonempty"),
      (questions, missing, "v1", "{}", "Inspect", "missing_input", "model/deployment"),
      (questions, "model", missing, "{}", "Inspect", "missing_input", "apiVersion is null"),
      (questions, "model", "v1", "[]", "Inspect", "invalid_request", "object"),
      (missing, "model", "v1", "{}", missing, "missing_input", "questions is null"))
      .toDF("questions", "model", "version", "options", "text", "expectedCode", "expectedMessage")
    Using.resource(new AIRecordingServer((_, _) => 200 -> "", encodeResponse = _ => pngBytes)) { imageServer =>
      val data = cases.withColumn("asset", lit(imageServer.url + "/photo.png"))
      for (fromColumn <- Seq(false, true);
           stage <- Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions())) {
        Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
          stage.setUrl(server.url + "/v1").setModelCol("model").setApiVersionCol("version")
            .setExtraBodyCol("options").setQuestionsCol("questions").setColumnTypes(Map("asset" -> "path"))
            .setOutputCol("answer").setErrorCol("error")
          if (fromColumn) stage.setInputCol("text") else stage.setPromptTemplate("{text}")
          val result = stage.transform(data)
          assert(result.schema == stage.transformSchema(data.schema))
          val rows = result.collect()
          assert(rows.length == 6)
          rows.foreach { row =>
            val error = AIJson.objectString(row.getAs[Row]("error").getString(0))
            assert(error.fields("code") == JsString(row.getAs[String]("expectedCode")))
            assert(error.fields("message").asInstanceOf[JsString].value
              .contains(row.getAs[String]("expectedMessage")))
            assert(row.getAs[Row]("answer") == null)
          }
          assert(server.requests.isEmpty && imageServer.requests.isEmpty)
        }
      }
    }
  }

  test("Decisions path columns preserve missing invalid IO and image size errors") {
    import spark.implicits._
    val missing = Paths.get(path(), "private-image.png").toUri.toString
    val nonImages = Seq("pdf" -> "%PDF-1.4\n", "txt" -> "not an image", "wav" -> "RIFF0000WAVE",
      "tiff" -> "II*\u0000\u0008\u0000\u0000\u0000", "bmp" -> "BM0000", "png" -> "#define image_width 2\n")
      .map { case (extension, content) =>
        val file = Files.write(Paths.get(path() + "." + extension), content.getBytes("UTF-8"))
        ("local-" + extension, Some(file.toUri.toString), "invalid_request")
      }
    val invalid = Seq(
      ("null", Option.empty[String], "missing_input"),
      ("blank", Some(" \t "), "missing_input"),
      ("missing", Some(missing), "input_io_error"),
      ("base64", Some("data:image/png;base64,@@@"), "invalid_request"),
      ("metadata-no-comma", Some("data:image/png"), "invalid_request"),
      ("metadata-no-base64", Some("data:image/png,AQID"), "invalid_request"),
      ("native-metadata", Some("data:image/png;name;base64,AQID"), "invalid_request"),
      ("empty-base64", Some("data:image/png;base64,"), "invalid_request"),
      ("jpg", Some("data:image/jpg;base64,AQID"), "invalid_request"),
      ("pdf", Some("data:application/pdf;base64,JVBERi0xLjQK"), "invalid_request"),
      ("text", Some("data:text/plain;base64,aGVsbG8="), "invalid_request"),
      ("audio", Some("data:audio/wav;base64,UklGRg=="), "invalid_request"),
      ("tiff", Some("data:image/tiff;base64,SUkqAAgAAAA="), "invalid_request"),
      ("bmp", Some("data:image/bmp;base64,Qk0wMDAw"), "invalid_request"),
      ("xbm", Some("data:image/x-bitmap;base64,I2RlZmluZSB4IDIK"), "invalid_request"),
      ("office", Some("data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,AQID"),
        "invalid_request")) ++ nonImages
    val data = invalid.toDF("kind", "asset", "expected")
    val imageFile = Files.write(Paths.get(path() + ".png"), pngBytes)
    val images = Seq(pngDataUrl, imageFile.toUri.toString).toDF("asset")
    Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions()).foreach { stage =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
        stage.setUrl(server.url + "/v1").setModel("model").setQuestions(NativeRequestFixtures.DecisionQuestions)
          .setPromptTemplate("Inspect {asset}").setColumnTypes(Map("asset" -> "path"))
          .setOutputCol("answer").setErrorCol("error")
        val rows = stage.transform(data).collect()
        assert(rows.length == invalid.size)
        rows.foreach { row =>
          val errorRow = row.getAs[Row]("error")
          assert(errorRow != null, s"Expected an error for ${row.getAs[String]("kind")}")
          val error = AIJson.objectString(errorRow.getString(0))
          assert(error.fields("code") == JsString(row.getAs[String]("expected")))
          val kind = row.getAs[String]("kind")
          if (Set("pdf", "text", "audio", "office", "tiff", "bmp", "xbm", "jpg")(kind) || kind.startsWith("local-")) {
            assert(error.fields("message") ==
              JsString("Decisions path columns only support JPEG, PNG, GIF, and WebP images"))
          }
          assert(!error.compactPrint.contains(missing) && !error.compactPrint.contains("private-image"))
          assert(row.getAs[Row]("answer") == null)
        }
        assert(server.requests.isEmpty)
        stage.setFileSizeLimitMB((pngBytes.length - 1).toDouble / (1024 * 1024))
        val tooLarge = stage.transform(images).collect()
        assert(tooLarge.forall(row => AIJson.objectString(row.getAs[Row]("error").getString(0))
          .fields("code") == JsString("invalid_request")))
        assert(server.requests.isEmpty)
        stage.setFileSizeLimitMB(pngBytes.length.toDouble / (1024 * 1024))
        assert(stage.transform(images).collect().forall(_.getAs[Row]("error") == null))
        assert(server.requests.size() == 2)
      }
    }
  }

  test("Decisions non-image path extensions reject before IO and size checks") {
    import spark.implicits._
    val pdf = "%PDF-1.4\n".getBytes("UTF-8")
    val file = Files.write(Paths.get(path() + ".pdf"), pdf)
    val missing = Paths.get(path(), "missing.pdf").toUri.toString
    Using.resource(new AIRecordingServer((_, _) => 200 -> "", encodeResponse = _ => pngBytes)) { imageServer =>
      val data = Seq(file.toUri.toString, missing, imageServer.url + "/report.pdf",
        imageServer.url + "/image.ashx", Paths.get(path(), "missing.bmp").toUri.toString,
        "data:application/pdf;base64," + Base64.getEncoder.encodeToString(pdf)).toDF("asset")
      for (bounded <- Seq(false, true);
           stage <- Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions())) {
        Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
          stage.setUrl(server.url + "/v1").setModel("model").setQuestions(NativeRequestFixtures.DecisionQuestions)
            .setPromptTemplate("Inspect {asset}").setColumnTypes(Map("asset" -> "path"))
            .setOutputCol("answer").setErrorCol("error")
          if (bounded) stage.setFileSizeLimitMB(1.0 / (1024 * 1024))
          val rows = stage.transform(data).collect()
          assert(rows.length == 6)
          rows.foreach { row =>
            val error = AIJson.objectString(row.getAs[Row]("error").getString(0))
            assert(error.fields("code") == JsString("invalid_request"))
            assert(error.fields("message") ==
              JsString("Decisions path columns only support JPEG, PNG, GIF, and WebP images"))
            assert(row.getAs[Row]("answer") == null)
          }
          assert(server.requests.isEmpty && imageServer.requests.isEmpty)
        }
      }
    }
  }

  test("Decisions path images count toward the shared question image limit") {
    import spark.implicits._
    val image = JsObject("type" -> JsString("input_image"), "image_url" -> JsString(pngDataUrl))
    val nullableImage = JsObject(image.fields ++ Map("file_id" -> JsNull, "detail" -> JsNull))
    val gif = Base64.getEncoder.encodeToString(imageBytes("gif"))
    val gifUrl = "data:image/gif;base64," + gif
    for (pathCount <- Seq(1, 2);
         stage <- Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions())) {
      val cases = Seq(128 - pathCount, 129 - pathCount).map { count =>
        val questions = JsArray(
          JsObject("type" -> JsString("predicate"), "name" -> JsString("first"),
            "instructions" -> JsArray(Vector.fill(64)(image))),
          JsObject("type" -> JsString("predicate"), "name" -> JsString("second"),
            "instructions" -> JsArray(Vector.fill(count - 64)(nullableImage))),
          JsObject("type" -> JsString("predicate"), "name" -> JsString("context"),
            "instructions" -> JsString("Inspect all the supplied images.")))
        (count, questions.compactPrint)
      }.toDF("count", "questions")
      Using.resources(
        new AIRecordingServer((_, _) => 200 -> "", encodeResponse = _ => pngBytes),
        new AIRecordingServer((_, _) => 200 -> "", encodeResponse = _ => Base64.getDecoder.decode(gif))) {
        (imageServer, secondServer) =>
        val data = cases.withColumn("second", lit(if (pathCount == 2) secondServer.url + "/other.gif" else " \t "))
          .withColumn("asset", lit(imageServer.url + "/photo.png"))
        Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
          stage.setUrl(server.url + "/v1").setModel("model").setQuestionsCol("questions")
            .setPromptTemplate("Inspect {asset}").setColumnTypes(ListMap("asset" -> "path", "second" -> "path"))
            .setOutputCol("answer").setErrorCol("error")
          val rows = stage.transform(data).collect().sortBy(_.getAs[Int]("count"))
          assert(rows(0).getAs[Row]("error") == null && rows(0).getAs[Row]("answer") != null)
          val error = AIJson.objectString(rows(1).getAs[Row]("error").getString(0))
          assert(error.fields("code") == JsString("invalid_request"))
          assert(error.fields("message").asInstanceOf[JsString].value.contains("at most 128 images"))
          assert(rows(1).getAs[Row]("answer") == null && server.requests.size() == 1)
          assert(imageServer.requests.size() == 1 && secondServer.requests.size() == pathCount - 1)
          val sent = AIJson.objectString(server.requests.peek()._2)
          val questions = sent.fields("questions").asInstanceOf[JsArray].elements
          assert(questions.size == 3)
          assert(questions(0).asJsObject.fields("instructions") == JsArray(Vector.fill(64)(image)))
          assert(questions(1).asJsObject.fields("instructions") == JsArray(Vector.fill(64 - pathCount)(image)))
          assert(questions(2).asJsObject.fields("instructions") == JsString("Inspect all the supplied images."))
          val content = sent.fields("input")
            .asInstanceOf[JsArray].elements.head.asJsObject.fields("content").asInstanceOf[JsArray].elements
          val expectedImages = (if (pathCount == 2) Vector(gifUrl) else Vector.empty) :+ pngDataUrl
          assert(content.tail == expectedImages.map(url =>
            JsObject("type" -> JsString("input_image"), "image_url" -> JsString(url))))
        }
      }
    }
  }

  test("Decisions empty and text-only column mappings preserve native text input") {
    import spark.implicits._
    val data = Seq("Inspect the evidence").toDF("text")
    for (fromColumn <- Seq(false, true); mapping <- Seq(Map.empty[String, String], Map("text" -> "text"));
         stage <- Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions())) {
      Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
        stage.setUrl(server.url + "/v1").setModel("model").setQuestions(NativeRequestFixtures.DecisionQuestions)
          .setColumnTypes(mapping).setOutputCol("answer").setErrorCol("error")
        if (fromColumn) stage.setInputCol("text") else stage.setPromptTemplate("{text}")
        val result = stage.transform(data)
        assert(result.schema == stage.transformSchema(data.schema))
        val row = result.head()
        assert(row.getAs[Row]("error") == null && row.getAs[Row]("answer") != null)
        assert(server.requests.size() == 1)
        assert(AIJson.objectString(server.requests.peek()._2).fields("input") == JsString("Inspect the evidence"))
      }
    }
  }

  test("Decisions rejects unused settings with and without image path columns before HTTP") {
    import spark.implicits._
    val data = Seq("hello").toDF("text")
    val settings: Seq[(String, AIPrompt => AIPrompt)] = Seq(
      "fileSizeLimitMB" -> (_.setFileSizeLimitMB(1.0)),
      "fileSizeLimitMB" -> (_.setColumnTypes(Map("text" -> "text")).setFileSizeLimitMB(1.0)),
      "systemPrompt" -> (_.setColumnTypes(Map("text" -> "path")).setSystemPrompt("Do not ignore this")),
      "dropPrompt" -> (_.setColumnTypes(Map("text" -> "path")).setDropPrompt(false)),
      "dropPrompt" -> (_.setColumnTypes(Map("text" -> "path")).setDropPrompt(true)))
    Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
      Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions()).foreach { base =>
        base.setUrl(server.url + "/v1").setModel("model").setInputCol("text")
          .setQuestions(NativeRequestFixtures.DecisionQuestions)
        settings.foreach { case (name, configure) =>
          val stage = configure(base.copy(ParamMap.empty))
          Seq[() => Any](() => stage.transformSchema(data.schema), () => stage.transform(data)).foreach { call =>
            val message = intercept[IllegalArgumentException](call()).getMessage
            if (name == "fileSizeLimitMB") {
              assert(message == "requirement failed: fileSizeLimitMB requires a 'path' column in columnTypes " +
                "for apiType='decisions'")
            } else assert(message.contains(name))
          }
          assert(server.requests.isEmpty)
        }
      }
    }
  }

  test("Decisions path columns reject conflicting input modes and keep System One text only") {
    import spark.implicits._
    val data = Seq(("Inspect", pngDataUrl)).toDF("text", "asset")
      .withColumn("messages", array(struct(lit("user").as("role"), col("text").as("content"))))
    Using.resource(new AIRecordingServer((_, _) => 200 -> "{}")) { server =>
      Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions()).foreach { stage =>
        stage.setUrl(server.url + "/v1").setModel("model").setInputCol("text")
          .setQuestions(NativeRequestFixtures.DecisionQuestions).setColumnTypes(Map("asset" -> "path"))
        val json = stage.copy(ParamMap.empty).setInputMode("json")
        val messages = stage.copy(ParamMap.empty)
        messages.clear(messages.inputCol)
        messages.setMessagesCol("messages")
        Seq(json -> "Decisions path columns require inputMode='text'",
          messages -> "Non-empty columnTypes cannot be combined with messagesCol").foreach { case (configured, error) =>
          Seq[() => Any](() => configured.transformSchema(data.schema), () => configured.transform(data)).foreach {
            call => assert(intercept[IllegalArgumentException](call()).getMessage.contains(error))
          }
        }
      }
      val system = new AISystemOne().setUrl(server.url).setModel("model")
        .setInputCol("text").setQuestions(NativeRequestFixtures.SystemQuestions)
        .setColumnTypes(Map("asset" -> "path"))
      assert(intercept[IllegalArgumentException](system.transformSchema(data.schema))
        .getMessage.contains("columnTypes"))
      assert(server.requests.isEmpty)
    }
  }

  test("Decisions counts shared and question images together before sending either public stage") {
    import spark.implicits._
    val image = JsObject("type" -> JsString("input_image"), "image_url" -> JsString(validImage))
    val questions = JsArray(JsObject("type" -> JsString("predicate"), "name" -> JsString("matches"),
      "instructions" -> JsArray(image))).compactPrint
    val data = Seq(127, 128).map { count =>
      val evidence = JsArray(JsObject("role" -> JsString("user"),
        "content" -> JsArray(Vector.fill(count)(image))))
      (count, evidence.compactPrint, questions)
    }.toDF("count", "evidence", "questions")
    for (fromColumn <- Seq(false, true);
         stage <- Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions())) {
      Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
        stage.setUrl(server.url + "/v1").setModel("model").setInputCol("evidence").setInputMode("json")
          .setOutputCol("answer").setErrorCol("error")
        if (fromColumn) stage.setQuestionsCol("questions") else stage.setQuestions(questions)
        val rows = stage.transform(data).collect().sortBy(_.getAs[Int]("count"))
        assert(rows(0).getAs[Row]("error") == null && rows(0).getAs[Row]("answer") != null)
        val error = AIJson.objectString(rows(1).getAs[Row]("error").getString(0))
        assert(error.fields("code") == JsString("invalid_request"))
        assert(error.fields("message").asInstanceOf[JsString].value.contains("at most 128 images"))
        assert(rows(1).getAs[Row]("answer") == null && server.requests.size() == 1)
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
