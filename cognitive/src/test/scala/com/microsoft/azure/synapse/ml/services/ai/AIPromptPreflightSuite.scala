// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.core.contracts.HasInputCol
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.{HTTPSchema, URLKey}
import com.microsoft.azure.synapse.ml.param.GlobalParams
import com.microsoft.azure.synapse.ml.services.OpenAIApiVersion
import com.microsoft.azure.synapse.ml.services.aifoundry.AIFoundryModel
import com.microsoft.azure.synapse.ml.services.openai.{OpenAIDecisions, OpenAIDeploymentNameKey,
  OpenAIPrompt, OpenAITemperatureKey}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import spray.json._

import java.nio.file.Files
import java.util.concurrent.{Callable, CountDownLatch, ExecutionException, Executors, TimeUnit}
import scala.collection.JavaConverters._
import scala.util.Using

class AIPromptPreflightSuite extends TestBase {
  private val chatReply =
    """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}]}"""

  private def input: DataFrame = spark.createDataFrame(
    Seq(Row("hello")).asJava, new StructType().add("text", StringType))

  test("rendered template types must be strings before lazy inference") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setErrorCol("error")
      Seq("{array(text)}", "{encode(text, 'UTF-8')}").foreach { template =>
        stage.setPromptTemplate(template)
        val schemaError = intercept[IllegalArgumentException](stage.transformSchema(input.schema))
        val transformError = intercept[IllegalArgumentException](stage.transform(input))
        Seq(schemaError, transformError).foreach { error =>
          assert(error.getMessage.contains("Prompt templates must produce strings") && error.getCause == null)
        }
        assert(server.requests.isEmpty)
      }
      stage.setPromptTemplate("{cast(array(text) as string)}")
      assert(stage.transform(input).head().getAs[Row]("error") == null)
      assert(server.requests.size() == 1 && server.requests.peek()._2.contains("[hello]"))
    }
  }

  test("column type validation cannot be bypassed by setting the Param directly") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setPromptTemplate("{text}")
      Seq("image", "Path", Option.empty[String].orNull).foreach { kind =>
        stage.set(stage.columnTypes, Map("text" -> kind))
        intercept[IllegalArgumentException](stage.transformSchema(input.schema))
        intercept[IllegalArgumentException](stage.transform(input))
        assert(server.requests.isEmpty)
      }
    }
  }

  Seq("chat_completions", "responses", "decisions").foreach { api =>
    test(s"$api explicit messages reject unused construction settings before HTTP") {
      val data = input.selectExpr("text", "array(named_struct('role', 'user', 'content', text)) AS messages")
      val settings: Seq[(String, AIPrompt => AIPrompt)] = if (api == "decisions") {
        Seq("inputMode" -> (_.setInputMode("text")), "inputMode" -> (_.setInputMode("json")))
      } else Seq("systemPrompt" -> (_.setSystemPrompt("Do not ignore this")),
        "columnTypes" -> (_.setColumnTypes(Map("text" -> "path"))),
        "columnTypes" -> (_.setColumnTypes(Map("text" -> "text"))),
        "fileSizeLimitMB" -> (_.setFileSizeLimitMB(1.0)),
        "dropPrompt" -> (_.setDropPrompt(false)), "dropPrompt" -> (_.setDropPrompt(true)))
      val stages = if (api == "decisions") Seq(new AIPrompt().setApiType(api), new OpenAIDecisions())
      else Seq(new AIPrompt().setApiType(api))
      val reply = if (api == "responses") {
        """{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}"""
      } else chatReply
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        stages.foreach { base =>
          base.setUrl(server.url + "/v1").setModel("model").setMessagesCol("messages").setErrorCol("error")
          if (api == "decisions") base.setQuestions(NativeRequestFixtures.DecisionQuestions)
          settings.foreach { case (name, configure) =>
            val stage = configure(base.copy(ParamMap.empty))
            assert(intercept[IllegalArgumentException](stage.transformSchema(data.schema)).getMessage.contains(name))
            assert(intercept[IllegalArgumentException](stage.transform(data)).getMessage.contains(name))
            assert(server.requests.isEmpty)
          }
          if (api != "decisions") {
            assert(base.setColumnTypes(Map.empty[String, String]).transform(data).head().getAs[Row]("error") == null)
            assert(server.requests.size() == 1)
            server.requests.clear()
          }
        }
      }
    }
  }

  Seq("chat_completions", "responses").foreach { api =>
    test(s"$api filename interpolation resolves column aliases without rewriting escaped literals") {
      val caseSensitive = spark.conf.get("spark.sql.caseSensitive")
      val reply = if (api == "chat_completions") chatReply else {
        """{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}"""
      }
      try {
        spark.conf.set("spark.sql.caseSensitive", "false")
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          val file = "data:text/plain;base64,aGVsbG8="
          val data = spark.createDataFrame(Seq(Row(file, file, file, "keep")).asJava,
            new StructType().add("aSsEt", StringType).add("file.name", StringType)
              .add("file`name", StringType).add("tag", StringType))
          val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
            .setPromptTemplate("Inspect {asset} / {`FILE.NAME`} / {`file``NAME`}; " +
              "literal {{ASSET}}; SQL {'asset'}; expression {upper(tag)}")
            .setColumnTypes(Map("ASSET" -> "path", "FiLe.NaMe" -> "path", "FILE`NAME" -> "path"))
            .setOutputCol("answer").setErrorCol("error")
          val result = stage.transform(data)
          assert(result.schema == stage.transformSchema(data.schema))
          val row = result.head()
          assert(row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null)
          assert(server.requests.peek()._2.contains(
            "Inspect attachment.txt / attachment.txt / attachment.txt; literal {ASSET}; SQL asset; expression KEEP"))
          assert(row.getAs[String]("aSsEt") == file && row.getAs[String]("file`name") == file)
          spark.conf.set("spark.sql.caseSensitive", "true")
          intercept[IllegalArgumentException](stage.transformSchema(data.schema))
          assert(server.requests.size() == 1)
        }
      } finally spark.conf.set("spark.sql.caseSensitive", caseSensitive)
    }
  }

  test("saved stages preserve explicitly disabled postprocessing") {
    val session = spark
    Seq(new AIPrompt(), new AISystemOne(), new OpenAIDecisions()).foreach { stage =>
      val root = Files.createTempDirectory("aiprompt-postprocessing")
      try {
        stage.setPostProcessing("")
        stage.write.session(session).save(root.resolve("explicit").toString)
        val loaded = AIPrompt.read.session(session).load(root.resolve("explicit").toString)
        Seq(stage, loaded, loaded.copy(org.apache.spark.ml.param.ParamMap.empty)).foreach { candidate =>
          intercept[IllegalArgumentException](candidate.setPostProcessingOptions(Map("delimiter" -> ";")))
        }
        loaded.clear(loaded.postProcessing)
        loaded.setPostProcessingOptions(Map("delimiter" -> ";"))
        assert(loaded.getPostProcessing == "csv")
        val implicitStage = new AIPrompt()
        implicitStage.write.session(session).save(root.resolve("implicit").toString)
        val implicitLoaded = AIPrompt.read.session(session).load(root.resolve("implicit").toString)
        assert(!implicitLoaded.isSet(implicitLoaded.postProcessing))
        implicitLoaded.setPostProcessingOptions(Map("delimiter" -> ";"))
        assert(implicitLoaded.getPostProcessing == "csv")
      } finally org.apache.commons.io.FileUtils.deleteDirectory(root.toFile)
    }
  }

  test("Responses incomplete details preserve partial text and diagnostics without a status") {
    Seq("", """"status":"completed",""").foreach { status =>
      val reply = s"""{$status"incomplete_details":{"reason":"max_output_tokens"},
        |"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"partial"}]}],
        |"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}}""".stripMargin
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val stage = new AIPrompt().setApiType("responses").setUrl(server.url + "/v1").setModel("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error").setRawResponseCol("raw")
          .setUsageCol("usage")
        val row = stage.transform(input).head()
        assert(row.getAs[String]("answer") == "partial")
        assert(Option(row.getAs[Row]("error")).exists(_.getString(0).contains("incomplete")))
        assert(row.getAs[Row]("usage").getLong(2) == 3L && row.getAs[String]("raw") == reply)
      }
    }
  }

  test("Foundry routes use the same implicit model binding as the request body") {
    val stage = new AIPrompt().setUrl("https://example.services.ai.azure.com").setInputCol("text")
    val previousModel = GlobalParams.getGlobalParam(AIFoundryModel)
    val previousDeployment = GlobalParams.getGlobalParam(OpenAIDeploymentNameKey)
    def route(candidate: AIPrompt, expectedPath: String, expectedModel: String): Unit = {
      candidate.transformSchema(input.schema)
      val adapter = new AIRequestAdapter(candidate.effectiveCopy(), input.schema)
      val (url, body) = adapter.request(Row("hello"), None,
        Some(Seq(new org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema(
          Array[Any]("user", "hello"), new StructType().add("role", StringType).add("content", StringType)))))
      assert(new java.net.URI(url).getPath == expectedPath)
      assert(AIJson.objectString(body).fields("model") == JsString(expectedModel))
    }
    try {
      GlobalParams.setGlobalParam(AIFoundryModel, Left("global-model"))
      GlobalParams.setGlobalParam(OpenAIDeploymentNameKey, Left("global-deployment"))
      route(stage, "/openai/deployments/global-deployment/chat/completions", "global-deployment")
      route(stage.copy(org.apache.spark.ml.param.ParamMap.empty).setModel("explicit-model"),
        "/models/chat/completions", "explicit-model")
      route(stage.copy(org.apache.spark.ml.param.ParamMap.empty).setDeploymentName("explicit-deployment"),
        "/openai/deployments/explicit-deployment/chat/completions", "explicit-deployment")
    } finally {
      previousModel match {
        case Some(value) => GlobalParams.setGlobalParam(AIFoundryModel, value)
        case None => GlobalParams.resetGlobalParam(AIFoundryModel)
      }
      previousDeployment match {
        case Some(value) => GlobalParams.setGlobalParam(OpenAIDeploymentNameKey, value)
        case None => GlobalParams.resetGlobalParam(OpenAIDeploymentNameKey)
      }
    }
  }

  Seq("systemone", "decisions", "embeddings").foreach { api =>
    test(s"$api ignores inactive global temperature in schema and requests") {
      val previous = GlobalParams.getGlobalParam(OpenAITemperatureKey)
      val reply = api match {
        case "systemone" => NativeRequestFixtures.SystemResponse
        case "decisions" => NativeRequestFixtures.DecisionResponse
        case _ => """{"data":[{"index":0,"embedding":[0.25,0.5]}],"model":"model"}"""
      }
      try {
        GlobalParams.resetGlobalParam(OpenAITemperatureKey)
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          val stage = (api match {
            case "systemone" => new AISystemOne().setStateCol("text")
              .setQuestions(NativeRequestFixtures.SystemQuestions)
            case "decisions" => new OpenAIDecisions().setInputCol("text")
              .setQuestions(NativeRequestFixtures.DecisionQuestions)
            case _ => new AIPrompt().setApiType(api).setInputCol("text")
          }).setUrl(if (api == "systemone") server.url else server.url + "/v1")
            .setModel("model").setOutputCol("answer").setErrorCol("error")
          val original = stage.effectiveCopy()
          val expectedSchema = stage.transformSchema(input.schema)
          val defaults = Seq[Either[Double, String]](Right("absent-global-column"), Right("text"), Left(0.25))
          defaults.foreach { default =>
            GlobalParams.setGlobalParam(OpenAITemperatureKey, default)
            assert(stage.transformSchema(input.schema) == expectedSchema)
            val row = stage.transform(input).head()
            assert(row.getAs[Row]("error") == null && !row.isNullAt(row.fieldIndex("answer")))
            assert(!AIJson.objectString(server.requests.asScala.last._2).fields.contains("temperature"))
            assert(GlobalParams.getGlobalParam(OpenAITemperatureKey).contains(default))
          }
          val requestsBefore = server.requests.size()
          Seq(stage.copy(ParamMap.empty).setTemperature(0.25),
            stage.copy(ParamMap.empty).setTemperatureCol("text")).foreach { explicit =>
            intercept[IllegalArgumentException](explicit.transformSchema(input.schema))
          }
          assert(server.requests.size() == requestsBefore)
        }
      } finally {
        previous match {
          case Some(value) => GlobalParams.setGlobalParam(OpenAITemperatureKey, value)
          case None => GlobalParams.resetGlobalParam(OpenAITemperatureKey)
        }
      }
    }
  }

  test("native stages require explicit models despite global chat and Foundry defaults") {
    val previousModel = GlobalParams.getGlobalParam(AIFoundryModel)
    val previousDeployment = GlobalParams.getGlobalParam(OpenAIDeploymentNameKey)
    try {
      Seq("systemone", "decisions").foreach { api =>
        val questions = if (api == "systemone") NativeRequestFixtures.SystemQuestions
          else NativeRequestFixtures.DecisionQuestions
        val direct = if (api == "systemone") new AISystemOne() else new OpenAIDecisions()
        Seq(direct, new AIPrompt().setApiType(api)).foreach { stage =>
          stage.setUrl(if (api == "systemone") "https://example.services.ai.azure.com"
            else "https://example.invalid/v1").setInputCol("text").setQuestions(questions)
          GlobalParams.resetGlobalParam(AIFoundryModel)
          GlobalParams.resetGlobalParam(OpenAIDeploymentNameKey)
          val explicit = stage.copy(ParamMap.empty).setModel("native-model")
          val schema = explicit.transformSchema(input.schema)
          Seq[Either[String, String]](Left("chat-model"), Right("absent-model-column")).foreach { value =>
            GlobalParams.setGlobalParam(AIFoundryModel, value)
            GlobalParams.setGlobalParam(OpenAIDeploymentNameKey, value)
            val error = intercept[IllegalArgumentException](stage.transformSchema(input.schema))
            assert(error.getMessage.contains("Set deploymentName/model"))
            assert(explicit.transformSchema(input.schema) == schema)
            assert(GlobalParams.getGlobalParam(AIFoundryModel).contains(value))
          }
        }
      }
    } finally {
      previousModel match {
        case Some(value) => GlobalParams.setGlobalParam(AIFoundryModel, value)
        case None => GlobalParams.resetGlobalParam(AIFoundryModel)
      }
      previousDeployment match {
        case Some(value) => GlobalParams.setGlobalParam(OpenAIDeploymentNameKey, value)
        case None => GlobalParams.resetGlobalParam(OpenAIDeploymentNameKey)
      }
    }
  }

  test("explicit message rows keep their own schema when intermediate encoders are materialized") {
    val session = spark.newSession()
    session.conf.set("spark.sql.optimizer.excludedRules",
      "org.apache.spark.sql.catalyst.optimizer.EliminateSerialization")
    Seq(("chat_completions", false), ("chat_completions", true), ("responses", false)).foreach {
      case (api, withName) =>
        val base = new StructType().add("role", StringType).add("content", StringType)
        val message = if (withName) base.add("name", StringType) else base
        val schema = new StructType().add("messages", ArrayType(message))
        val value = if (withName) Row("user", "hello", "speaker") else Row("user", "hello")
        val data = session.createDataFrame(Seq(Row(Seq(value))).asJava, schema)
        val reply = if (api == "chat_completions") chatReply else {
          """{"status":"completed","output":[{"type":"message","role":"assistant",
            |"content":[{"type":"output_text","text":"answer"}]}]}""".stripMargin
        }
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
            .setMessagesCol("messages").setOutputCol("answer").setErrorCol("error")
          val result = stage.transform(data)
          assert(result.schema == stage.transformSchema(schema))
          val row = result.head()
          assert(row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null)
          assert(row.getSeq[Row](row.fieldIndex("messages")) == Seq(value))
          assert(server.requests.size() == 1)
          assert(server.requests.peek()._2.contains("hello"))
        }
    }
  }

  Seq("systemone", "decisions", "chat_completions", "responses", "embeddings", "custom", "global-v1")
    .foreach { route =>
      test(s"global API versions are inactive for $route in schema and requests") {
        val previous = GlobalParams.getGlobalParam(OpenAIApiVersion)
        val previousUrl = GlobalParams.getGlobalParam(URLKey)
        val data = input.withColumn("wrongVersion", org.apache.spark.sql.functions.lit(1))
        val api = if (Set("custom", "global-v1")(route)) "chat_completions" else route
        val reply = api match {
          case "systemone" => NativeRequestFixtures.SystemResponse
          case "decisions" => NativeRequestFixtures.DecisionResponse
          case "embeddings" => """{"data":[{"index":0,"embedding":[0.25,0.5]}],"model":"model"}"""
          case "responses" =>
            """{"output":[{"type":"message","role":"assistant",
              |"content":[{"type":"output_text","text":"answer"}]}]}""".stripMargin
          case _ => chatReply
        }
        try {
          GlobalParams.resetGlobalParam(OpenAIApiVersion)
          Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
            val stage = (api match {
              case "systemone" => new AISystemOne().setStateCol("text")
                .setQuestions(NativeRequestFixtures.SystemQuestions)
              case "decisions" => new OpenAIDecisions().setInputCol("text")
                .setQuestions(NativeRequestFixtures.DecisionQuestions)
              case _ => new AIPrompt().setApiType(api).setInputCol("text")
            }).setModel("model").setOutputCol("answer").setErrorCol("error")
            route match {
              case "systemone" => stage.setUrl(server.url + "/providers/microsoft/v1/systemone")
              case "custom" => stage.setCustomUrlRoot(server.url + "/chat/completions?api-version=owned")
              case "global-v1" => GlobalParams.setGlobalParam(URLKey, server.url + "/v1/chat/completions")
              case _ => stage.setUrl(server.url + "/v1")
            }
            val expectedSchema = stage.transformSchema(data.schema)
            val original = stage.transform(data).head()
            assert(original.getAs[Row]("error") == null && !original.isNullAt(original.fieldIndex("answer")))
            val expectedRequest = server.requests.asScala.last
            val defaults = Seq[Either[String, String]](
              Right("absentVersion"), Right("wrongVersion"), Left("dated-one"), Left("dated-two"))
            defaults.foreach { default =>
              GlobalParams.setGlobalParam(OpenAIApiVersion, default)
              assert(stage.transformSchema(data.schema) == expectedSchema)
              val row = stage.transform(data).head()
              assert(row.getAs[Row]("error") == null && !row.isNullAt(row.fieldIndex("answer")))
              assert(server.requests.asScala.last == expectedRequest)
              assert(GlobalParams.getGlobalParam(OpenAIApiVersion).contains(default))
            }
            Seq(stage.copy(ParamMap.empty).setApiVersion("explicit"),
              stage.copy(ParamMap.empty).setApiVersionCol("text")).foreach { explicit =>
              val error = intercept[IllegalArgumentException](explicit.transformSchema(data.schema))
              assert(error.getMessage.contains("apiVersion is not supported"))
              if (route == "custom") assert(error.getMessage.contains("customUrlRoot"))
              val transformError = intercept[IllegalArgumentException](explicit.transform(data))
              assert(transformError.getMessage == error.getMessage)
            }
            assert(server.requests.size() == 5)
          }
        } finally {
          previous match {
            case Some(value) => GlobalParams.setGlobalParam(OpenAIApiVersion, value)
            case None => GlobalParams.resetGlobalParam(OpenAIApiVersion)
          }
          previousUrl match {
            case Some(value) => GlobalParams.setGlobalParam(URLKey, value)
            case None => GlobalParams.resetGlobalParam(URLKey)
          }
        }
      }
    }

  test("global API versions remain active for dated routes") {
    val previous = GlobalParams.getGlobalParam(OpenAIApiVersion)
    val data = input.withColumn("version", org.apache.spark.sql.functions.lit("dated-column"))
    try {
      Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
        val stage = new AIPrompt().setUrl(server.url).setModel("model").setInputCol("text")
          .setOutputCol("answer").setErrorCol("error")
        GlobalParams.setGlobalParam(OpenAIApiVersion, Right("absentVersion"))
        intercept[IllegalArgumentException](stage.transformSchema(data.schema))
        GlobalParams.setGlobalParam(OpenAIApiVersion, Right("version"))
        val columnVersion = stage.transform(data).head()
        assert(columnVersion.getAs[String]("answer") == "answer" && columnVersion.getAs[Row]("error") == null)
        assert(server.requests.asScala.last._1.endsWith("?api-version=dated-column"))
        GlobalParams.setGlobalParam(OpenAIApiVersion, Left("dated-one"))
        val scalarVersion = stage.transform(data).head()
        assert(scalarVersion.getAs[String]("answer") == "answer" && scalarVersion.getAs[Row]("error") == null)
        assert(server.requests.asScala.last._1.endsWith("?api-version=dated-one"))
        GlobalParams.setGlobalParam(OpenAIApiVersion, Left("dated-two"))
        assert(stage.transform(data).head().getAs[String]("answer") == "answer")
        assert(server.requests.asScala.last._1.endsWith("?api-version=dated-two"))
      }
    } finally {
      previous match {
        case Some(value) => GlobalParams.setGlobalParam(OpenAIApiVersion, value)
        case None => GlobalParams.resetGlobalParam(OpenAIApiVersion)
      }
    }
  }

  test("public input binding follows the repository contract without introducing a default") {
    Seq(new AIPrompt(), new AISystemOne(), new OpenAIDecisions()).foreach { stage =>
      assert(classOf[HasInputCol].isInstance(stage))
      assert(stage.getDefault(stage.inputCol).isEmpty)
      assert(stage.setInputCol("text").getInputCol == "text")
    }
  }

  test("API selector documentation matches the accepted protocol domain on every public stage") {
    val protocols = Seq("chat_completions", "responses", "systemone", "decisions", "embeddings")
    Seq(new OpenAIPrompt(), new AIPrompt(), new AISystemOne(), new OpenAIDecisions()).foreach { stage =>
      protocols.foreach { protocol =>
        assert(stage.apiType.doc.contains(s"'$protocol'") == stage.apiType.isValid(protocol))
      }
    }
    assert(new OpenAIPrompt().apiType.doc ==
      "The OpenAI API type to use: 'chat_completions' or 'responses'")
  }

  test("Python template hooks validate JSON replacements and render class-specific provenance") {
    val setter = "value = self._service_param_value_to_java(value)"
    Seq("extraBody", "questions").foreach { name =>
      Seq("", setter + "\n" + setter).foreach { source =>
        assert(intercept[IllegalArgumentException](AIPromptPython.paramSetter(source, name))
          .getMessage.contains(s"$name setter template"))
      }
      assert(AIPromptPython.paramSetter(setter, name).contains("self._ai_json"))
    }
    Seq(new OpenAIPrompt(), new AIPrompt(), new AISystemOne(), new OpenAIDecisions()).foreach { stage =>
      val methods = stage.pyAdditionalMethods
      val explicit = if (stage.isInstanceOf[AIPrompt]) "self.isSet(self.postProcessing)"
        else "self._post_processing_explicitly_set"
      assert(methods.contains(s"def _post_processing_is_explicit(self):\n    return $explicit"))
      Seq("clear(param)", "copy(extra)").foreach { call =>
        assert(methods.contains(s"super(${stage.getClass.getSimpleName}, self).$call"))
      }
    }
  }

  test("struct JSON postprocessing diagnoses malformed and incompatible fields without rejecting null objects") {
    val cases = Seq(
      ("not json", true), ("""{"value":"wrong"}""", true),
      ("""{"value":1},"extra":2""", true), ("""{"value":1} {"value":2}""", true),
      ("""{"value":1,"nested":{"count":"wrong"}}""", true),
      ("""{"value":null,"nested":null}""", false), ("{}", false),
      ("```json\n{\"value\":2}\n```", false),
      ("""{"value":3,"__synapseml_corrupt_record":"user field"}""", false))
    Seq("chat_completions", "responses").foreach { api =>
      cases.foreach { case (text, invalid) =>
        val quoted = JsString(text).compactPrint
        val reply = if (api == "chat_completions") {
          s"""{"choices":[{"index":0,"message":{"role":"assistant","content":$quoted},"finish_reason":"stop"}]}"""
        } else {
          s"""{"status":"completed","output":[{"type":"message","role":"assistant",
             |"content":[{"type":"output_text","text":$quoted}]}]}""".stripMargin
        }
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
            .setInputCol("text").setOutputCol("answer").setErrorCol("error").setRawResponseCol("raw")
            .setPostProcessingOptions(Map("jsonSchema" ->
              "value INT, nested STRUCT<count: INT>, __synapseml_corrupt_record STRING"))
          val result = stage.transform(input)
          assert(result.schema == stage.transformSchema(input.schema))
          val row = result.head()
          assert(Option(row.getAs[Row]("error")).exists(_.getString(0).contains("parse_error")) == invalid,
            s"$api postprocessing incorrectly classified $text")
          assert(row.getAs[String]("raw") == reply)
          if (text.contains("user field")) {
            assert(row.getAs[Row]("answer").getAs[String]("__synapseml_corrupt_record") == "user field")
          }
          assert(server.requests.size() == 1)
        }
      }
    }
  }

  test("sub-nanosecond concurrent timeouts fail preflight instead of becoming immediate") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
      Seq(Double.MinPositiveValue, 0.5e-9, 0.999e-9).foreach { timeout =>
        stage.setConcurrentTimeout(timeout)
        val error = intercept[IllegalArgumentException](stage.transformSchema(input.schema))
        assert(error.getMessage.contains("concurrentTimeout"))
        intercept[IllegalArgumentException](stage.transform(input))
      }
      stage.setConcurrentTimeout(1e-9).transformSchema(input.schema)
      assert(server.requests.isEmpty)
    }
  }

  test("sub-millisecond HTTP timeouts fail preflight instead of becoming infinite") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
      Seq(Double.MinPositiveValue, 0.0005, 0.000999).foreach { timeout =>
        val error = intercept[IllegalArgumentException](stage.setTimeout(timeout).transformSchema(input.schema))
        assert(error.getMessage.contains("timeout"))
        intercept[IllegalArgumentException](stage.transform(input))
      }
      stage.setTimeout(0.001).transformSchema(input.schema)
      assert(server.requests.isEmpty)
    }
  }

  test("native scalar null questions fail preflight while null question cells remain row errors") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> "{}")) { server =>
      val data = input.withColumn("questions", org.apache.spark.sql.functions.lit(
        Option.empty[String].orNull).cast(StringType))
      Seq(new AISystemOne(), new AIPrompt().setApiType("systemone"),
        new OpenAIDecisions(), new AIPrompt().setApiType("decisions")).foreach { stage =>
        stage.setUrl(if (stage.getApiType == "systemone") server.url else server.url + "/v1")
          .setModel("model").setInputCol("text").setQuestions("null").setErrorCol("error")
        intercept[IllegalArgumentException](stage.transformSchema(data.schema))
        intercept[IllegalArgumentException](stage.transform(data))
        val row = stage.setQuestionsCol("questions").transform(data).head()
        assert(row.getAs[Row]("error").getString(0).contains("missing_input"))
      }
      assert(server.requests.isEmpty)
    }
  }

  test("struct JSON postprocessing preserves an existing incomplete-response diagnostic") {
    val reply = chatReply.replace("\"answer\"", "\"not json\"").replace("\"stop\"", "\"length\"")
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
        .setOutputCol("answer").setErrorCol("error").setRawResponseCol("raw")
        .setPostProcessingOptions(Map("jsonSchema" -> "value INT"))
      val row = stage.transform(input).head()
      assert(row.getAs[Row]("error").getString(0).contains("incomplete"))
      assert(row.getAs[String]("raw") == reply)
    }
  }

  test("container JSON postprocessing diagnoses nested conversion failures but preserves valid nulls") {
    val cases = Seq(
      ("ARRAY<STRUCT<value: INT>>", """[{"value":"wrong"}]""", true),
      ("ARRAY<STRUCT<value: INT>>", """{"value":"wrong"}""", true),
      ("ARRAY<STRUCT<value: INT>>", """{"value":1}""", false),
      ("ARRAY<STRUCT<value: INT>>", """[{"value":null},null,{}]""", false),
      ("ARRAY<STRUCT<value: INT>>", "```json\n[{\"value\":2}]\n```", false),
      ("MAP<STRING, STRUCT<value: INT>>", """{"entry":{"value":"wrong"}}""", true),
      ("MAP<STRING, STRUCT<value: INT>>", """{"entry":{"value":null},"empty":null}""", false),
      ("MAP<STRING, ARRAY<INT>>", """{"entry":[1,"wrong"]}""", true),
      ("MAP<STRING, ARRAY<INT>>", """{"entry":[1,null]}""", false))
    Seq("chat_completions", "responses").foreach { api =>
      cases.foreach { case (schema, text, invalid) =>
        val quoted = JsString(text).compactPrint
        val reply = if (api == "chat_completions") {
          s"""{"choices":[{"index":0,"message":{"role":"assistant","content":$quoted},"finish_reason":"stop"}]}"""
        } else {
          s"""{"status":"completed","output":[{"type":"message","role":"assistant",
             |"content":[{"type":"output_text","text":$quoted}]}]}""".stripMargin
        }
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
            .setInputCol("text").setOutputCol("answer").setErrorCol("error").setRawResponseCol("raw")
            .setPostProcessingOptions(Map("jsonSchema" -> schema))
          val result = stage.transform(input)
          assert(result.schema == stage.transformSchema(input.schema))
          val row = result.head()
          assert(Option(row.getAs[Row]("error")).exists(_.getString(0).contains("parse_error")) == invalid,
            s"$api postprocessing incorrectly classified $text as $schema")
          if (!invalid) assert(!row.isNullAt(row.fieldIndex("answer")))
          if (text == """{"value":1}""") assert(row.getAs[Seq[Row]]("answer") == Seq(Row(1)))
          assert(row.getAs[String]("raw") == reply && server.requests.size() == 1)
        }
      }
    }
  }

  test("complete deployment URLs reject literal and encoded dot segments for scalar and column models") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      Seq(".", "..", "%2e", "%2E%2e", "..%2fother", "foo%2F..%2Fadmin",
        "foo%5c..%5cadmin", "%252e%252e%252fadmin").foreach { segment =>
        Seq("chat_completions", "embeddings").foreach { api =>
          Seq(false, true).foreach { custom =>
            val route = if (api == "embeddings") "embeddings" else "chat/completions"
            val address = server.url + s"/openai/deployments/$segment/$route"
            val stage = new AIPrompt().setApiType(api).setInputCol("text")
              .setDeploymentName(java.net.URLDecoder.decode(segment, "UTF-8"))
            if (custom) stage.setCustomUrlRoot(address) else stage.setUrl(address)
            Seq(stage, stage.copy(ParamMap.empty).setDeploymentNameCol("text")).foreach { candidate =>
              val error = intercept[IllegalArgumentException](candidate.transformSchema(input.schema))
              assert(error.getMessage.contains("dot segment"))
            }
          }
        }
      }
      Seq(".", "..").foreach { model =>
        new AIPrompt().setUrl(server.url + "/v1").setModel(model).setInputCol("text").transform(input).collect()
        assert(AIJson.objectString(server.requests.asScala.last._2).fields("model") == JsString(model))
      }
      assert(server.requests.size() == 2)
    }
  }

  test("complete URL overrides enforce deployment identity and models-route selection before HTTP") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val address = server.url + "/openai/deployments/model/chat/completions/?version=custom"
      def stage: AIPrompt = new AIPrompt().setCustomUrlRoot(address).setDeploymentName("model")
        .setInputCol("text").setOutputCol("answer").setErrorCol("error")
      intercept[IllegalArgumentException](stage.setDeploymentName("other").transformSchema(input.schema))
      val row = stage.setDeploymentNameCol("text").transform(input).head()
      assert(row.getAs[Row]("error").getString(0).contains("invalid_request"))
      val models = server.url + "/models/chat/completions?version=custom"
      intercept[IllegalArgumentException](stage.setCustomUrlRoot(models).transformSchema(input.schema))
      assert(server.requests.isEmpty)
      stage.transform(input).collect()
      assert(server.requests.peek()._1 == "/openai/deployments/model/chat/completions/?version=custom")
      val valid = stage.setCustomUrlRoot(models)
      valid.clear(valid.deploymentName)
      valid.setModel("model").transform(input).collect()
      assert(server.requests.asScala.last._1 == "/models/chat/completions?version=custom")
    }
  }

  test("complete URL overrides preserve trailing path slashes and query bytes") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      Seq("/proxy/", "/proxy?value=a/", "/proxy/?value=a%2Fb%2F&case=A+B").foreach { suffix =>
        new AIPrompt().setCustomUrlRoot(server.url + suffix).setModel("model").setInputCol("text")
          .transform(input).collect()
        assert(server.requests.asScala.last._1 == suffix)
      }
    }
  }

  test("invalid concurrent timeout defaults fail before HTTP") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      Seq(0.0, -1.0, 1e-10, Double.NaN, Double.PositiveInfinity).foreach { timeout =>
          val stage = new TestableAIDefaultTimeoutPrompt("invalid-default-timeout").setConcurrentTimeoutDefault(timeout)
            .setUrl(server.url + "/v1").setModel("model").setInputCol("text")
          assert(intercept[IllegalArgumentException](stage.transformSchema(input.schema))
            .getMessage.contains("concurrentTimeout"))
          assert(intercept[IllegalArgumentException](stage.transform(input)).getMessage.contains("concurrentTimeout"))
          assert(server.requests.isEmpty)
      }
    }
  }

  test("Spark job cancellation stops bounded retry workers at both concurrency settings") {
    Seq((1, true), (2, true), (2, false)).foreach { case (concurrency, interrupt) =>
      val received = new CountDownLatch(concurrency)
      Using.resource(new AIRecordingServer((number, _) => {
        received.countDown()
        if (number <= concurrency) 429 -> "{}" else 200 -> chatReply
      }, retryAfter = "3")) { server =>
        val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
          .setConcurrency(concurrency).setTimeout(5)
        val data = input.union(input).coalesce(1)
        val context = spark.sparkContext
        val group = java.util.UUID.randomUUID().toString
        val executor = Executors.newSingleThreadExecutor()
        val job = executor.submit(new Callable[Unit] {
          override def call(): Unit = {
            context.setJobGroup(group, "AIPrompt cancellation regression", interruptOnCancel = interrupt)
            try stage.transform(data).collect() finally context.clearJobGroup()
          }
        })
        try {
          assert(received.await(30, TimeUnit.SECONDS), "The requests never reached retry backoff")
          context.cancelJobGroup(group)
          intercept[ExecutionException](job.get(15, TimeUnit.SECONDS))
          Thread.sleep(4000)
          assert(server.requests.size() == concurrency, "Canceled workers issued another paid request")
        } finally {
          context.cancelJobGroup(group)
          job.cancel(true)
          executor.shutdownNow()
          assert(executor.awaitTermination(15, TimeUnit.SECONDS))
        }
      }
    }
  }

  test("timeout and early consumer completion abort in-flight HTTP without leaving retry workers") {
    Seq(false, true).foreach { earlyCompletion =>
      val blocked = new CountDownLatch(1)
      val release = new CountDownLatch(1)
      Using.resource(new AIRecordingServer((_, body) => {
        if (body.contains("blocked") || !earlyCompletion) {
          blocked.countDown()
          release.await(15, TimeUnit.SECONDS)
        } else {
          assert(blocked.await(15, TimeUnit.SECONDS))
        }
        200 -> chatReply
      })) { server =>
        try {
          val data = spark.createDataFrame(Seq(Row("ready"), Row("blocked")).asJava,
            new StructType().add("text", StringType)).coalesce(1)
          val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
            .setConcurrency(2).setConcurrentTimeout(2).setTimeout(60).setOutputCol("answer")
          val result = stage.transform(data)
          if (earlyCompletion) assert(result.limit(1).head().getAs[String]("answer") == "answer")
          else intercept[org.apache.spark.SparkException](result.collect())
          assert(blocked.getCount == 0, "The regression did not reach an in-flight HTTP request")
          assert(!Thread.getAllStackTraces.keySet().asScala.exists(thread =>
            thread.isAlive && thread.getName.startsWith("synapseml-bounded-http-")))
        } finally release.countDown()
      }
    }
  }

  test("implicit Fabric requests preserve workload classification with explicit credentials and header precedence") {
    val previous = GlobalParams.getGlobalParam(URLKey)
    val root = "https://fabric.example.invalid/cognitive/openai"
    val reply = chatReply
    try {
      GlobalParams.setGlobalParam(URLKey, root)
      Seq(false, true).foreach { explicitKey =>
        val stage = new TestableAIFabricAuthPrompt("fabric-classification").setDeploymentName("model")
          .setInputCol("text").setOutputCol("answer")
          .setCustomHeaders(Map("x-taxonomy-traffictype" -> "Interactive", "X-Llm-Service-Tier" -> "standard",
            "X-Taxonomy-ExtendedProperties" -> "caller", "x-caller" -> "preserved"))
        if (explicitKey) stage.setSubscriptionKey("synthetic-explicit-key")
        stage.setHandler { (_, request) =>
          val headers = request.headers.map(h => h.name.toLowerCase(java.util.Locale.ROOT) -> h.value).toMap
          assert(headers("x-taxonomy-traffictype") == "Background")
          assert(headers("x-llm-service-tier") == "flex")
          val properties = headers("x-taxonomy-extendedproperties").parseJson.asJsObject.fields
          assert(properties("feature") == JsString("synapseml") && properties.contains("runtime"))
          assert(headers("x-caller") == "preserved")
          HTTPSchema.stringToResponse(reply, 200, "OK")
        }
        assert(stage.transform(input).head().getAs[String]("answer") == "answer")
      }
      Seq("explicit", "other").foreach { destination =>
        val stage = new TestableAIFabricAuthPrompt("fabric-not-classified").setDeploymentName("model")
          .setInputCol("text").setOutputCol("answer")
        if (destination == "explicit") stage.setUrl(root)
        else GlobalParams.setGlobalParam(URLKey, "https://other.example.invalid/v1")
        stage.setHandler { (_, request) =>
          assert(!request.headers.exists(_.name.equalsIgnoreCase("X-Taxonomy-TrafficType")))
          assert(!request.headers.exists(_.name.equalsIgnoreCase("X-Llm-Service-Tier")))
          HTTPSchema.stringToResponse(reply, 200, "OK")
        }
        assert(stage.transform(input).head().getAs[String]("answer") == "answer")
      }
    } finally {
      previous match {
        case Some(value) => GlobalParams.setGlobalParam(URLKey, value)
        case None => GlobalParams.resetGlobalParam(URLKey)
      }
    }
  }

  test("implicit Fabric scalar model preflight rejects unavailable models only during transform") {
    val previous = GlobalParams.getGlobalParam(URLKey)
    try {
      GlobalParams.setGlobalParam(URLKey, "https://fabric.example.invalid/cognitive/openai")
      val stage = new TestableAIFabricAuthPrompt("fabric-preflight").setModel("blocked-model").setInputCol("text")
      stage.transformSchema(input.schema)
      val error = intercept[IllegalArgumentException](stage.transform(input))
      assert(error.getMessage.contains("Synthetic Fabric model is unavailable"))
      val reply = chatReply
      val column = new TestableAIFabricAuthPrompt("fabric-column-model").setModelCol("text")
        .setInputCol("text").setOutputCol("answer")
      column.setHandler((_, _) => HTTPSchema.stringToResponse(reply, 200, "OK"))
      assert(column.transform(input).head().getAs[String]("answer") == "answer")
    } finally {
      previous match {
        case Some(value) => GlobalParams.setGlobalParam(URLKey, value)
        case None => GlobalParams.resetGlobalParam(URLKey)
      }
    }
  }
}
