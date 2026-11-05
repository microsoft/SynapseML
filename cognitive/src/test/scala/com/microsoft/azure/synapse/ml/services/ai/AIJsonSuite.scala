// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.codegen.CodegenConfig
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.param.AnyJsonFormat
import com.microsoft.azure.synapse.ml.services.openai.{JsonParser, OpenAIDecisions, OutputParser}
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.lit
import org.scalatest.concurrent.TimeLimits
import org.scalatest.time.{Seconds, Span}
import spray.json.{JsArray, JsBoolean, JsNull, JsNumber, JsObject, JsString, enrichString}

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.{ArrayList, LinkedHashMap}
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.util.Using

private[ai] class TestableAIDefaultJsonPrompt(override val uid: String) extends AIPrompt(uid) {
  setDefault(extraBody -> Left(Map("value" -> BigDecimal("0.12345678901234567890123456789"))))
}

private[ai] class TestableAIDefaultRawPrompt(override val uid: String) extends AIPrompt(uid) {
  setDefault(rawResponseCol -> "raw")
}

private[ai] class TestableAIDateParserPrompt(override val uid: String) extends AIPrompt(uid) {
  override private[services] def getParser: OutputParser =
    new JsonParser("value DATE", Map("dateFormat" -> "dd/MM/yyyy"))
}

class AIJsonSuite extends TestBase with TimeLimits {
  private val missing: Null = None.orNull
  private val chatReply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""

  test("ordered JSON retains insertion order through request builder map operations") {
    val names = Vector("zebra", "alpha", "middle", "beta", "omega", "first")
    val original = AIJson.orderedMap(names.zipWithIndex)
    val updated = original.updated("middle", 20) + ("last" -> 6)
    assert(updated.keys.toVector == names :+ "last")
    val merged = updated ++ Seq("alpha" -> 10, "tail" -> 7)
    assert(merged.keys.toVector == names ++ Vector("last", "tail"))
    val removed = (merged - "zebra") -- Seq("beta", "omega")
    assert(removed.keys.toVector == Vector("alpha", "middle", "first", "last", "tail"))
    val filtered = removed.filter(_._2 > 5).filterNot(_._1 == "first")
    assert(filtered.keys.toVector == Vector("alpha", "middle", "last", "tail"))
    assert(filtered("alpha") == 10 && filtered("middle") == 20)
    assert(original.keys.toVector == names)
  }

  Seq("chat_completions", "responses").foreach { api =>
    Seq(false, true).foreach { columnBinding =>
      test(s"$api tool requests require explicitly projected raw responses with column binding $columnBinding") {
        val reply = if (api == "chat_completions") {
          """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer",
            |"tool_calls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{}"}}]}}]}"""
            .stripMargin
        } else {
          """{"status":"completed","output":[{"type":"message","role":"assistant",
            |"content":[{"type":"output_text","text":"answer"}]},
            |{"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{}"}]}""".stripMargin
        }
        val tools = """{"tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}]}"""
        val data = spark.range(1).select(lit("hello").as("text"), lit(tools).as("body"))
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          val stage = new TestableAIDefaultRawPrompt("default-raw")
            .setApiType(api).setUrl(server.url + "/v1").setModel("model")
            .setInputCol("text").setOutputCol("answer").setErrorCol("error")
          if (columnBinding) stage.setExtraBodyCol("body") else stage.setExtraBody(tools)
          assert(stage.isDefined(stage.rawResponseCol) && !stage.isSet(stage.rawResponseCol))
          if (columnBinding) {
            val rejected = stage.transform(data)
            assert(!rejected.schema.fieldNames.contains("raw"))
            val row = rejected.head()
            assert(row.getAs[String]("answer") == null)
            assert(row.getAs[Row]("error").getString(0).contains("invalid_request"))
          } else {
            intercept[IllegalArgumentException](stage.transform(data))
          }
          assert(server.requests.isEmpty)
          stage.setRawResponseCol("raw")
          val result = stage.transform(data)
          assert(result.schema == stage.transformSchema(data.schema))
          val row = result.head()
          assert(row.getAs[Row]("error") == null)
          assert(row.getAs[String]("answer") == "answer")
          assert(row.getAs[String]("raw") == reply)
          assert(server.requests.size() == 1)
        }
      }
    }
  }

  test("JSON postprocessing diagnostics use the same parser options as the parsed output") {
    val content = JsObject("value" -> JsString("31/12/2025")).compactPrint
    val reply = JsObject("choices" -> JsArray(JsObject("index" -> JsNumber(0),
      "message" -> JsObject("role" -> JsString("assistant"), "content" -> JsString(content))))).compactPrint
    Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
      val stage = new TestableAIDateParserPrompt("date-parser")
        .setUrl(server.url + "/v1").setModel("model").setInputCol("text")
        .setOutputCol("answer").setErrorCol("error")
        .setPostProcessingOptions(Map("jsonSchema" -> "value DATE"))
      val data = spark.range(1).select(lit("hello").as("text"))
      val result = stage.transform(data)
      assert(result.schema == stage.transformSchema(data.schema))
      val row = result.head()
      assert(row.getAs[Row]("answer").getAs[java.sql.Date]("value") == java.sql.Date.valueOf("2025-12-31"))
      assert(row.getAs[Row]("error") == null)
      assert(server.requests.size() == 1)
    }
  }

  test("JSON numeric limit errors are distinguished from malformed documents") {
    val error = intercept[IllegalArgumentException](AIJson.parse("1" * 1001))
    assert(error.getMessage.contains("JSON exceeds supported parsing limits"))
    assert(intercept[IllegalArgumentException](AIJson.parse("""{"broken":}"""))
      .getMessage.contains("Invalid JSON"))
  }

  test("container numeric setters enforce decodable numeric token boundaries") {
    Seq(1000, 1001).foreach { size =>
      val digits = "1" * size
      val numbers: Seq[java.lang.Number] = Seq(BigInt(digits), BigDecimal(digits),
        new java.math.BigInteger(digits), new java.math.BigDecimal(digits))
      numbers.foreach { number =>
        val stage = AIPrompt.withSession(spark)(new AIPrompt())
        val scalaMap = Map[String, Any]("x" -> number)
        val javaMap = new LinkedHashMap[String, Object]()
        javaMap.put("x", number)
        val setters: Seq[() => AIPrompt] = Seq(
          () => stage.setExtraBody(scalaMap), () => stage.setExtraBody(javaMap),
          () => stage.setQuestions(scalaMap), () => stage.setQuestions(javaMap))
        setters.foreach { set =>
          if (size == 1001) intercept[IllegalArgumentException](set())
          else set()
        }
        if (size == 1000) {
          assert(stage.extraBody.jsonDecode(stage.extraBody.jsonEncode(Left(stage.getExtraBody))) ==
            Left(stage.getExtraBody))
          assert(stage.questions.jsonDecode(stage.questions.jsonEncode(Left(stage.getQuestions))) ==
            Left(stage.getQuestions))
        }
      }
    }
  }

  test("typed JSON numeric setters enforce the same parser and persistence boundaries") {
    Seq("1" * 1000, "1" * 1001, "-" + "1" * 1000, "1." + "1" * 999).foreach { token =>
      val typed = JsObject("x" -> JsNumber(BigDecimal(token)))
      val text = s"""{"x":$token}"""
      val supported = scala.util.Try(AIJson.objectString(text)).isSuccess
      val stage = AIPrompt.withSession(spark)(new AIPrompt())
      val setters: Seq[() => AIPrompt] = Seq(
        () => stage.setExtraBody(typed), () => stage.setExtraBody(text),
        () => stage.setQuestions(typed), () => stage.setQuestions(text))
      setters.foreach { set =>
        if (!supported) intercept[IllegalArgumentException](set())
        else set()
      }
      if (supported) {
        assert(stage.extraBody.jsonDecode(stage.extraBody.jsonEncode(Left(stage.getExtraBody))) ==
          Left(stage.getExtraBody))
        assert(stage.questions.jsonDecode(stage.questions.jsonEncode(Left(stage.getQuestions))) ==
          Left(stage.getQuestions))
      }
    }
  }

  test("large ordered JSON objects avoid quadratic per-row construction and merging") {
    val entries = (0 until 50000).map(index => s"field_$index" -> s"value_$index")
    val json = entries.map { case (key, value) => s""""$key":"$value"""" }.mkString("{", ",", "}")
    val javaMap = new LinkedHashMap[String, String]()
    entries.foreach { case (key, value) => javaMap.put(key, value) }
    failAfter(Span(30, Seconds)) {
      val parsed = AIJson.objectString(json)
      assert(parsed.fields.keys.toVector == entries.map(_._1).toVector)
      assert(AIJson.fromAny(javaMap).compactPrint == json)
      val decoded = AIJson.AnyFormat.read(parsed)
      assert(AIJson.fromAny(decoded).compactPrint == json)
      val merged = AIExtraBody.merge(JsObject(), parsed, Set.empty, "chat_completions", lossless = true)
      assert(merged.compactPrint == json)
      val updated = merged.fields.updated("field_25000", JsString("replaced")) - "field_0"
      assert(updated.keys.toVector == entries.map(_._1).tail.toVector)
      assert(updated("field_25000") == JsString("replaced"))
    }
  }

  test("JSON-valued defaults preserve exact numbers and explicitness through save load") {
    val data = spark.range(1).select(lit("hello").as("text"))
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val stage = new TestableAIDefaultJsonPrompt("default-json-precision")
        .setUrl(server.url + "/v1").setDeploymentName("explicit-model")
        .setInputCol("text").setOutputCol("answer").setErrorCol("error")
      val expected = AIJson.fromAny(stage.getExtraBody)
      val directory = Files.createTempDirectory("aiprompt-default-json-").resolve("stage").toString
      try {
        stage.copy(ParamMap.empty).write.session(spark).save(directory)
        val loaded = AIPrompt.load(directory)
        assert(!loaded.isSet(loaded.extraBody))
        assert(loaded.getDefault(loaded.extraBody) == stage.getDefault(stage.extraBody))
        assert(AIJson.fromAny(loaded.getExtraBody) == expected)
        assert(stage.transformSchema(data.schema) == loaded.transformSchema(data.schema))
        assert(loaded.transform(data).head().getAs[String]("answer") == "answer")
        assert(AIJson.objectString(server.requests.peek()._2).fields("value") ==
          expected.asJsObject.fields("value"))
      } finally {
        org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File(directory).getParentFile)
      }
    }
  }

  test("extraBody preserves top-level order through setters persistence and HTTP") {
    val data = spark.range(1).select(lit("hello").as("text"))
    val names = Seq("e_one", "d_two", "c_three", "b_four", "a_five", "z_six")
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val stage = new AIPrompt().setUrl(server.url + "/v1").setDeploymentName("explicit-model")
        .setInputCol("text").setOutputCol("answer").setErrorCol("error")
      Seq(names, names.reverse).foreach { order =>
        val fields = order.map(name => name -> JsString(name))
        val extension = JsObject(ListMap(fields: _*))
        val javaMap = new java.util.LinkedHashMap[String, Object]()
        order.foreach(name => javaMap.put(name, name))
        val setters: Seq[() => AIPrompt] = Seq(
          () => stage.setExtraBody(extension),
          () => stage.setExtraBody(extension.compactPrint),
          () => stage.setExtraBody(ListMap(order.map(name => name -> (name: Any)): _*)),
          () => stage.setExtraBody(javaMap))
        setters.foreach { set =>
          assert(set().getExtraBody.keys.toSeq == order)
        }
        val directory = Files.createTempDirectory("aiprompt-extension-order-").resolve("stage").toString
        try {
          stage.copy(ParamMap.empty).write.session(spark).save(directory)
          val loaded = AIPrompt.load(directory)
          assert(loaded.getExtraBody.keys.toSeq == order)
          assert(stage.transformSchema(data.schema) == loaded.transformSchema(data.schema))
          assert(loaded.transform(data).head().getAs[String]("answer") == "answer")
          val wireKeys = AIJson.objectString(server.requests.asScala.last._2).fields.keys.toSeq
          assert(wireKeys.filter(names.contains) == order)
        } finally {
          org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File(directory).getParentFile)
        }
      }
    }
  }

  Seq("chat_completions", "systemone", "decisions").foreach { api =>
    test(s"generated R wrapper defaults preserve caller intent through $api inference") {
      val stage = api match {
        case "systemone" => new AISystemOne()
        case "decisions" => new OpenAIDecisions()
        case _ => new AIPrompt()
      }
      val directory = Files.createTempDirectory(tmpDir, "aiprompt-r-wrapper-").toFile
      try {
        val conf = CodegenConfig("test-module", None, directory.getAbsolutePath, directory.getAbsolutePath,
          "1.0.0", "1.0.0", "1.0.0", "synapseml-test")
        stage.makeRFile(conf)
        val files = conf.rSrcDir.listFiles().filter(_.getName.endsWith(".R"))
        assert(files.length == 1)
        val source = FileUtils.readFileToString(files.head, StandardCharsets.UTF_8)
        val arguments = source.split("\n").map(_.trim).dropWhile(!_.contains("<- function("))
          .drop(1).takeWhile(!_.startsWith("uid=")).filter(_.contains("=")).map { line =>
            val pair = line.stripSuffix(",").split("=", 2)
            pair(0) -> pair(1)
          }.toMap
        // Reproduce the generated conditional setters for omitted R arguments.
        stage.params.foreach { raw =>
          val param = raw.asInstanceOf[Param[Any]]
          if (arguments(param.name) != "NULL") {
            stage.set(param, stage.getDefault(param).getOrElse(fail(s"Unexpected R default for ${param.name}")))
          }
        }
        val response = api match {
          case "systemone" => NativeRequestFixtures.SystemResponse
          case "decisions" => NativeRequestFixtures.DecisionResponse
          case _ => chatReply
        }
        Using.resource(new AIRecordingServer((_, _) => 200 -> response)) { server =>
          stage.setUrl(server.url + (if (api == "systemone") "" else "/v1"))
            .setModel("model").setOutputCol("answer").setErrorCol("error")
          api match {
            case "systemone" => stage.setStateCol("text").setQuestions(NativeRequestFixtures.SystemQuestions)
            case "decisions" => stage.setInputCol("text").setQuestions(NativeRequestFixtures.DecisionQuestions)
            case _ => stage.setPromptTemplate("{text}")
          }
          val input = spark.range(1).select(lit("hello").as("text"))
          val expectedSchema = stage.transformSchema(input.schema)
          val result = stage.transform(input)
          assert(result.schema == expectedSchema)
          assert(result.head().getAs[Row]("error") == null)
          assert(server.requests.size() == 1)
          assert(!stage.isSet(stage.messagesCol) && !stage.isSet(stage.systemPrompt))
        }
      } finally {
        FileUtils.deleteDirectory(directory)
      }
    }
  }

  test("native questions preserve object and array order through facade leaf persistence and column bindings") {
    val data = spark.range(1).select(lit("hello").as("text"))
    val names = Seq("e_one", "d_two", "c_three", "b_four", "a_five", "z_six")
    val criteria = JsObject(ListMap(names.map(name => name -> JsString(name)): _*))
    val systemQuestions = JsObject(ListMap(names.map(name => name -> JsObject(
      "type" -> JsString("choice"), "instructions" -> JsString(name), "criteria" -> criteria)): _*))
    val decisionQuestions = JsArray(names.map(name => JsObject("type" -> JsString("predicate"),
      "name" -> JsString(name), "instructions" -> JsString(name), "extension" -> criteria)).toVector)
    Seq(
      (new AIPrompt().setApiType("systemone"), AIPrompt.load _, systemQuestions, NativeRequestFixtures.SystemResponse),
      (new AISystemOne(), AISystemOne.load _, systemQuestions, NativeRequestFixtures.SystemResponse),
      (new AIPrompt().setApiType("decisions"), AIPrompt.load _, decisionQuestions,
        NativeRequestFixtures.DecisionResponse),
      (new OpenAIDecisions(), OpenAIDecisions.load _, decisionQuestions, NativeRequestFixtures.DecisionResponse)
    ).foreach { case (stage, load, questions, reply) =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        stage.setUrl(server.url).setModel("model").setInputCol("text").setQuestions(questions.compactPrint)
          .setErrorCol("error")
        val saved = Files.createTempDirectory(tmpDir, "aiprompt-native-question-order").resolve("model").toString
        stage.write.save(saved)
        val fromColumn = stage.copy(ParamMap.empty).setQuestionsCol("questions")
        val savedColumn = Files.createTempDirectory(tmpDir, "aiprompt-native-question-column").resolve("model").toString
        fromColumn.write.save(savedColumn)
        Seq(stage -> data, stage.copy(ParamMap.empty) -> data, load(saved) -> data,
          load(savedColumn) -> data.withColumn("questions", lit(questions.compactPrint))).foreach {
          case (candidate, input) =>
            server.requests.clear()
            assert(Option(candidate.transform(input).head().getAs[Row]("error")).isEmpty)
            assert(server.requests.size() == 1)
            val sent = AIJson.objectString(server.requests.peek()._2).fields("questions")
            assert(sent.compactPrint == questions.compactPrint)
        }
      }
    }
  }

  Seq(
    "chat_completions" -> """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}""",
    "responses" -> """{"status":"completed","output":[{"type":"message","role":"assistant",
      |"content":[{"type":"output_text","text":"answer"}]}]}""".stripMargin
  ).foreach { case (api, reply) =>
    test(s"$api malformed per-row response schema names produce errors without failing valid rows") {
      import spark.implicits._
      val invalid = Seq(
        """{"properties":{},"name":null}""",
        """{"type":"object","name":42}""",
        """{"type":"json_schema","json_schema":{"name":null,"schema":{"type":"object"}}}""",
        """{"type":"json_schema","json_schema":{"name":42,"schema":{"type":"object"}}}""")
      val data = (invalid :+ """{"type":"text"}""").zipWithIndex.map { case (format, id) =>
        (id, "hello", format)
      }.toDF("id", "text", "format")
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
          .setInputCol("text").setResponseFormatCol("format").setOutputCol("answer").setErrorCol("error")
        val result = stage.transform(data)
        assert(result.schema == stage.transformSchema(data.schema))
        val rows = result.collect().sortBy(_.getAs[Int]("id"))
        assert(rows.take(invalid.size).forall(_.getAs[Row]("error").getString(0).contains("invalid_request")))
        assert(rows.last.getAs[Row]("error") == null && rows.last.getAs[String]("answer") == "answer")
        assert(server.requests.size() == 1)
      }
    }

    test(s"$api preserves ordered response schemas through setters persistence columns and body extensions") {
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val names = Seq("e_one", "d_two", "c_three", "b_four", "a_five", "z_six")
        val data = spark.range(1).select(lit("hello").as("text"))
        def prompt: AIPrompt = new AIPrompt().setApiType(api).setUrl(server.url + "/v1")
          .setModel("model").setInputCol("text").setErrorCol("error")
        Seq(names, names.reverse).foreach { order =>
          val schema = Map[String, Any]("type" -> "object",
            "properties" -> ListMap(order.map(_ -> Map("type" -> "string")): _*),
            "required" -> names, "additionalProperties" -> false)
          val stage = prompt.setResponseSchema(schema)
          def verify(candidate: AIPrompt, data: DataFrame): Unit = {
            server.requests.clear()
            assert(Option(candidate.transform(data).head().getAs[Row]("error")).isEmpty)
            assert(server.requests.size() == 1)
            val body = server.requests.peek()._2
            val start = body.indexOf("\"properties\":{")
            assert(start >= 0)
            val positions = order.map(name => body.indexOf(s""""$name":""", start))
            assert(positions.forall(_ >= 0) && positions == positions.sorted)
          }
          val saved = Files.createTempDirectory(tmpDir, "aiprompt-ordered-schema").resolve("model").toString
          stage.write.save(saved)
          Seq("original" -> stage, "copy" -> stage.copy(ParamMap.empty),
            "loaded" -> AIPrompt.load(saved)).foreach { case (label, candidate) =>
            withClue(s"$label: ")(verify(candidate, data))
          }
          val formatJson = AnyJsonFormat.anyFormat.write(stage.getResponseFormat).compactPrint
          verify(prompt.setResponseFormatCol("format"), data.withColumn("format", lit(formatJson)))
          val details = Map[String, Any]("name" -> "response_schema", "strict" -> true, "schema" -> schema)
          val extension = if (api == "chat_completions") {
            Map("response_format" -> Map("type" -> "json_schema", "json_schema" -> details))
          } else Map("text" -> Map("format" -> (details + ("type" -> "json_schema"))))
          val extended = prompt.setExtraBody(extension)
          verify(extended, data)
          val savedExtension = Files.createTempDirectory(tmpDir, "aiprompt-ordered-extension").resolve("model").toString
          extended.copy(ParamMap.empty).write.save(savedExtension)
          withClue("loaded extension: ")(verify(AIPrompt.load(savedExtension), data))
          verify(prompt.setExtraBodyCol("body"),
            data.withColumn("body", lit(AnyJsonFormat.anyFormat.write(extension).compactPrint)))
        }
      }
    }
  }

  private def merge(base: String,
                    extension: String,
                    owned: Set[List[String]] = Set.empty,
                    api: String = "responses",
                    lossless: Boolean = true): JsObject =
    AIExtraBody.merge(AIJson.objectString(base), AIJson.objectString(extension), owned, api, lossless)

  test("strict JSON parsing preserves booleans null arrays and exact numeric values") {
    val text = """{"nested":{"items":[true,false,null,"true",1,1.125,9223372036854775808]},
      |"text":"quoted \"text\" and escaped \n newline"}""".stripMargin
    assert(AIJson.parse(text) == text.parseJson)
    assert(AIJson.objectString(text) == text.parseJson)
    assert(AIJson.parse("null") == JsNull)
  }

  test("duplicate object keys are rejected at every nesting level without disclosing payloads") {
    Seq(
      """{"private-key":1,"private-key":2}""",
      """{"nested":{"private-key":1,"private-key":2}}""",
      """{"items":[{"private-key":1,"private-key":2}]}"""
    ).foreach { text =>
      val error = intercept[IllegalArgumentException](AIJson.parse(text))
      assert(!error.getMessage.contains("private-key"))
      assert(!error.getMessage.contains(text))
    }
  }

  test("malformed truncated multiple-root and nonfinite JSON is rejected safely") {
    Seq(
      "", " ", """{"private":"unterminated""", """{"x":""", "[1,",
      """{"x":1,}""", """{"x":NaN}""", """{"x":Infinity}""", """{"x":-Infinity}"""
    ).foreach { text =>
      val error = intercept[IllegalArgumentException](AIJson.parse(text))
      assert(!error.getMessage.contains("unterminated"))
    }
    intercept[IllegalArgumentException](AIJson.parse("{} {}"))
    intercept[IllegalArgumentException](AIJson.parse("[] true"))
    intercept[IllegalArgumentException](AIJson.parse(missing))
  }

  test("body bindings reject non-object roots while generic JSON values remain supported") {
    Seq("[]", "null", "true", "1", "\"string\"").foreach { text =>
      intercept[IllegalArgumentException](AIJson.objectString(text))
    }
    intercept[IllegalArgumentException](AIJson.obj(AIJson.fromAny(Seq("not", "an", "object"))))
    assert(AIJson.fromAny(Seq("array", true, missing)) == JsArray(JsString("array"), JsBoolean(true), JsNull))
  }

  test("generic object bindings reject null before changing configuration") {
    val stage = AIPrompt.withSession(spark)(new AIPrompt())
    Seq(stage.extraBody, stage.responseFormat).foreach { param =>
      val original = Map[String, Any]("type" -> "json_object")
      stage.setScalarParam(param, original)
      val invalid: Seq[Either[Map[String, Any], String]] = Seq(missing, Left(missing), Right(missing))
      val setters: Seq[() => AIPrompt] = Seq(
        () => stage.setScalarParam(param, missing), () => stage.setVectorParam(param, missing)) ++
        invalid.flatMap(value => Seq(
          () => stage.set(param, value), () => stage.copy(ParamMap(param -> value))))
      setters.foreach { set =>
        intercept[IllegalArgumentException](set())
        assert(stage.getScalarParam(param) == original)
      }
      stage.setVectorParam(param, "")
      assert(stage.getVectorParam(param) == "")
    }
  }

  test("generic object bindings preserve nested null and optional null cells through save load and HTTP") {
    val data = spark.range(1).select(lit("hello").as("text"), lit(missing).cast("string").as("body"))
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      Seq(false, true).foreach { columnBinding =>
        val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error")
        if (columnBinding) {
          stage.setVectorParam(stage.extraBody, "body").setVectorParam(stage.responseFormat, "body")
        } else {
          stage.setScalarParam(stage.extraBody, Map[String, Any]("metadata" -> Map("optional" -> missing)))
          stage.setScalarParam(stage.responseFormat, Map[String, Any]("type" -> "json_object"))
        }
        val saved = Files.createTempDirectory(tmpDir, "aiprompt-null-bindings").resolve("stage").toString
        stage.copy(ParamMap.empty).write.session(spark).save(saved)
        val loaded = AIPrompt.load(saved)
        assert(loaded.get(loaded.extraBody) == stage.get(stage.extraBody))
        assert(loaded.get(loaded.responseFormat) == stage.get(stage.responseFormat))
        val result = loaded.transform(data)
        assert(result.schema == loaded.transformSchema(data.schema))
        val row = result.head()
        assert(row.getAs[Row]("error") == null && row.getAs[String]("answer") == "answer")
        val body = AIJson.objectString(server.requests.asScala.last._2)
        if (columnBinding) {
          assert(!body.fields.contains("metadata") && !body.fields.contains("response_format"))
        } else {
          assert(body.fields("metadata") == JsObject("optional" -> JsNull))
          assert(body.fields("response_format") == JsObject("type" -> JsString("json_object")))
        }
      }
      assert(server.requests.size() == 2)
    }
  }

  test("Java and Scala containers convert without coercing typed values or rejecting shared children") {
    val shared = mutable.ArrayBuffer[Any]("value", true, missing)
    val scalaValues = Map[String, Any](
      "left" -> shared, "right" -> shared, "integer" -> BigInt("9223372036854775808"),
      "decimal" -> BigDecimal("0.1234567890123456789"), "array" -> Array(1, 2))
    val javaValues = new LinkedHashMap[String, Any]()
    val javaList = new ArrayList[Any]()
    javaList.add("value")
    javaList.add(true)
    javaList.add(missing)
    javaValues.put("left", javaList)
    javaValues.put("right", javaList)
    javaValues.put("integer", new java.math.BigInteger("9223372036854775808"))
    javaValues.put("decimal", new java.math.BigDecimal("0.1234567890123456789"))
    javaValues.put("array", Array(1, 2))
    val expected = """{"left":["value",true,null],"right":["value",true,null],
      |"integer":9223372036854775808,"decimal":0.1234567890123456789,"array":[1,2]}""".stripMargin.parseJson
    assert(AIJson.fromAny(scalaValues) == expected)
    assert(AIJson.fromAny(javaValues) == expected)
  }

  test("nonfinite Scala and Java numeric members are rejected before body serialization") {
    val numbers: Seq[Any] = Seq(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity,
      Float.NaN, Float.PositiveInfinity, Float.NegativeInfinity,
      java.lang.Double.valueOf(Double.NaN), java.lang.Float.valueOf(Float.PositiveInfinity))
    numbers.foreach { number =>
      intercept[IllegalArgumentException](AIJson.fromAny(Map("value" -> number)))
      val javaValues = new ArrayList[Any]()
      javaValues.add(number)
      intercept[IllegalArgumentException](AIJson.fromAny(javaValues))
    }
  }

  test("finite Float values retain their decimal representation through setters persistence and HTTP") {
    val values = Seq(0.7f, 0.1f, -0.7f, Float.MinPositiveValue, Float.MaxValue)
    val scalaValues = values.zipWithIndex.map { case (value, index) => s"v$index" -> value }.toMap
    val javaValues = new LinkedHashMap[String, Object]()
    scalaValues.foreach { case (key, value) => javaValues.put(key, java.lang.Float.valueOf(value)) }
    val expected = JsObject(scalaValues.map { case (key, value) =>
      key -> JsNumber(BigDecimal(java.lang.Float.toString(value)))
    })
    val input = spark.range(1).select(lit("hello").as("text"))
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model")
        .setInputCol("text").setOutputCol("answer").setErrorCol("error")
      val setters: Seq[() => AIPrompt] = Seq(
        () => stage.setExtraBody(scalaValues), () => stage.setExtraBody(javaValues))
      setters.foreach { set =>
        set()
        val saved = Files.createTempDirectory(tmpDir, "aiprompt-floats").resolve("stage").toString
        stage.copy(ParamMap.empty).write.session(spark).save(saved)
        val loaded = AIPrompt.load(saved)
        assert(AIJson.fromAny(loaded.getExtraBody) == expected)
        assert(loaded.transform(input).head().getAs[Row]("error") == null)
        val sent = AIJson.objectString(server.requests.asScala.last._2)
        assert(JsObject(sent.fields.filter { case (key, _) => scalaValues.contains(key) }) == expected)
      }
      assert(server.requests.size() == 2)
      val questionSetters: Seq[() => AIPrompt] = Seq(
        () => stage.setQuestions(scalaValues), () => stage.setQuestions(javaValues))
      questionSetters.foreach { set =>
        val candidate = set()
        assert(AIJson.fromAny(candidate.getQuestions) == expected)
        assert(AIJson.fromAny(candidate.questions.jsonDecode(
          candidate.questions.jsonEncode(Left(candidate.getQuestions))).left.get) == expected)
      }
    }
  }

  test("non-string object keys and unsupported member types are rejected") {
    intercept[IllegalArgumentException](AIJson.fromAny(Map(1 -> "value")))
    val javaValues = new LinkedHashMap[Any, Any]()
    javaValues.put(true, "value")
    intercept[IllegalArgumentException](AIJson.fromAny(javaValues))
    val error = intercept[IllegalArgumentException](AIJson.fromAny(Map("value" -> new Object())))
    assert(error.getMessage == "Unsupported JSON value type")
  }

  test("Scala map sequence and array cycles are rejected without recursion overflow") {
    val map = mutable.Map.empty[String, Any]
    map.update("self", map)
    val sequence = mutable.ArrayBuffer.empty[Any]
    sequence += sequence
    val array = new Array[AnyRef](1)
    array(0) = array
    Seq(map, sequence, array).foreach { cyclic =>
      val error = intercept[IllegalArgumentException](AIJson.fromAny(cyclic))
      assert(error.getMessage.contains("Cyclic"))
    }
  }

  test("Java map list and mixed Java Scala cycles are rejected") {
    val map = new LinkedHashMap[String, Any]()
    map.put("self", map)
    val list = new ArrayList[Any]()
    list.add(list)
    val mixed = new ArrayList[Any]()
    mixed.add(Map("back" -> mixed))
    Seq(map, list, mixed).foreach { cyclic =>
      val error = intercept[IllegalArgumentException](AIJson.fromAny(cyclic))
      assert(error.getMessage.contains("Cyclic"))
    }
  }

  test("excessive JSON and container nesting is rejected with a controlled error") {
    val depth = 110
    val text = ("[" * depth) + "0" + ("]" * depth)
    intercept[IllegalArgumentException](AIJson.parse(text))
    val nested = (1 to depth).foldLeft[Any]("leaf") { case (value, _) => Seq(value) }
    intercept[IllegalArgumentException](AIJson.fromAny(nested))
  }

  test("maximum JSON payload depth survives public object and question parameter persistence") {
    val text = ("{\"level\":" * 100) + "\"leaf\"" + ("}" * 100)
    val expected = AIJson.parse(text)
    val root = Files.createTempDirectory("aiprompt-json-depth")
    try {
      val stage = new AIPrompt().setExtraBody(text).setQuestions(text)
      stage.write.session(spark).save(root.resolve("model").toString)
      val loaded = AIPrompt.read.session(spark).load(root.resolve("model").toString)
      assert(AIJson.fromAny(loaded.getExtraBody) == expected)
      assert(AIJson.fromAny(loaded.getQuestions) == expected)
      val array = ("[" * 100) + "\"leaf\"" + ("]" * 100)
      stage.setQuestions(array)
      assert(AIJson.fromAny(stage.questions.jsonDecode(stage.questions.jsonEncode(Left(stage.getQuestions)))
        .left.get) == AIJson.parse(array))
    } finally org.apache.commons.io.FileUtils.deleteDirectory(root.toFile)
  }

  test("JSON payload depth rejects the first unsupported scalar leaf for every setter representation") {
    val text = ("{\"level\":" * 101) + "\"leaf\"" + ("}" * 101)
    val nested = (1 to 101).foldLeft[Any]("leaf") { case (value, _) => Map("level" -> value) }
    val json = (1 to 101).foldLeft[spray.json.JsValue](JsString("leaf")) { case (value, _) =>
      JsObject("level" -> value)
    }
    intercept[IllegalArgumentException](AIJson.parse(text))
    intercept[IllegalArgumentException](AIJson.fromAny(nested))
    intercept[IllegalArgumentException](new AIPrompt().setExtraBody(json.asJsObject))
    intercept[IllegalArgumentException](new AIPrompt().setQuestions(json))
    val stage = new AIPrompt()
    intercept[IllegalArgumentException](stage.extraBody.jsonDecode("{\"left\":" + text + "}"))
    intercept[IllegalArgumentException](stage.questions.jsonDecode("{\"left\":" + text + "}"))
  }

  test("recursive disjoint members preserve explicit typed subtrees and unknown provider options") {
    val base = """{"model":"deployment","text":{"format":{"type":"json_schema","name":"answer"}},
      |"metadata":{"retained":"yes"}}""".stripMargin
    val extension = """{"text":{"verbosity":"low"},"metadata":{"new":"value"},
      |"future_provider_field":{"array":[1,true,{"value":null}]}}""".stripMargin
    val expected = """{"model":"deployment","text":{"format":{"type":"json_schema","name":"answer"},
      |"verbosity":"low"},"metadata":{"retained":"yes","new":"value"},
      |"future_provider_field":{"array":[1,true,{"value":null}]}}""".stripMargin.parseJson
    assert(merge(base, extension, Set(List("text", "format"))) == expected)
  }

  test("implicit defaults can be overridden and null arrays and scalars replace atomically") {
    val base = """{"temperature":1,"metadata":{"old":1},"stop":["old","retained"],
      |"text":{"verbosity":"medium"}}""".stripMargin
    val extension = """{"temperature":0.3,"metadata":null,"stop":["new"],"text":"replacement"}"""
    assert(merge(base, extension) == extension.parseJson)
    assert(merge("""{"metadata":{"old":1}}""", """{"metadata":[]}""") == """{"metadata":[]}""".parseJson)
    assert(merge("""{"metadata":null}""", """{"metadata":{"new":1}}""") ==
      """{"metadata":{"new":1}}""".parseJson)
  }

  test("equal explicit values and modifications below an owned subtree are conflicts") {
    intercept[IllegalArgumentException] {
      merge("""{"temperature":0.3}""", """{"temperature":0.3}""", Set(List("temperature")))
    }
    val error = intercept[IllegalArgumentException] {
      merge("""{"text":{"format":{"type":"json_schema"}}}""",
        """{"text":{"format":{"name":"private-value"}}}""", Set(List("text", "format")))
    }
    assert(error.getMessage.contains("text.format"))
    assert(!error.getMessage.contains("private-value"))
    intercept[IllegalArgumentException] {
      merge("""{"text":{"verbosity":"medium"}}""", """{"text":{}}""", Set(List("text")))
    }
  }

  test("atomic ancestor replacements cannot overwrite explicitly owned descendants") {
    Seq("null", "[]", "\"replacement\"", "3", "true").foreach { replacement =>
      intercept[IllegalArgumentException] {
        merge("""{"text":{"format":{"type":"json_schema"}}}""", s"""{"text":$replacement}""",
          Set(List("text", "format", "type")))
      }
    }
    assert(merge("""{"text":{"format":{"type":"json_schema"}}}""", """{"text":{}}""",
      Set(List("text", "format", "type"))) == """{"text":{"format":{"type":"json_schema"}}}""".parseJson)
  }

  test("ownership checks also apply when an extension introduces a missing parent object") {
    intercept[IllegalArgumentException] {
      merge("{}", """{"text":{"format":{"type":"json_schema"}}}""", Set(List("text", "format")))
    }
    assert(merge("{}", """{"text":{"verbosity":"low"}}""", Set(List("text", "format"))) ==
      """{"text":{"verbosity":"low"}}""".parseJson)
  }

  test("explicit token ownership conflicts with every semantic alias including equal values") {
    val aliases = Seq("max_tokens", "max_completion_tokens", "max_output_tokens")
    aliases.foreach { typed =>
      aliases.foreach { extra =>
        intercept[IllegalArgumentException] {
          merge(s"""{"$typed":12}""", s"""{"$extra":12}""", Set(List(typed)))
        }
      }
    }
  }

  test("raw token aliases replace implicit alternate defaults rather than leaving contradictory fields") {
    val aliases = Seq("max_tokens", "max_completion_tokens", "max_output_tokens")
    aliases.foreach { default =>
      aliases.foreach { extra =>
        assert(merge(s"""{"$default":12,"temperature":0.2}""", s"""{"$extra":6}""") ==
          s"""{"$extra":6,"temperature":0.2}""".parseJson)
      }
    }
    intercept[IllegalArgumentException](merge("{}", """{"max_tokens":12,"max_completion_tokens":12}"""))
    intercept[IllegalArgumentException](merge("""{"max_tokens":12,"max_output_tokens":12}""", "{}"))
  }

  test("identity routing authentication transport and storage fields are reserved in body extensions") {
    val reserved = Seq("model", "messages", "input", "state", "questions", "url", "endpoint", "base_url",
      "api_version", "api_key", "subscription_key", "subscriptionKey", "AADToken", "CustomAuthHeader",
      "customHeaders", "headers", "extra_headers", "extra_query", "query", "timeout", "max_retries",
      "retry_policy", "outputPath", "outputBatchSize", "output_path", "output_batch_size", "customUrlRoot")
    reserved.foreach { key =>
      val error = intercept[IllegalArgumentException](merge("{}", s"""{"$key":"private-value"}"""))
      assert(error.getMessage.contains(key))
      assert(!error.getMessage.contains("private-value"))
    }
    assert(merge("{}", """{"provider_options":{"model":"nested-value"}}""") ==
      """{"provider_options":{"model":"nested-value"}}""".parseJson)
  }

  test("streaming and background lifecycle protections validate the fully merged body") {
    Seq("stream", "background").foreach { key =>
      Seq("true", "null", "\"true\"", "1", "{}").foreach { value =>
        intercept[IllegalArgumentException](merge("{}", s"""{"$key":$value}"""))
      }
      intercept[IllegalArgumentException](merge(s"""{"$key":true}""", "{}"))
      assert(merge("{}", s"""{"$key":false}""") == s"""{"$key":false}""".parseJson)
    }
  }

  test("tool request fields require raw response exposure whether supplied by the base or extension") {
    Seq("tools", "tool_choice", "functions", "function_call").foreach { key =>
      val request = s"""{"$key":[]}"""
      intercept[IllegalArgumentException](merge("{}", request, lossless = false))
      intercept[IllegalArgumentException](merge(request, "{}", lossless = false))
      assert(merge("{}", request, lossless = true) == request.parseJson)
    }
    assert(merge("{}", """{"metadata":{"tools":"ordinary metadata"}}""", lossless = false) ==
      """{"metadata":{"tools":"ordinary metadata"}}""".parseJson)
  }

  test("embedding encoding must remain float after merge and is not silently stripped") {
    Seq("\"base64\"", "\"future\"", "null", "true", "1").foreach { encoding =>
      intercept[IllegalArgumentException](merge("{}", s"""{"encoding_format":$encoding}""", api = "embeddings"))
    }
    intercept[IllegalArgumentException](merge("""{"encoding_format":"base64"}""", "{}", api = "embeddings"))
    assert(merge("{}", """{"encoding_format":"float"}""", api = "embeddings") ==
      """{"encoding_format":"float"}""".parseJson)
    assert(merge("""{"encoding_format":"float"}""", "{}", api = "embeddings") ==
      """{"encoding_format":"float"}""".parseJson)
    assert(merge("{}", "{}", api = "embeddings") == JsObject())
    assert(merge("{}", """{"encoding_format":"base64"}""", api = "responses") ==
      """{"encoding_format":"base64"}""".parseJson)
  }
}
