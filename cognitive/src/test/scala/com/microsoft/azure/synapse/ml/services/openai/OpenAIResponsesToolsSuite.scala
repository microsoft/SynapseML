// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.http.util.EntityUtils
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row, functions => F}
import org.apache.spark.sql.types._
import spray.json._

class OpenAIResponsesToolsSuite extends TransformerFuzzing[OpenAIResponses] {
  override val compareDataInSerializationTest: Boolean = false
  override def ignoreExperimentFuzzing: Boolean = true

  import spark.implicits._

  private def configured: OpenAIResponses = new OpenAIResponses()
    .setUrl("http://localhost:1/v1")
    .setDeploymentName("gpt-5.1")
    .setSubscriptionKey("offline-dummy")
    .setMessagesCol("messages")
    .setOutputCol("out")
    .setTools(ToolTestFixtures.WeatherToolJson)
    .setToolChoice("auto")

  private def messagesDf: DataFrame =
    Seq(Seq(OpenAIMessage("user", "Weather in Seattle?"))).toDF("messages")

  private def responseFrame(json: String): DataFrame =
    spark.read.schema(ResponsesModelResponseV2.schema).json(Seq(json).toDS)

  private def wrappedResponse(json: String): DataFrame = {
    val response = responseFrame(json)
    response.select(F.struct(response.columns.map(F.col): _*).as("out"))
  }

  private def entityJson(entity: org.apache.http.HttpEntity): JsObject =
    EntityUtils.toString(entity).parseJson.asJsObject

  test("Responses keeps its public schema while parsing tool fields internally") {
    assert(configured.responseDataType === ResponsesModelResponse.schema)
    assert(configured.transformSchema(messagesDf.schema)("out").dataType === ResponsesModelResponseV2.schema)
  }

  test("tools and modern params serialize as JSON values with wire names") {
    val transformer = configured
      .setParallelToolCalls(true)
      .setMaxToolCalls(3)
      .setMaxCompletionTokens(800)
      .setVerbosity("low")
      .setReasoningEffort("low")
      .setReasoningSummary("concise")
      .setReasoningContext("current_turn")
      .setReasoningMode("standard")
      .setInstructions("Use trusted tools.")
      .setTruncation("auto")
      .setMetadata(Map("suite" -> "offline"))
      .setInclude(Seq("reasoning.encrypted_content"))
      .setTopLogprobs(5)
      .setSafetyIdentifier("hashed-user")
      .setPromptCacheKey("weather-prefix")
      .setServiceTier("flex")

    val params = transformer.getOptionalParams(ToolTestFixtures.dynamicRow())
    val payload = entityJson(transformer.getStringEntity(
      Seq(ToolTestFixtures.message("user", "Weather?")),
      params))

    assert(payload.fields("tools").isInstanceOf[JsArray])
    assert(payload.fields("tool_choice") === JsString("auto"))
    assert(payload.fields("parallel_tool_calls") === JsBoolean(true))
    assert(payload.fields("max_tool_calls") === JsNumber(3))
    assert(payload.fields("max_output_tokens") === JsNumber(800))
    assert(payload.fields("instructions") === JsString("Use trusted tools."))
    assert(payload.fields("metadata").asJsObject.fields("suite") === JsString("offline"))
    assert(payload.fields("include") === JsArray(JsString("reasoning.encrypted_content")))
    val reasoning = payload.fields("reasoning").asJsObject
    assert(reasoning.fields("effort") === JsString("low"))
    assert(reasoning.fields("summary") === JsString("concise"))
    assert(reasoning.fields("context") === JsString("current_turn"))
    assert(reasoning.fields("mode") === JsString("standard"))
    assert(!payload.fields.contains("reasoning_effort"))
    assert(!payload.fields.contains("reasoning_summary"))
    assert(!payload.fields.contains("stream"))
    assert(!payload.fields.contains("background"))
  }

  test("column-bound tools and tool choice are resolved per row") {
    val transformer = new OpenAIResponses()
      .setDeploymentName("gpt-5.1")
      .setToolsCol("row_tools")
      .setToolChoiceCol("row_choice")
      .setParallelToolCallsCol("row_parallel")
      .setMaxToolCallsCol("row_max")

    val row = ToolTestFixtures.dynamicRow(
      ("row_tools", StringType, ToolTestFixtures.WeatherToolJson),
      ("row_choice", StringType, """{"type":"function","name":"get_weather"}"""),
      ("row_parallel", BooleanType, false),
      ("row_max", IntegerType, 2)
    )
    val params = transformer.getOptionalParams(row)
    assert(params("tools").isInstanceOf[JsArray])
    assert(params("tool_choice").asInstanceOf[JsObject].fields("name") ===
      JsString("get_weather"))
    assert(params("parallel_tool_calls") === false)
    assert(params("max_tool_calls") === 2)

    val blank = ToolTestFixtures.dynamicRow(
      ("row_tools", StringType, " "),
      ("row_choice", StringType, "")
    )
    val blankTransformer = new OpenAIResponses()
      .setToolsCol("row_tools")
      .setToolChoiceCol("row_choice")
    val blankParams = blankTransformer.getOptionalParams(blank)
    assert(!blankParams.contains("tools"))
    assert(!blankParams.contains("tool_choice"))
  }

  test("Scala and Java setters validate shapes and documented bounds") {
    val javaTool = new java.util.LinkedHashMap[String, Object]()
    javaTool.put("type", "function")
    javaTool.put("name", "java_weather")
    javaTool.put("parameters", null) //scalastyle:ignore null
    javaTool.put("strict", null) //scalastyle:ignore null
    val javaTools = new java.util.ArrayList[java.util.Map[String, Object]]()
    javaTools.add(javaTool)
    val transformer = new OpenAIResponses().setTools(javaTools)
    assert(OpenAIToolUtils.parseTools(transformer.getTools).elements.head
      .asJsObject.fields("name") === JsString("java_weather"))

    val javaChoice = new java.util.LinkedHashMap[String, Object]()
    javaChoice.put("type", "function")
    javaChoice.put("name", "java_weather")
    transformer.setToolChoice(javaChoice)
    assert(transformer.getToolChoice.parseJson.asJsObject.fields("name") ===
      JsString("java_weather"))

    Seq(-1, 21).foreach(value =>
      assertThrows[IllegalArgumentException](new OpenAIResponses().setTopLogprobs(value)))
    Seq(0, -1).foreach(value =>
      assertThrows[IllegalArgumentException](new OpenAIResponses().setMaxToolCalls(value)))
    assertThrows[IllegalArgumentException](new OpenAIResponses().setInclude(Seq.empty))
    assertThrows[IllegalArgumentException](new OpenAIResponses().setSafetyIdentifier(" "))
    assertThrows[IllegalArgumentException] {
      new OpenAIResponses().setMetadata((1 to 17).map(i => s"k$i" -> "v").toMap)
    }
    assertThrows[IllegalArgumentException] {
      new OpenAIResponses().setMetadata(Map(("k" * 65) -> "v"))
    }
    assertThrows[IllegalArgumentException] {
      new OpenAIResponses().setMetadata(Map("k" -> ("v" * 513)))
    }
    Seq(
      (t: OpenAIResponses) => t.setInstructions(" "),
      (t: OpenAIResponses) => t.setTruncation(" "),
      (t: OpenAIResponses) => t.setPromptCacheKey(" "),
      (t: OpenAIResponses) => t.setServiceTier(" "),
      (t: OpenAIResponses) => t.setConversation(" "),
      (t: OpenAIResponses) => t.setReasoningSummary(" "),
      (t: OpenAIResponses) => t.setReasoningContext(" "),
      (t: OpenAIResponses) => t.setReasoningMode(" ")
    ).foreach(setter =>
      assertThrows[IllegalArgumentException](setter(new OpenAIResponses())))
  }

  test("tools Java state distinguishes unset scalar and column modes") {
    val transformer = new OpenAIResponses()
    assert(transformer.getToolsParamMode === "unset")
    transformer.setTools(Seq(ToolTestFixtures.WeatherToolMap))
    assert(transformer.getToolsParamMode === "scalar")
    transformer
      .addFunctionTool("get_forecast", "forecast", ToolTestFixtures.EmptyObjectSchema)
      .addFunctionTool("get_alerts", "alerts", ToolTestFixtures.EmptyObjectSchema)
    assert(OpenAIToolUtils.parseTools(transformer.getTools).elements.flatMap(
      OpenAIToolUtils.functionName) ===
      Seq("get_weather", "get_forecast", "get_alerts"))
    transformer.setToolsCol("row_tools")
    assert(transformer.getToolsParamMode === "column")
  }

  test("Scala addFunctionTool rejects column-bound tools instead of switching modes") {
    val transformer = new OpenAIResponses().setToolsCol("row_tools")
    val error = intercept[IllegalArgumentException] {
      transformer.addFunctionTool("get_forecast", "forecast", Map("type" -> "object"))
    }
    assert(error.getMessage.contains("column-bound"))
    assert(transformer.getToolsParamMode === "column")
    assert(transformer.getToolsCol === "row_tools")
  }

  test("input assembly and prepareEntity preserve continuation order") {
    val transformer = new OpenAIResponses()
      .setDeploymentName("gpt-5.1")
      .setMessagesCol("messages")
      .setInputItemsCol("items")
      .setFunctionCallOutputsCol("outputs")
      .setPreviousResponseIdCol("response_id")
      .setTools(ToolTestFixtures.WeatherToolJson)

    val messages = Seq(ToolTestFixtures.message("user", "Weather?"))
    val items =
      """[{"type":"reasoning","id":"rs_1","encrypted_content":"E"},
        |{"type":"function_call","id":"fc_1","call_id":"call_a","name":"get_weather",
        |"arguments":"{\"city\":\"Seattle\"}"}]""".stripMargin
    val outputs = Seq(
      ToolTestFixtures.functionOutput("call_a", """{"tempC":20}""", "completed"))
    val row = ToolTestFixtures.dynamicRow(
      ("messages", ToolTestFixtures.MessagesArrayType, messages),
      ("items", StringType, items),
      ("outputs", OpenAIToolColumns.FunctionCallOutputStructType, outputs),
      ("response_id", StringType, "resp_1")
    )

    val prepared = entityJson(transformer.prepareEntity(row).get)
    val direct = entityJson(transformer.getStringEntity(
      messages,
      items,
      outputs,
      transformer.getOptionalParams(row)))
    assert(prepared === direct)
    val input = prepared.fields("input").asInstanceOf[JsArray].elements
    assert(input.size === 4)
    assert(input.head.asJsObject.fields("role") === JsString("user"))
    assert(input(1).asJsObject.fields("type") === JsString("reasoning"))
    assert(input(2).asJsObject.fields("type") === JsString("function_call"))
    assert(input(3).asJsObject.fields("type") === JsString("function_call_output"))
    assert(prepared.fields("previous_response_id") === JsString("resp_1"))
  }

  test("continuation-only payload assembly never requires messagesCol") {
    val transformer = new OpenAIResponses()
      .setDeploymentName("gpt-5.1")
      .setFunctionCallOutputsCol("outputs")
      .setPreviousResponseIdCol("response_id")
      .setTools(ToolTestFixtures.WeatherToolJson)
    val outputs = Seq(ToolTestFixtures.functionOutput("call_a", "ok"))
    val row = ToolTestFixtures.dynamicRow(
      ("outputs", OpenAIToolColumns.FunctionCallOutputStructType, outputs),
      ("response_id", StringType, "resp_1")
    )
    val input = entityJson(transformer.getStringEntity(
      Seq.empty,
      transformer.getOptionalParams(row)))
      .fields("input").asInstanceOf[JsArray].elements
    assert(input.size === 1)
    assert(input.head.asJsObject.fields("type") === JsString("function_call_output"))
    assert(!transformer.shouldSkip(row))
  }

  test("empty messages without continuation inputs are skipped") {
    val row = ToolTestFixtures.dynamicRow(
      ("messages", ToolTestFixtures.MessagesArrayType, Seq.empty[Row]))
    assert(configured.shouldSkip(row))
  }

  test("assistant messages use output_text and existing content parts remain untouched") {
    val transformer = new OpenAIResponses()
    val payload = entityJson(transformer.getStringEntity(
      Seq(
        ToolTestFixtures.message("user", "u"),
        ToolTestFixtures.message("assistant", "a")
      ),
      Map.empty))
    val input = payload.fields("input").asInstanceOf[JsArray].elements
    assert(input.head.asJsObject.fields("content").asInstanceOf[JsArray]
      .elements.head.asJsObject.fields("type") === JsString("input_text"))
    assert(input(1).asJsObject.fields("content").asInstanceOf[JsArray]
      .elements.head.asJsObject.fields("type") === JsString("output_text"))
  }

  test("widened response schema preserves tool reasoning usage and envelope fields") {
    val row = responseFrame(ToolTestFixtures.MixedResponseJson).collect().head
    val output = row.getAs[Seq[Row]]("output")
    assert(output(1).getAs[String]("encrypted_content") === "encrypted")
    assert(output(1).getAs[Seq[Row]]("summary").head.getAs[String]("text") === "stateless")
    assert(output(2).getAs[String]("phase") === "commentary")
    assert(output(3).getAs[String]("call_id") === "call_a")
    val usage = row.getAs[Row]("usage")
    assert(usage.getAs[Row]("input_tokens_details").getAs[Long]("cache_write_tokens") === 2L)
    assert(usage.getAs[Row]("output_tokens_details").getAs[java.lang.Long](
      "cache_write_tokens") == null) //scalastyle:ignore null
    assert(ResponsesModelResponseV2.schema.fieldNames.take(7) ===
      Array("id", "object", "created_at", "model", "output", "system_fingerprint", "usage"))
    val choiceType = ResponsesModelResponseV2.schema("output").dataType.asInstanceOf[ArrayType]
      .elementType.asInstanceOf[StructType]
    assert(choiceType.fieldNames.take(2) === Array("content", "status"))
  }

  test("public response DTO schemas remain backward compatible") {
    assert(TokenDetails.schema.fieldNames ===
      Array(
        "audio_tokens",
        "cached_tokens",
        "reasoning_tokens",
        "accepted_prediction_tokens",
        "rejected_prediction_tokens"))
    assert(ResponsesModelResponse.schema.fieldNames ===
      Array("id", "object", "created_at", "model", "output", "system_fingerprint", "usage"))
    val choiceType = ResponsesModelResponse.schema("output").dataType.asInstanceOf[ArrayType]
      .elementType.asInstanceOf[StructType]
    assert(choiceType.fieldNames === Array("content", "status"))
  }

  test("text extraction prefers the last typed message and suppresses mixed legacy fallback") {
    val transformer = new OpenAIResponses()
    val mixed =
      """{
        |"id":"r","object":"response","created_at":"1","model":"m",
        |"output":[
        | {"status":"completed","content":[{"type":"output_text","text":"LEGACY"}]},
        | {"type":"function_call","call_id":"c","name":"f","arguments":"{}","status":"completed"}
        |],"system_fingerprint":null,"usage":null
        |}""".stripMargin
    val mixedText = wrappedResponse(mixed)
      .select(transformer.getOutputMessageText("out").as("text"))
      .collect().head.getAs[String]("text")
    assert(mixedText == null) //scalastyle:ignore null

    val text = wrappedResponse(ToolTestFixtures.MixedResponseJson)
      .select(transformer.getOutputMessageText("out").as("text"))
      .collect().head.getAs[String]("text")
    assert(text === "Working")
  }

  test("tool-call-only responses are not content-filter errors") {
    val transformer = new OpenAIResponses()
    val toolRow = responseFrame(ToolTestFixtures.ToolCallResponseJson).collect().head
    assert(!transformer.isContentFiltered(toolRow))

    val filtered =
      """{
        |"id":"r","object":"response","created_at":"1","model":"m",
        |"output":[{"type":"message","status":"incomplete","content":[]}],
        |"system_fingerprint":null,"usage":null,
        |"incomplete_details":{"reason":"content_filter"}
        |}""".stripMargin
    val filteredRow = responseFrame(filtered).collect().head
    assert(transformer.isContentFiltered(filteredRow))
    assert(transformer.getFilterReason(filteredRow) === "content_filter")

    val maxed = filtered.replace("content_filter", "max_output_tokens")
    assert(!transformer.isContentFiltered(responseFrame(maxed).collect().head))
  }

  test("toolCallsColumn and replayItemsColumn are structured and deterministic") {
    val callsDf = wrappedResponse(ToolTestFixtures.MixedResponseJson)
      .select(OpenAIToolColumns.toolCallsColumn("out").as("calls"))
    val calls = callsDf.collect().head.getAs[Seq[Row]]("calls")
    assert(calls.size === 1)
    assert(calls.head === Row("call_a", "fc_1", "function_call", "get_weather",
      """{"city":"Seattle"}""", "completed", 0))
    assert(callsDf.schema("calls").dataType === OpenAIToolColumns.ToolCallStructType)

    val oldSetting = spark.conf.getOption("spark.sql.jsonGenerator.ignoreNullFields")
    try {
      Seq("true", "false").foreach { setting =>
        spark.conf.set("spark.sql.jsonGenerator.ignoreNullFields", setting)
        val replay = wrappedResponse(ToolTestFixtures.MixedResponseJson)
          .select(OpenAIToolColumns.replayItemsColumn("out").as("items"))
          .collect().head.getString(0).parseJson.asInstanceOf[JsArray]
        assert(replay.elements.size === 3)
        assert(!replay.elements.exists(_.asJsObject.fields.get("id").contains(
          JsString("rs_drop"))))
        assert(replay.elements.exists(_.asJsObject.fields.get("id").contains(
          JsString("rs_keep"))))
        assert(replay.elements.forall(_.asJsObject.fields.values.forall(_ != JsNull)))
      }
    } finally {
      oldSetting match {
        case Some(value) => spark.conf.set("spark.sql.jsonGenerator.ignoreNullFields", value)
        case None => spark.conf.unset("spark.sql.jsonGenerator.ignoreNullFields")
      }
    }
  }

  test("driver validation rejects invalid continuation and output configurations") {
    val messagesSchema = StructType(Seq(
      StructField("messages", ToolTestFixtures.MessagesArrayType),
      StructField("items", StringType),
      StructField("outputs", OpenAIToolColumns.FunctionCallOutputStructType),
      StructField("response_id", StringType)
    ))

    assertThrows[IllegalArgumentException] {
      new OpenAIResponses().setToolChoice("required")
        .setMessagesCol("messages").transformSchema(messagesSchema)
    }
    assertThrows[IllegalArgumentException] {
      new OpenAIResponses()
        .setTools(Seq(ToolTestFixtures.WeatherToolMap))
        .setToolChoiceFunction("unknown")
        .setMessagesCol("messages")
        .transformSchema(messagesSchema)
    }
    assertThrows[IllegalArgumentException] {
      new OpenAIResponses()
        .setFunctionCallOutputsCol("outputs")
        .setTools(ToolTestFixtures.WeatherToolJson)
        .transformSchema(messagesSchema)
    }
    assertThrows[IllegalArgumentException] {
      new OpenAIResponses()
        .setFunctionCallOutputsCol("outputs")
        .setPreviousResponseIdCol("response_id")
        .transformSchema(messagesSchema)
    }
    assertThrows[IllegalArgumentException] {
      new OpenAIResponses()
        .setInputItemsCol("items")
        .setFunctionCallOutputsCol("outputs")
        .setTools(ToolTestFixtures.WeatherToolJson)
        .transformSchema(messagesSchema)
    }
    assertThrows[IllegalArgumentException] {
      new OpenAIResponses()
        .setMessagesCol("items")
        .setInputItemsCol("items")
        .transformSchema(messagesSchema)
    }
    assertThrows[IllegalArgumentException] {
      configured.setToolCallsCol("messages").transformSchema(messagesDf.schema)
    }
    assertThrows[IllegalArgumentException] {
      configured
        .setOutputCol("out")
        .setToolCallsCol("out")
        .transformSchema(messagesDf.schema)
    }
  }

  test("continuation schema validation ignores nullability but rejects wrong fields") {
    val validElement = StructType(Seq(
      StructField("call_id", StringType, nullable = false),
      StructField("output", StringType, nullable = false),
      StructField("status", StringType, nullable = true)
    ))
    val validSchema = StructType(Seq(
      StructField("outputs", ArrayType(validElement, containsNull = false)),
      StructField("response_id", StringType)
    ))
    new OpenAIResponses()
      .setFunctionCallOutputsCol("outputs")
      .setPreviousResponseIdCol("response_id")
      .setTools(ToolTestFixtures.WeatherToolJson)
      .transformSchema(validSchema)

    val invalidSchema = StructType(Seq(
      StructField("outputs", ArrayType(StructType(Seq(
        StructField("id", StringType),
        StructField("output", StringType)
      )))),
      StructField("response_id", StringType)
    ))
    assertThrows[IllegalArgumentException] {
      new OpenAIResponses()
        .setFunctionCallOutputsCol("outputs")
        .setPreviousResponseIdCol("response_id")
        .setTools(ToolTestFixtures.WeatherToolJson)
        .transformSchema(invalidSchema)
    }
  }

  test("current endpoint compatibility checks warn without crashing") {
    val schema = StructType(Seq(
      StructField("messages", ToolTestFixtures.MessagesArrayType),
      StructField("response_id", StringType),
      StructField("items", StringType),
      StructField("outputs", OpenAIToolColumns.FunctionCallOutputStructType)
    ))
    new OpenAIResponses()
      .setMessagesCol("messages")
      .setServiceTier("flex")
      .transformSchema(schema)
    new OpenAIResponses()
      .setUrl("https://example.openai.azure.com/")
      .setMessagesCol("messages")
      .setServiceTier("flex")
      .setMaxToolCalls(3)
      .setInclude(Seq("reasoning.encrypted_content"))
      .transformSchema(schema)
    new OpenAIResponses()
      .setMessagesCol("messages")
      .setPreviousResponseIdCol("response_id")
      .setStore(false)
      .transformSchema(schema)
    new OpenAIResponses()
      .setMessagesCol("messages")
      .setInputItemsCol("items")
      .setFunctionCallOutputsCol("outputs")
      .setTools(ToolTestFixtures.WeatherToolJson)
      .setStore(true)
      .transformSchema(schema)
  }

  test("new params survive save and load without initialization hazards") {
    val transformer = configured
      .addFunctionTool("get_forecast", "forecast", ToolTestFixtures.EmptyObjectSchema)
      .addFunctionTool("get_alerts", "alerts", ToolTestFixtures.EmptyObjectSchema)
      .setParallelToolCalls(false)
      .setMaxToolCalls(2)
      .setMetadata(Map("k" -> "v"))
      .setInclude(Seq("reasoning.encrypted_content"))
      .setToolCallsCol("calls")
    val path = tmpDir.resolve("responses-tools").toString
    transformer.write.overwrite().save(path)
    val loaded = OpenAIResponses.load(path)
    assert(OpenAIToolUtils.parseTools(loaded.getTools).elements.flatMap(
      OpenAIToolUtils.functionName) ===
      Seq("get_weather", "get_forecast", "get_alerts"))
    assert(loaded.getToolChoice === "auto")
    assert(!loaded.getParallelToolCalls)
    assert(loaded.getMaxToolCalls === 2)
    assert(loaded.getMetadata === Map("k" -> "v"))
    assert(loaded.getInclude === Seq("reasoning.encrypted_content"))
    assert(loaded.getToolCallsCol === "calls")

    val empty = new OpenAIResponses()
    assert(empty.sharedTextParams.forall(_ != null))
    val names = empty.sharedTextParams.map(_.name).toSet
    assert(names.contains("parallelToolCalls"))
    assert(names.contains("conversation"))
    assert(!names.contains("tools"))
    assert(!names.contains("toolChoice"))
    assert(!names.contains("reasoningSummary"))
    assert(empty.getOptionalParams(ToolTestFixtures.dynamicRow()).contains("store"))
  }

  test("generated Python exposes ergonomic tool and continuation methods") {
    val generated = new OpenAIResponses().generatedPythonClass
    Seq(
      "def setTools(",
      "def setToolsCol(",
      "def setToolChoice(",
      "def setParallelToolCalls(",
      "def setMaxToolCalls(",
      "def setInputItemsCol(",
      "def setFunctionCallOutputsCol(",
      "def setToolCallsCol(",
      "def getToolsAsList(",
      "def replayItemsColumn("
    ).foreach(method => assert(generated.contains(method), method))
    assert(generated.contains("mode = self._java_obj.getToolsParamMode()"))
    assert(generated.contains("tools is column-bound to {!r}"))
    assert(!generated.contains("self.isSet(self.tools)"))
    assert(generated.contains("tools.append({"))
    assert(generated.contains("return self.setTools(tools)"))
    assert(generated.contains(
      "self._java_obj.toolCallsColumn(outputCol or self.getOutputCol())"))
  }

  override def testObjects(): Seq[TestObject[OpenAIResponses]] =
    Seq(new TestObject(configured, messagesDf))

  override def reader: MLReadable[_] = OpenAIResponses
}
