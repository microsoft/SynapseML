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

class AIPromptRecoverySuite extends AIPromptOutputTestBase {
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
  test("explicit model bindings ignore unused global deployment columns and scalar values") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> embeddingReply)) { server =>
      val previous = GlobalParams.getGlobalParam(OpenAIDeploymentNameKey)
      val stage = prompt(server, "embeddings")
      stage.clear(stage.deploymentName)
      stage.setModel("explicit-model")
      try {
        GlobalParams.resetGlobalParam(OpenAIDeploymentNameKey)
        val original = stage.effectiveCopy()
        val fingerprint = AIPromptOutput.fingerprint(original, input.schema,
          original.transformSchema(input.schema), spark)
        Seq(Right("absent-global-column"), Left("")).foreach { default =>
          GlobalParams.setGlobalParam(OpenAIDeploymentNameKey, default)
          val effective = stage.effectiveCopy()
          assert(AIPromptOutput.fingerprint(effective, input.schema,
            effective.transformSchema(input.schema), spark) == fingerprint)
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
  test("JSON-valued body parameters preserve exact numbers and JVM arrays through save load and fingerprinting") {
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
        assert(AIPromptOutput.fingerprint(stage, input.schema, output, spark) ==
          AIPromptOutput.fingerprint(loaded, input.schema, output, spark))
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
}
