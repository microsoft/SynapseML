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

class AIRecoveryJsonSuite extends AIPromptOutputTestBase {
  private val missing: Null = None.orNull
  private val chatReply = """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
  test("JSON-valued defaults preserve exact numbers explicitness and durable fingerprints through save load") {
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
        val output = stage.transformSchema(data.schema)
        assert(AIPromptOutput.fingerprint(stage, data.schema, output, spark) ==
          AIPromptOutput.fingerprint(loaded, data.schema, output, spark))
        assert(loaded.transform(data).head().getAs[String]("answer") == "answer")
        assert(AIJson.objectString(server.requests.peek()._2).fields("value") ==
          expected.asJsObject.fields("value"))
      } finally {
        org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File(directory).getParentFile)
      }
    }
  }

  test("extraBody preserves top-level order through setters persistence HTTP and durable fingerprints") {
    val data = spark.range(1).select(lit("hello").as("text"))
    val names = Seq("e_one", "d_two", "c_three", "b_four", "a_five", "z_six")
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply)) { server =>
      val stage = new AIPrompt().setUrl(server.url + "/v1").setDeploymentName("explicit-model")
        .setInputCol("text").setOutputCol("answer").setErrorCol("error")
      val fingerprints = Seq(names, names.reverse).map { order =>
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
          val output = stage.transformSchema(data.schema)
          val fingerprint = AIPromptOutput.fingerprint(stage, data.schema, output, spark)
          assert(AIPromptOutput.fingerprint(loaded, data.schema, output, spark) == fingerprint)
          assert(loaded.transform(data).head().getAs[String]("answer") == "answer")
          val wireKeys = AIJson.objectString(server.requests.asScala.last._2).fields.keys.toSeq
          assert(wireKeys.filter(names.contains) == order)
          fingerprint
        } finally {
          org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File(directory).getParentFile)
        }
      }
      assert(fingerprints.distinct.size == 2)
    }
  }
}
