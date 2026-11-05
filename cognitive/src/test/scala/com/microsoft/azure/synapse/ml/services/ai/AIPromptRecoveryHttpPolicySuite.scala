// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.HTTPSchema
import com.microsoft.azure.synapse.ml.services.openai.OpenAIDecisions
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{MapType, StringType, StructType}
import spray.json._

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.collection.JavaConverters._
import scala.util.Using

class AIPromptRecoveryHttpPolicySuite extends AIPromptOutputTestBase {
  private val chat = """{"choices":[{"index":0,"message":{"role":"assistant","content":"caf\u00e9"}}]}"""
  private def input = spark.createDataFrame(Seq(Row("hello")).asJava, new StructType().add("text", StringType))

  private def stage(url: String): AIPrompt = new AIPrompt().setUrl(url + "/v1").setModel("model")
    .setInputCol("text").setOutputCol("answer").setErrorCol("error").setRawResponseCol("raw")
  test("authentication framing hooks fail scalar preflight before HTTP or durable storage") {
    val cases: Seq[() => AIPrompt] = Seq(
      () => new TestableFramingTokenAIPrompt().setAADToken("synthetic-token"),
      () => new TestableFramingTokenAIPrompt().setCustomAuthHeader("synthetic-custom"),
      () => new TestableFramingTokenAIPrompt().setApiType("decisions").setSubscriptionKey("synthetic-key")
        .setQuestions(NativeRequestFixtures.DecisionQuestions),
      () => new TestableFramingKeyAIPrompt().setSubscriptionKey("synthetic-key"))
    Using.resource(new AIRecordingServer((_, _) => 200 -> chat)) { server =>
      val root = Files.createTempDirectory("ai-auth-framing-")
      try {
        cases.foreach { create =>
          Seq(true).foreach { durable =>
            val configured = create().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
            if (durable) configured.setOutputPath(root.resolve("run").toString)
            Seq[() => Any](() => configured.transformSchema(input.schema), () => configured.transform(input))
              .foreach { invoke =>
                val error = intercept[IllegalArgumentException](invoke())
                assert(error.getMessage.contains("HTTP framing headers") && error.getCause == null)
                assert(!error.getMessage.contains("synthetic-"))
            }
            assert(!Files.exists(root.resolve("run")) && server.requests.isEmpty)
          }
        }
      } finally org.apache.commons.io.FileUtils.deleteDirectory(root.toFile)
    }
  }
}
