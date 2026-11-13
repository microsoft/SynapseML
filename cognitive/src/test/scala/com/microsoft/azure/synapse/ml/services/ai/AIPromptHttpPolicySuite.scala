// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{MapType, StringType, StructType}

import scala.collection.JavaConverters._
import scala.util.Using

private[ai] class TestableFramingTokenAIPrompt(override val uid: String) extends AIPrompt(uid) {
  def this() = this(Identifiable.randomUID("TestableFramingTokenAIPrompt"))
  override protected val aadHeaderName: String = "cOnTeNt-LeNgTh"
}

private[ai] class TestableFramingKeyAIPrompt(override val uid: String) extends AIPrompt(uid) {
  def this() = this(Identifiable.randomUID("TestableFramingKeyAIPrompt"))
  override protected val subscriptionKeyHeaderName: String = "tRaNsFeR-EnCoDiNg"
}

class AIPromptHttpPolicySuite extends TestBase {
  private val chat = """{"choices":[{"index":0,"message":{"role":"assistant","content":"caf\u00e9"}}]}"""
  private def input = spark.createDataFrame(Seq(Row("hello")).asJava, new StructType().add("text", StringType))

  private def stage(url: String): AIPrompt = new AIPrompt().setUrl(url + "/v1").setModel("model")
    .setInputCol("text").setOutputCol("answer").setErrorCol("error").setRawResponseCol("raw")

  test("caller framing headers fail scalar preflight or individual rows without blocking other headers") {
    val forbidden = Seq("Content-Length" -> "0", "cOnTeNt-LeNgTh" -> "123",
      "Transfer-Encoding" -> "chunked", "tRaNsFeR-EnCoDiNg" -> "identity")
    val preserved = Map("Host" -> "proxy.example.invalid", "Connection" -> "close",
      "Expect" -> "100-continue", "TE" -> "trailers", "Upgrade" -> "example", "X-Proxy" -> "retained")
    val schema = new StructType().add("text", StringType).add("headers", MapType(StringType, StringType))
    val data = spark.createDataFrame((forbidden.map(header => Row("bad", Map(header))) :+
      Row("good", preserved)).asJava, schema)
    Seq(false, true).foreach { telemetry =>
      Using.resource(new AIRecordingServer((_, _) => 200 -> chat)) { server =>
        forbidden.foreach { header =>
          val configured = stage(server.url)
          configured.setScalarParam(if (telemetry) configured.telemHeaders else configured.customHeaders, Map(header))
          assert(intercept[IllegalArgumentException](configured.transformSchema(input.schema))
            .getMessage.contains("HTTP framing headers"))
          intercept[IllegalArgumentException](configured.transform(input))
          assert(server.requests.isEmpty)
        }
        val configured = stage(server.url)
        configured.setVectorParam(if (telemetry) configured.telemHeaders else configured.customHeaders, "headers")
        val rows = configured.transform(data).collect()
        rows.filter(_.getAs[String]("text") == "bad").foreach { row =>
          val diagnostic = row.getAs[Row]("error").getString(0)
          assert(diagnostic.contains("invalid_request") && diagnostic.contains("HTTP framing headers"))
        }
        assert(rows.find(_.getAs[String]("text") == "good").get.getAs[Row]("error") == null)
        assert(server.requests.size() == 1)
        preserved.foreach { case (name, value) =>
          assert(server.headers.peek()(name.toLowerCase(java.util.Locale.ROOT)) == value)
        }
      }
    }
  }

  test("authentication framing hooks fail scalar preflight before HTTP") {
    val cases: Seq[() => AIPrompt] = Seq(
      () => new TestableFramingTokenAIPrompt().setAADToken("synthetic-token"),
      () => new TestableFramingTokenAIPrompt().setCustomAuthHeader("synthetic-custom"),
      () => new TestableFramingTokenAIPrompt().setApiType("decisions").setSubscriptionKey("synthetic-key")
        .setQuestions(NativeRequestFixtures.DecisionQuestions),
      () => new TestableFramingKeyAIPrompt().setSubscriptionKey("synthetic-key"))
    Using.resource(new AIRecordingServer((_, _) => 200 -> chat)) { server =>
      cases.foreach { create =>
        val configured = create().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
        Seq[() => Any](() => configured.transformSchema(input.schema), () => configured.transform(input))
          .foreach { invoke =>
            val error = intercept[IllegalArgumentException](invoke())
            assert(error.getMessage.contains("HTTP framing headers") && error.getCause == null)
            assert(!error.getMessage.contains("synthetic-"))
            assert(server.requests.isEmpty)
          }
      }
    }
  }

  test("authentication framing hooks are checked only after credential selection and bearer conversion") {
    val data = spark.createDataFrame(Seq(Row("valid", "synthetic-key"),
      Row("fallback", Option.empty[String].orNull)).asJava,
      new StructType().add("text", StringType).add("key", StringType))
    Using.resource(new AIRecordingServer((_, _) => 200 -> chat)) { server =>
      val configured = new TestableFramingTokenAIPrompt().setUrl(server.url + "/v1").setModel("model")
        .setInputCol("text").setOutputCol("answer").setErrorCol("error")
        .setSubscriptionKeyCol("key").setAADToken("synthetic-token")
      assert(configured.transformSchema(data.schema).fieldNames.contains("answer"))
      val rows = configured.transform(data).collect()
      assert(rows.find(_.getAs[String]("text") == "valid").get.getAs[Row]("error") == null)
      val rejected = rows.find(_.getAs[String]("text") == "fallback").get
      val diagnostic = rejected.getAs[Row]("error").getString(0)
      assert(diagnostic.contains("invalid_request") && diagnostic.contains("HTTP framing headers"))
      assert(!diagnostic.contains("synthetic-token") && rejected.getAs[String]("answer") == null)
      assert(server.requests.size() == 1 && server.headers.peek()("api-key") == "synthetic-key")
    }
    Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
      val converted = new TestableFramingKeyAIPrompt().setUrl(server.url + "/v1").setApiType("decisions")
        .setModel("model").setInputCol("text").setErrorCol("error").setSubscriptionKey("synthetic-key")
        .setQuestions(NativeRequestFixtures.DecisionQuestions)
      assert(converted.transform(input).head().getAs[Row]("error") == null)
      assert(server.requests.size() == 1 && server.headers.peek()("authorization") == "Bearer synthetic-key")
    }
  }
}
