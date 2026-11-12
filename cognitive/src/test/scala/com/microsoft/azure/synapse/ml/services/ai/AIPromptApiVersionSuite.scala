// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.io.http.HTTPSchema
import com.microsoft.azure.synapse.ml.param.GlobalParams
import com.microsoft.azure.synapse.ml.services.OpenAIApiVersion
import com.microsoft.azure.synapse.ml.services.openai.OpenAIDecisions
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType}
import spray.json.JsString

import scala.collection.JavaConverters._
import scala.util.Using

class AIPromptApiVersionSuite extends AIPromptTestBase {
  private val inputSchema = new StructType().add("text", StringType)
  private val cases = Seq(
    ("chat_completions", "chat/completions",
      """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""),
    ("responses", "responses",
      """{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}"""))

  private def datedRoute(api: String, route: String): String =
    if (api == "responses") "/openai/responses" else s"/openai/deployments/model/$route"

  cases.foreach { case (api, route, reply) =>
    test(s"$api defaults to v1 through copy save load and clearing an override") {
      val previous = GlobalParams.getGlobalParam(OpenAIApiVersion)
      val data = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
      try {
        GlobalParams.resetGlobalParam(OpenAIApiVersion)
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          val stage = new AIPrompt().setApiType(api).setUrl(server.url).setDeploymentName("model")
            .setInputCol("text").setOutputCol("answer").setErrorCol("error")
          val saved = path()
          stage.write.session(spark).save(saved)
          Seq(stage, stage.copy(ParamMap.empty), AIPrompt.load(saved)).foreach { candidate =>
            val row = candidate.transform(data).head()
            assert(row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null)
            assert(server.requests.asScala.last._1 == s"/openai/v1/$route")
            candidate.setApiVersion("preview")
            candidate.clear(candidate.apiVersion)
            assert(candidate.transform(data).head().getAs[Row]("error") == null)
            assert(server.requests.asScala.last._1 == s"/openai/v1/$route")
            assert(!candidate.isSet(candidate.apiVersion))
          }
        }
      } finally {
        previous match {
          case Some(value) => GlobalParams.setGlobalParam(OpenAIApiVersion, value)
          case None => GlobalParams.resetGlobalParam(OpenAIApiVersion)
        }
      }
    }

    test(s"$api API version selects v1 or forwards arbitrary query versions") {
      val data = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val stage = new AIPrompt().setApiType(api).setUrl(server.url).setDeploymentName("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error")
        val versions = Seq("v1" -> s"/openai/v1/$route",
          "preview" -> (datedRoute(api, route) + "?api-version=preview"),
          "2025-04-01" -> (datedRoute(api, route) + "?api-version=2025-04-01"),
          "future&option=1" -> (datedRoute(api, route) + "?api-version=future%26option%3D1"))
        versions.foreach { case (version, expected) =>
          val row = stage.setApiVersion(version).transform(data).head()
          assert(row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null)
          val (address, body) = server.requests.asScala.last
          assert(address == expected)
          assert(AIJson.objectString(body).fields("model") == JsString("model"))
          assert(!AIJson.objectString(body).fields.contains("api-version"))
        }
      }
    }

    test(s"$api default v1 version normalizes known endpoint forms without changing stored URLs") {
      val data = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
      val endpoints = Seq(
        "" -> s"/openai/v1/$route",
        "/openai" -> s"/openai/v1/$route",
        datedRoute(api, route) -> s"/openai/v1/$route",
        "/proxy" -> s"/proxy/openai/v1/$route",
        ("/proxy" + datedRoute(api, route)) -> s"/proxy/openai/v1/$route",
        "/v1" -> s"/v1/$route",
        "/openai/V1" -> s"/openai/V1/$route",
        s"/openai/V1/$route" -> s"/openai/V1/$route")
      endpoints.foreach { case (suffix, expected) =>
        val endpoint = "https://resource.openai.azure.com" + suffix + "///"
        val stage = new AIPrompt().setApiType(api).setUrl(endpoint).setDeploymentName("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error")
          .setHandler { (_, request) =>
            assert(request.requestLine.uri == "https://resource.openai.azure.com" + expected)
            HTTPSchema.stringToResponse(reply, 200, "OK")
          }
        assert(stage.transformSchema(inputSchema) == stage.transform(data).schema)
        assert(stage.transform(data).head().getAs[Row]("error") == null && stage.getUrl == endpoint)
      }
    }

    test(s"$api version columns preserve routing through copy and save load") {
      val schema = inputSchema.add("version", StringType)
      val data = spark.createDataFrame(Seq(
        Row("one", "v1"), Row("two", "preview"), Row("three", "2025-04-01"),
        Row("missing", Option.empty[String].orNull)).asJava, schema)
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val stage = new AIPrompt().setApiType(api).setUrl(server.url).setDeploymentName("model")
          .setInputCol("text").setApiVersion("v1").setApiVersionCol("version")
          .setOutputCol("answer").setErrorCol("error")
        val saved = path()
        stage.write.session(spark).save(saved)
        Seq(stage, stage.copy(ParamMap.empty), AIPrompt.load(saved)).foreach { candidate =>
          val rows = candidate.transform(data.coalesce(1)).collect()
          assert(rows.take(3).forall(_.getAs[Row]("error") == null))
          assert(rows.last.getAs[Row]("error").getString(0).contains("missing_input"))
          assert(server.requests.size() == 3)
          assert(server.requests.asScala.map(_._1).toSet == Set(s"/openai/v1/$route",
            datedRoute(api, route) + "?api-version=preview",
            datedRoute(api, route) + "?api-version=2025-04-01"))
          server.requests.clear()
        }
        stage.setApiVersion("v1").transform(data.limit(1)).collect()
        assert(server.requests.peek()._1 == s"/openai/v1/$route")
      }
    }
  }

  test("version columns defer deployment path validation until the row selects its route") {
    val schema = inputSchema.add("version", StringType)
    val data = spark.createDataFrame(Seq(Row("one", "v1"), Row("two", "preview")).asJava, schema)
    Using.resource(new AIRecordingServer((_, _) => 200 -> cases.head._3)) { server =>
      val stage = new AIPrompt().setUrl(server.url).setDeploymentName(".").setApiVersionCol("version")
        .setInputCol("text").setOutputCol("answer").setErrorCol("error")
      assert(stage.transformSchema(schema) == stage.transform(data).schema)
      val rows = stage.transform(data.coalesce(1)).collect()
      assert(rows.head.getAs[String]("answer") == "answer" && rows.head.getAs[Row]("error") == null)
      assert(rows.last.getAs[Row]("error").getString(0).contains("invalid_request"))
      assert(server.requests.size() == 1 && server.requests.peek()._1 == "/openai/v1/chat/completions")
    }
  }

  test("global versions override the fallback without mutating stage parameters") {
    val previous = GlobalParams.getGlobalParam(OpenAIApiVersion)
    val data = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
    try {
      for (version <- Seq("v1", "preview"); (api, route, reply) <- cases) {
        GlobalParams.setGlobalParam(OpenAIApiVersion, Left(version))
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          val stage = new AIPrompt().setApiType(api).setUrl(server.url).setDeploymentName("model")
            .setInputCol("text").setErrorCol("error")
          assert(stage.transform(data).head().getAs[Row]("error") == null)
          val expected = if (version == "v1") s"/openai/v1/$route"
            else datedRoute(api, route) + "?api-version=preview"
          assert(server.requests.peek()._1 == expected)
          assert(!stage.isSet(stage.apiVersion))
        }
      }
    } finally {
      previous match {
        case Some(value) => GlobalParams.setGlobalParam(OpenAIApiVersion, value)
        case None => GlobalParams.resetGlobalParam(OpenAIApiVersion)
      }
    }
  }

  test("Decisions defaults to v1 and forwards other scalar and column versions without an allowlist") {
    val data = spark.createDataFrame(Seq(Row("hello")).asJava, inputSchema)
    Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
      Seq(new AIPrompt().setApiType("decisions"), new OpenAIDecisions()).foreach { stage =>
        stage.setUrl(server.url).setModel("model").setInputCol("text")
          .setQuestions(NativeRequestFixtures.DecisionQuestions).setOutputCol("answer").setErrorCol("error")
        assert(stage.effectiveCopy().getApiVersion == "v1")
        val saved = path()
        stage.write.session(spark).save(saved)
        val restored = AIPrompt.load(saved)
        assert(restored.effectiveCopy().getApiVersion == "v1")
        assert(restored.transform(data).head().getAs[Row]("error") == null)
        assert(server.requests.asScala.last._1 == "/v1/decisions")
        Seq("v1" -> "/v1/decisions", "v2" -> "/v1/decisions?api-version=v2",
          "preview" -> "/v1/decisions?api-version=preview").foreach { case (version, expected) =>
          assert(stage.setApiVersion(version).transform(data).head().getAs[Row]("error") == null)
          assert(server.requests.asScala.last._1 == expected)
        }
        assert(stage.setApiVersionCol("text").transform(data).head().getAs[Row]("error") == null)
        assert(server.requests.asScala.last._1 == "/v1/decisions?api-version=hello")
      }
    }
  }
}
