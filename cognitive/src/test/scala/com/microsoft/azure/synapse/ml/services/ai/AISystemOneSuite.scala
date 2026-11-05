// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.core.test.fuzzing.TestObject
import com.microsoft.azure.synapse.ml.services.openai.OpenAIDecisions
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType}
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.JavaConverters._
import scala.util.Using

class AISystemOneSuite extends AIPromptFuzzing[AISystemOne] {
  override def reader: MLReadable[_] = AISystemOne

  override def testObjects(): Seq[TestObject[AISystemOne]] = Seq(new TestObject(
    new AISystemOne("systemone-fuzzing").setUrl("https://example.invalid").setDeploymentName("synthetic-model")
      .setSubscriptionKey("synthetic-key").setStateCol("state").setQuestions(NativeRequestFixtures.SystemQuestions)
      .setOutputCol("decision").setErrorCol("error"),
    spark.createDataFrame(Seq(Row(Option.empty[String].orNull)).asJava, inputSchema)))

  private val inputSchema = new StructType().add("state", StringType)
  private val inputText = "Checkout is down again. This is my third request."
  private def input = spark.createDataFrame(Seq(Row(inputText)).asJava, inputSchema)

  test("direct System One and facade use the native route and retain every answer JSON value") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.SystemResponse)) { server =>
      Seq(new AISystemOne(), new AIPrompt().setApiType("systemone")).foreach { stage =>
        stage.setUrl(server.url).setDeploymentName("Decision-1").setStateCol("state")
          .setQuestions(NativeRequestFixtures.SystemQuestions).setOutputCol("decision").setErrorCol("error")
          .setUsageCol("usage").setRawResponseCol("raw")
        val result = stage.transform(input)
        assert(result.schema == stage.transformSchema(input.schema))
        val row = result.head()
        assert(row.getAs[Row]("error") == null)
        val decision = row.getAs[Row]("decision")
        assert(decision.getAs[String]("model") == "microsoft-decision-1")
        val answers = decision.getAs[Map[String, String]]("answers")
        assert(answers("repeat_contact") == "true")
        assert(answers("severity") == "1.7")
        assert(AIJson.parse(answers("team")).asJsObject.fields("additional") ==
          JsArray(JsNumber(1), JsBoolean(false), JsNull))
        assert(row.getAs[String]("raw").contains("future"))
      }
      val expected = JsObject("model" -> JsString("Decision-1"), "state" -> JsString(inputText),
        "questions" -> NativeRequestFixtures.SystemQuestions.parseJson)
      assert(server.requests.size() == 2 && server.requests.asScala.forall { case (path, body) =>
        path == "/providers/microsoft/v1/systemone" && body.parseJson == expected
      })
    }
  }

  test("JSON state interpretation and question columns remain native JSON rather than escaped text") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.SystemResponse)) { server =>
      val data = spark.createDataFrame(Seq(Row("""{"message":"outage","count":3}""",
        NativeRequestFixtures.SystemQuestions)).asJava, inputSchema.add("questions", StringType))
      val stage = new AISystemOne().setUrl(server.url).setDeploymentName("Decision-1")
        .setStateCol("state").setStateMode("json").setQuestionsCol("questions")
      stage.transform(data).collect()
      assert(AIJson.objectString(server.requests.peek()._2).fields("state").isInstanceOf[JsObject])
      assert(AIJson.objectString(server.requests.peek()._2).fields("questions").isInstanceOf[JsObject])
    }
  }

  test("direct Decisions and facade preserve question order and typed native answers") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> NativeRequestFixtures.DecisionResponse)) { server =>
      Seq(new OpenAIDecisions(), new AIPrompt().setApiType("decisions")).foreach { stage =>
        stage.setUrl(server.url + "/v1").setModel("gpt-6-luna").setInputCol("state")
          .setQuestions(NativeRequestFixtures.DecisionQuestions).setSafetyIdentifier("synthetic-test")
          .setOutputCol("decision").setErrorCol("error")
        val result = stage.transform(input)
        assert(result.schema == stage.transformSchema(input.schema))
        val row = result.head()
        assert(row.getAs[Row]("error") == null)
        val decision = row.getAs[Row]("decision")
        val answers = decision.getSeq[Row](decision.fieldIndex("answers"))
        assert(answers.map(_.getAs[String]("name")) == Seq("repeat_contact", "team", "severity"))
        assert(answers(1).getAs[Row]("choice").getAs[String]("stringValue") == "engineering")
        assert(answers(2).getSeq[Row](answers(2).fieldIndex("scoreProbabilities")).last.getAs[Int]("value") == 2)
      }
      val expected = JsObject("model" -> JsString("gpt-6-luna"), "input" -> JsString(inputText),
        "questions" -> NativeRequestFixtures.DecisionQuestions.parseJson,
        "safety_identifier" -> JsString("synthetic-test"))
      assert(server.requests.size() == 2 && server.requests.asScala.forall { case (path, body) =>
        path == "/v1/decisions" && body.parseJson == expected
      })
    }
  }

  test("direct Decisions and facade preserve exact score bounds as terminal parse errors") {
    val scores = Seq("0", "2", "-1e-400", "2.0000000000000000001")
    val data = spark.createDataFrame(scores.map(Row(_)).asJava, inputSchema)
    def response(score: String): String =
      NativeRequestFixtures.DecisionResponse.replace("\"score\":1.7", s""""score":$score""")
    Using.resource(new AIRecordingServer((_, body) =>
      200 -> response(AIJson.objectString(body).fields("input").convertTo[String]))) { server =>
      Seq(new OpenAIDecisions(), new AIPrompt().setApiType("decisions")).foreach { stage =>
        stage.setUrl(server.url + "/v1").setModel("model").setInputCol("state")
          .setQuestions(NativeRequestFixtures.DecisionQuestions).setOutputCol("answer").setErrorCol("error")
          .setRawResponseCol("raw").setUsageCol("usage")
        val result = stage.transform(data)
        assert(result.schema == stage.transformSchema(data.schema))
        val rows = result.collect()
        assert(rows.length == scores.size)
        rows.foreach { row =>
          val score = row.getAs[String]("state")
          assert(row.getAs[String]("raw") == response(score))
          assert(row.getAs[Row]("usage").getAs[Long]("total_tokens") == 20L)
          if (score == "0" || score == "2") {
            assert(row.getAs[Row]("error") == null)
            assert(row.getAs[Row]("answer").getAs[Seq[Row]]("answers")(2).getAs[Double]("score") == score.toDouble)
          } else {
            assert(row.getAs[Row]("answer") == null)
            assert(row.getAs[Row]("error").getString(0).parseJson.asJsObject.fields("code") == JsString("parse_error"))
          }
        }
      }
      assert(server.requests.size() == scores.size * 2)
    }
  }

  Seq(
    ("systemone", () => new AISystemOne(), NativeRequestFixtures.SystemQuestions,
      NativeRequestFixtures.SystemResponse, "/providers/microsoft/v1/systemone", "state"),
    ("decisions", () => new OpenAIDecisions(), NativeRequestFixtures.DecisionQuestions,
      NativeRequestFixtures.DecisionResponse, "/v1/decisions", "input")
  ).foreach { case (api, create, questions, reply, route, inputKey) =>
    val expected = JsObject("model" -> JsString("synthetic-model"), inputKey -> JsString(inputText),
      "questions" -> questions.parseJson)
    val serviceError = """{"error":{"code":"synthetic_error","message":"Synthetic native service failure"}}"""

    test(s"$api protocol mock checks native requests authentication and transient retries") {
      Seq(create(), new AIPrompt().setApiType(api)).foreach { stage =>
        Using.resource(new AIRecordingServer((count, body) => {
          if (body.parseJson != expected) 400 -> serviceError
          else if (count == 1) 503 -> serviceError
          else 200 -> reply
        })) { server =>
          stage.setUrl(server.url).setModel("synthetic-model").setInputCol("state").setQuestions(questions)
            .setSubscriptionKey("synthetic-key").setOutputCol("answer").setErrorCol("error").setConcurrency(1)
          val result = stage.transform(input)
          assert(result.schema == stage.transformSchema(input.schema))
          assert(result.head().getAs[Row]("error") == null)
          assert(server.requests.size() == 2 && server.requests.asScala.forall { case (path, body) =>
            path == route && body.parseJson == expected
          })
          val auth = if (api == "systemone") Some("synthetic-key") -> None
            else None -> Some("Bearer synthetic-key")
          assert(server.headers.asScala.forall { headers =>
            (headers.get("api-key"), headers.get("authorization")) == auth
          })
        }
      }
    }

    test(s"$api protocol mock preserves terminal HTTP and malformed response diagnostics without retries") {
      Seq((400, serviceError, "http_error"), (200, "{}", "parse_error")).foreach {
        case (status, response, code) =>
          Seq(create(), new AIPrompt().setApiType(api)).foreach { stage =>
            Using.resource(new AIRecordingServer((_, _) => status -> response)) { server =>
              stage.setUrl(server.url).setModel("synthetic-model").setInputCol("state").setQuestions(questions)
                .setOutputCol("answer").setErrorCol("error").setRawResponseCol("raw")
              val row = stage.transform(input).head()
              assert(row.getAs[Row]("error").getString(0).contains(code) && row.isNullAt(row.fieldIndex("answer")))
              assert(row.getAs[String]("raw") == response && server.requests.size() == 1)
              assert(server.requests.peek()._1 == route && server.requests.peek()._2.parseJson == expected)
            }
          }
      }
    }
  }

  test("native malformed responses are parse errors and do not discard raw JSON") {
    Using.resource(new AIRecordingServer((_, _) => 200 -> """{"model":"Decision-1","answers":[]}""")) { server =>
      val stage = new AISystemOne().setUrl(server.url).setDeploymentName("Decision-1").setStateCol("state")
        .setQuestions(NativeRequestFixtures.SystemQuestions).setErrorCol("error").setRawResponseCol("raw")
      val row = stage.transform(input).head()
      assert(row.getAs[Row]("error").getString(0).contains("parse_error"))
      assert(row.getAs[String]("raw").contains("answers"))
    }
  }
}
