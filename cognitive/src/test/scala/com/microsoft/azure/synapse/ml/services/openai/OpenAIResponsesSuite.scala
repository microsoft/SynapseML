// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.Flaky
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.http.util.EntityUtils
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructField, StructType}
import org.scalactic.Equality
import spray.json._
import spray.json.DefaultJsonProtocol._

class OpenAIResponsesSuite extends TransformerFuzzing[OpenAIResponses]
  with OpenAIAPIKey with Flaky {

  import spark.implicits._

  lazy val responses: OpenAIResponses = new OpenAIResponses()
    .setDeploymentName(deploymentNameGpt4)
    .setCustomServiceName(openAIServiceName)
    .setApiVersion("2025-04-01-preview")
    .setMaxTokens(500)
    .setOutputCol("out")
    .setMessagesCol("messages")
    .setTemperature(0)
    .setSubscriptionKey(openAIAPIKey)

  lazy val goodDf: DataFrame = Seq(
    Seq(
      OpenAIMessage("system", "You are an AI chatbot with red as your favorite color"),
      OpenAIMessage("user", "Whats your favorite color")
    ),
    Seq(
      OpenAIMessage("system", "You are very excited"),
      OpenAIMessage("user", "How are you today")
    )
  ).toDF("messages")

  lazy val badDf: DataFrame = Seq(
    Seq(),
    Seq(
      OpenAIMessage("system", "You are very excited"),
      OpenAIMessage("user", null) // scalastyle:ignore null
    )
  ).toDF("messages")

  test("Basic Usage") {
    testResponses(responses, goodDf)
  }

  test("Robustness to bad inputs") {
    val results = responses.transform(badDf).collect()
    assert(Option(results.head.getAs[Row](responses.getErrorCol)).isDefined)
    assert(Option(results(1).getAs[Row](responses.getErrorCol)).isDefined)
  }

  test("getOptionalParam should include responseFormat") {
    val transformer = new OpenAIResponses()
      .setDeploymentName(deploymentNameGpt4)

    def validate(params: Map[String, Any], expected: String): Unit = {
      val payloadName = responses.responseFormat.payloadName
      assert(params.contains(payloadName))
      val value = params(payloadName).asInstanceOf[Map[String, String]]
      assert(value.get("type").contains(expected))
    }
    val rawMessages = Seq(
      OpenAIMessage("user", "Whats your favorite color")
    )
    val messages: Seq[Row] = rawMessages.toDF("role", "content", "name").collect()

    val optionalParams = transformer.getOptionalParams(messages.head)
    assert(!optionalParams.contains("response_format"))

    transformer.setResponseFormat("json_object")
    val paramsWithJson = transformer.getOptionalParams(messages.head)
    validate(paramsWithJson, "json_object")

    transformer.setResponseFormat(OpenAIResponseFormat.TEXT)
    val paramsWithText = transformer.getOptionalParams(messages.head)
    validate(paramsWithText, "text")
  }

  test("Responses setResponseFormat accepts json_schema Map and rejects bare string") {
    val transformer = new OpenAIResponses()
      .setDeploymentName(deploymentNameGpt4)

    val schemaMap: Map[String, Any] = Map(
      "type" -> "json_schema",
      "json_schema" -> Map(
        "name" -> "answer_schema",
        "strict" -> true,
        "schema" -> Map(
          "type" -> "object",
          "properties" -> Map(
            "answer" -> Map("type" -> "string")
          ),
          "required" -> Seq("answer"),
          "additionalProperties" -> false
        )
      )
    )

    transformer.setResponseFormat(schemaMap)
    val rawMessages = Seq(OpenAIMessage("user", "Whats your favorite color"))
    val messages: Seq[Row] = rawMessages.toDF("role", "content", "name").collect()
    val optionalParams = transformer.getOptionalParams(messages.head)
    val rf = optionalParams(responses.responseFormat.payloadName).asInstanceOf[Map[String, Any]]
    assert(rf("type") == "json_schema")
    val js = rf("json_schema").asInstanceOf[Map[String, Any]]
    assert(js("name") == "answer_schema")

    assertThrows[IllegalArgumentException] {
      transformer.setResponseFormat("json_schema")
    }
  }

  test("setResponseFormat should throw exception if invalid format") {
    val transformer = new OpenAIResponses()
      .setDeploymentName(deploymentNameGpt4)

    assertThrows[IllegalArgumentException] {
      transformer.setResponseFormat("invalid_format")
    }

    assertThrows[IllegalArgumentException] {
      transformer.setResponseFormat(Map("type" -> "invalid_format"))
    }

    assertThrows[IllegalArgumentException] {
      transformer.setResponseFormat(Map("invalid_key" -> "json_object"))
    }
  }

  test("getStringEntity serializes contentParts for multimodal payloads") {
    val transformer = new OpenAIResponses()
    val contentParts = Seq(
      Map("type" -> "input_text", "text" -> "Describe the attachment"),
      Map("type" -> "input_file", "filename" -> "example.pdf", "file_data" -> "data:application/pdf;base64,AAA")
    )
    val messageSchema = StructType(Seq(
      StructField("role", StringType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField(
        "content",
        ArrayType(
          MapType(
            StringType, StringType, valueContainsNull = true
            ), containsNull = false
          ), nullable = true
        )
    ))
    val row = new GenericRowWithSchema(
      Array[Any]("user", null, contentParts), messageSchema // scalastyle:ignore null
    )

    val entity = transformer.getStringEntity(Seq(row), Map.empty)
    val payload = EntityUtils.toString(entity)
    val json = payload.parseJson.asJsObject
    val JsArray(messages) = json.fields("input")
    assert(messages.nonEmpty)
    val JsObject(messageFields) = messages.head
    assert(messageFields.get("content").exists(_.isInstanceOf[JsArray]))
    val JsArray(parts) = messageFields("content")
    assert(parts.length == 2)
    val firstPartType = parts.head.asJsObject.fields("type").convertTo[String]
    val secondPartType = parts(1).asJsObject.fields("type").convertTo[String]
    assert(firstPartType == "input_text")
    assert(secondPartType == "input_file")
  }

  private def testResponses(model: OpenAIResponses,
                            df: DataFrame,
                            requiredLength: Int = 10): Unit = {
    val fromRow = ResponsesModelResponse.makeFromRowConverter
    model.transform(df).collect().foreach { row =>
      val responseRow = row.getAs[Row]("out")
      if (responseRow != null) {
        fromRow(responseRow).output.foreach { choice =>
          assert(choice.content.length > requiredLength)
        }
      }
    }
  }

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    super.assertDFEq(df1.drop("out"), df2.drop("out"))(eq)
  }

  override def testObjects(): Seq[TestObject[OpenAIResponses]] =
    Seq(new TestObject(responses, goodDf))

  override def reader: MLReadable[_] = OpenAIResponses
}
