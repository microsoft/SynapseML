// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.Secrets.getAccessToken
import com.microsoft.azure.synapse.ml.core.test.base.Flaky
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.scalactic.Equality

class OpenAIPromptSuite extends TransformerFuzzing[OpenAIPrompt] with OpenAIAPIKey with Flaky {

  import spark.implicits._

  override def beforeAll(): Unit = {
    val aadToken = getAccessToken("https://cognitiveservices.azure.com/")
    println(s"Triggering token creation early ${aadToken.length}")
    super.beforeAll()
  }

  lazy val prompt: OpenAIPrompt = new OpenAIPrompt()
    .setSubscriptionKey(openAIAPIKey)
    .setDeploymentName(deploymentName)
    .setCustomServiceName(openAIServiceName)
    .setOutputCol("outParsed")
    .setTemperature(0)

  lazy val df: DataFrame = Seq(
    ("apple", "fruits"),
    ("mercedes", "cars"),
    ("cake", "dishes"),
    (null, "none") //scalastyle:ignore null
  ).toDF("text", "category")

  test("RAI Usage") {
    val result = prompt
      .setDeploymentName(deploymentNameGpt4)
      .setPromptTemplate("Tell me about a graphically disgusting movie in detail")
      .transform(df)
      .select(prompt.getErrorCol)
      .collect().head.getAs[Row](0)
    assert(Option(result).nonEmpty)
  }

  test("Basic Usage") {
    val nonNullCount = prompt
      .setPromptTemplate("here is a comma separated list of 5 {category}: {text}, ")
      .setPostProcessing("csv")
      .transform(df)
      .select("outParsed")
      .collect()
      .count(r => Option(r.getSeq[String](0)).isDefined)

    assert(nonNullCount == 3)
  }

  test("Basic Usage JSON") {
    prompt.setPromptTemplate(
        """Split a word into prefix and postfix a respond in JSON
          |Cherry: {{"prefix": "Che", "suffix": "rry"}}
          |{text}:
          |""".stripMargin)
      .setPostProcessing("json")
      .setPostProcessingOptions(Map("jsonSchema" -> "prefix STRING, suffix STRING"))
      .transform(df)
      .select("outParsed")
      .where(col("outParsed").isNotNull)
      .collect()
      .foreach(r => assert(r.getStruct(0).getString(0).nonEmpty))
  }

  lazy val promptGpt4: OpenAIPrompt = new OpenAIPrompt()
    .setSubscriptionKey(openAIAPIKey)
    .setDeploymentName(deploymentNameGpt4)
    .setCustomServiceName(openAIServiceName)
    .setOutputCol("outParsed")
    .setTemperature(0)

  test("Basic Usage - Gpt 4") {
    val nonNullCount = promptGpt4
      .setPromptTemplate("here is a comma separated list of 5 {category}: {text}, ")
      .setPostProcessing("csv")
      .transform(df)
      .select("outParsed")
      .collect()
      .count(r => Option(r.getSeq[String](0)).isDefined)

    assert(nonNullCount == 3)
  }

  test("Basic Usage JSON - Gpt 4") {
    promptGpt4.setPromptTemplate(
        """Split a word into prefix and postfix a respond in JSON
          |Cherry: {{"prefix": "Che", "suffix": "rry"}}
          |{text}:
          |""".stripMargin)
      .setPostProcessing("json")
      .setPostProcessingOptions(Map("jsonSchema" -> "prefix STRING, suffix STRING"))
      .transform(df)
      .select("outParsed")
      .where(col("outParsed").isNotNull)
      .collect()
      .foreach(r => assert(r.getStruct(0).getString(0).nonEmpty))
  }

  test("Basic Usage JSON - Gpt 4 without explicit post-processing") {
    promptGpt4.setPromptTemplate(
                """Split a word into prefix and postfix a respond in JSON
                  |Cherry: {{"prefix": "Che", "suffix": "rry"}}
                  |{text}:
                  |""".stripMargin)
              .setPostProcessingOptions(Map("jsonSchema" -> "prefix STRING, suffix STRING"))
              .transform(df)
              .select("outParsed")
              .where(col("outParsed").isNotNull)
              .collect()
              .foreach(r => assert(r.getStruct(0).getString(0).nonEmpty))
  }

  test("Setting and Keeping Messages Col - Gpt 4") {
    promptGpt4.setMessagesCol("messages")
      .setDropPrompt(false)
      .setPromptTemplate(
        """Classify each word as to whether they are an F1 team or not
          |ferrari: TRUE
          |tomato: FALSE
          |{text}:
          |""".stripMargin)
      .transform(df)
      .select("messages")
      .where(col("messages").isNotNull)
      .collect()
      .foreach(r => assert(r.get(0) != null))
  }

  test("Basic Usage JSON - Gpt 4o with responseFormat") {
    val promptGpt4o: OpenAIPrompt = new OpenAIPrompt()
      .setSubscriptionKey(openAIAPIKey)
      .setDeploymentName(deploymentNameGpt4o)
      .setCustomServiceName(openAIServiceName)
      .setOutputCol("outParsed")
      .setTemperature(0)
      .setResponseFormat(OpenAIResponseFormat.JSON)

    promptGpt4o.setPromptTemplate(
                """Split a word into prefix and postfix
                  |Cherry: {{"prefix": "Che", "suffix": "rry"}}
                  |{text}:
                  |""".stripMargin)
              .setPostProcessing("json")
               .setResponseFormat("json_object")
              .setPostProcessingOptions(Map("jsonSchema" -> "prefix STRING, suffix STRING"))
              .transform(df)
              .select("outParsed")
              .where(col("outParsed").isNotNull)
              .collect()
              .foreach(r => assert(r.getStruct(0).getString(0).nonEmpty))
  }

  ignore("Custom EndPoint") {
    lazy val accessToken: String = sys.env.getOrElse("CUSTOM_ACCESS_TOKEN", "")
    lazy val customRootUrlValue: String = sys.env.getOrElse("CUSTOM_ROOT_URL", "")
    lazy val customHeadersValues: Map[String, String] = Map("X-ModelType" -> "gpt-4-turbo-chat-completions")

    lazy val customPromptGpt4: OpenAIPrompt = new OpenAIPrompt()
      .setCustomUrlRoot(customRootUrlValue)
      .setOutputCol("outParsed")
      .setTemperature(0)

    if (accessToken.isEmpty) {
      customPromptGpt4.setSubscriptionKey(openAIAPIKey)
        .setDeploymentName(deploymentNameGpt4)
        .setCustomServiceName(openAIServiceName)
    } else {
      customPromptGpt4.setAADToken(accessToken)
        .setCustomHeaders(customHeadersValues)
    }

    customPromptGpt4.setPromptTemplate("here is a comma separated list of 5 {category}: {text}, ")
      .setPostProcessing("csv")
      .transform(df)
      .select("outParsed")
      .collect()
      .count(r => Option(r.getSeq[String](0)).isDefined)
  }

  test("setPostProcessingOptions should set postProcessing to 'csv' for delimiter option") {
    val prompt = new OpenAIPrompt()
    prompt.setPostProcessingOptions(Map("delimiter" -> ","))
    assert(prompt.getPostProcessing == "csv")
  }

  test("setPostProcessingOptions should set postProcessing to 'json' for jsonSchema option") {
    val prompt = new OpenAIPrompt()
    prompt.setPostProcessingOptions(Map("jsonSchema" -> "schema"))
    assert(prompt.getPostProcessing == "json")
  }

  test("setPostProcessingOptions should set postProcessing to 'regex' for regex option") {
    val prompt = new OpenAIPrompt()
    prompt.setPostProcessingOptions(Map("regex" -> ".*", "regexGroup" -> "0"))
    assert(prompt.getPostProcessing == "regex")
  }

  test("setPostProcessingOptions should throw IllegalArgumentException for invalid options") {
    val prompt = new OpenAIPrompt()
    intercept[IllegalArgumentException] {
      prompt.setPostProcessingOptions(Map("invalidOption" -> "value"))
    }
  }

  test("setPostProcessingOptions should validate regex options contain regexGroup key") {
    val prompt = new OpenAIPrompt()
    intercept[IllegalArgumentException] {
      prompt.setPostProcessingOptions(Map("regex" -> ".*"))
    }
  }

  test("setPostProcessingOptions should validate existing postProcessing value") {
    val prompt = new OpenAIPrompt()
    prompt.setPostProcessing("csv")
    intercept[IllegalArgumentException] {
      prompt.setPostProcessingOptions(Map("jsonSchema" -> "schema"))
    }
  }

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    super.assertDFEq(df1.drop("out", "outParsed"), df2.drop("out", "outParsed"))(eq)
  }

  override def testObjects(): Seq[TestObject[OpenAIPrompt]] = {
    val testPrompt = prompt
      .setPromptTemplate("{text} rhymes with ")

    Seq(new TestObject(testPrompt, df))
  }

  override def reader: MLReadable[_] = OpenAIPrompt

}
