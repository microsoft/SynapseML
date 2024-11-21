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
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.{an, convertToAnyShouldWrapper}

class OpenAIPromptSuite extends TransformerFuzzing[OpenAIPrompt] with OpenAIAPIKey with Flaky {

  import spark.implicits._

  override def beforeAll(): Unit = {
    val aadToken = getAccessToken("https://cognitiveservices.azure.com/")
    println(s"Triggering token creation early ${aadToken.length}")
    super.beforeAll()
  }


  lazy val df: DataFrame = Seq(
    ("apple", "fruits"),
    ("mercedes", "cars"),
    ("cake", "dishes"),
    (null, "none") //scalastyle:ignore null
  ).toDF("text", "category")

  test("RAI Usage") {
    val prompt = createPromptInstance(deploymentNameGpt4)
    val result = prompt
      .setPromptTemplate("Tell me about a graphically disgusting movie in detail")
      .transform(df)
      .select(prompt.getErrorCol)
      .collect().head.getAs[Row](0)
    assert(Option(result).nonEmpty)
  }

  test("Basic Usage") {
    val prompt: OpenAIPrompt = createPromptInstance(deploymentName)
    val nonNullCount = prompt
      .setPromptTemplate("here is a comma separated list of 5 {category}: {text}, ")
      .setPostProcessing("csv")
      .transform(df)
      .select("outParsed")
      .collect()
      .count(r => Option(r.getSeq[String](0)).isDefined)

    assert(nonNullCount == 3)
  }

  test("Basic Usage with only post processing options") {
    val nonNullCount = createPromptInstance(deploymentName)
      .setPromptTemplate("here is a comma separated list of 5 {category}: {text}, ")
      .setPostProcessingOptions(Map("delimiter" -> ","))
      .transform(df)
      .select("outParsed")
      .collect()
      .count(r => Option(r.getSeq[String](0)).isDefined)

    assert(nonNullCount == 3)
  }

  test("Basic Usage JSON") {
    val prompt = createPromptInstance(deploymentName)
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

  test("Basic Usage JSON using text response format") {
    val prompt = createPromptInstance(deploymentName)
    prompt.setPromptTemplate(
        """Split a word into prefix and postfix a respond in JSON
          |Cherry: {{"prefix": "Che", "suffix": "rry"}}
          |{text}:
          |""".stripMargin)
      .setResponseFormat("text")
      .setPostProcessing("json")
      .setPostProcessingOptions(Map("jsonSchema" -> "prefix STRING, suffix STRING"))
      .transform(df)
      .select("outParsed")
      .where(col("outParsed").isNotNull)
      .collect()
      .foreach(r => assert(r.getStruct(0).getString(0).nonEmpty))
  }

  test("Basic Usage JSON using only post processing oiptions") {
    val prompt = createPromptInstance(deploymentName)
    prompt.setPromptTemplate(
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

  test("Basic Usage - Gpt 4") {
    val promptGpt4 = createPromptInstance(deploymentNameGpt4)
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
    val promptGpt4 = createPromptInstance(deploymentNameGpt4)
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

  test("Basic Usage JSON - Gpt 4 using responseFormat TEXT") {
    val promptGpt4 = createPromptInstance(deploymentNameGpt4)
    promptGpt4.setPromptTemplate(
        """Split a word into prefix and postfix a respond in JSON
          |Cherry: {{"prefix": "Che", "suffix": "rry"}}
          |{text}:
          |""".stripMargin)
      .setPostProcessing("json")
      .setResponseFormat("text")
      .setPostProcessingOptions(Map("jsonSchema" -> "prefix STRING, suffix STRING"))
      .transform(df)
      .select("outParsed")
      .where(col("outParsed").isNotNull)
      .collect()
      .foreach(r => assert(r.getStruct(0).getString(0).nonEmpty))
  }

  test("Basic Usage JSON - Gpt 4o using responseFormat JSON") {
    val promptGpt4o = createPromptInstance(deploymentNameGpt4o)
    promptGpt4o.setPromptTemplate(
        """Split a word into prefix and postfix
          |Cherry: {{"prefix": "Che", "suffix": "rry"}}
          |{text}:
          |""".stripMargin)
      .setResponseFormat("json")
      .setPostProcessingOptions(Map("jsonSchema" -> "prefix STRING, suffix STRING"))
      .transform(df)
      .select("outParsed")
      .where(col("outParsed").isNotNull)
      .collect()
      .foreach(r => assert(r.getStruct(0).getString(0).nonEmpty))
  }

  test("Basic Usage - Gpt 4o with response format json") {
    val promptGpt4o = createPromptInstance(deploymentNameGpt4o)
    val nonNullCount = promptGpt4o
      .setPromptTemplate("here is a comma separated list of 5 {category}: {text}, ")
      .setResponseFormat(OpenAIResponseFormat.JSON)
      .transform(df)
      .select("outParsed")
      .collect()
      .length

    assert(nonNullCount == 4)
  }

  test("Basic Usage - Gpt 4o with response format text") {
    val promptGpt4o = createPromptInstance(deploymentNameGpt4o)
    val nonNullCount = promptGpt4o
      .setPromptTemplate("here is a comma separated list of 5 {category}: {text}, ")
      .setResponseFormat(OpenAIResponseFormat.TEXT)
      .transform(df)
      .select("outParsed")
      .collect()
      .length

    assert(nonNullCount == 4)
  }

  test("Setting and Keeping Messages Col - Gpt 4") {
    val promptGpt4 = createPromptInstance(deploymentNameGpt4)
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

  ignore("Basic Usage - Davinci 3 with no response format") {
    val promptDavinci3 = createPromptInstance(deploymentNameDavinci3)
    val rowCount = promptDavinci3
      .setPromptTemplate("here is a comma separated list of 5 {category}: {text}, ")
      .transform(df)
      .select("outParsed")
      .collect()
      .length
    assert(rowCount == 4)
  }

  ignore("Basic Usage - Davinci 3 with response format json") {
    val promptDavinci3 = createPromptInstance(deploymentNameDavinci3)
    intercept[IllegalArgumentException] {
      promptDavinci3
        .setPromptTemplate("here is a comma separated list of 5 {category}: {text}, ")
        .setResponseFormat(OpenAIResponseFormat.JSON)
        .transform(df)
        .select("outParsed")
        .collect()
        .length
    }
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

  test("setResponseFormat should set the response format correctly with String") {
    val prompt = new OpenAIPrompt()
    prompt.setResponseFormat("json")
    prompt.getResponseFormat shouldEqual Map("type" -> "json_object")

    prompt.setResponseFormat("json_object")
    prompt.getResponseFormat shouldEqual Map("type" -> "json_object")

    prompt.setResponseFormat("text")
    prompt.getResponseFormat shouldEqual Map("type" -> "text")
  }

  test("setResponseFormat should throw an exception for invalid response format") {
    val prompt = new OpenAIPrompt()
    an[IllegalArgumentException] should be thrownBy {
      prompt.setResponseFormat("invalid_format")
    }
  }

  test("setPostProcessingOptions should set postProcessing to 'csv' for delimiter option") {
    val prompt = new OpenAIPrompt()
    prompt.setPostProcessingOptions(Map("delimiter" -> ","))
    prompt.getPostProcessing should be ("csv")
  }

  test("setPostProcessingOptions should set postProcessing to 'json' for jsonSchema option") {
    val prompt = new OpenAIPrompt()
    prompt.setPostProcessingOptions(Map("jsonSchema" -> "schema"))
    prompt.getPostProcessing should be ("json")
  }

  test("setPostProcessingOptions should set postProcessing to 'regex' for regex option") {
    val prompt = new OpenAIPrompt()
    prompt.setPostProcessingOptions(Map("regex" -> ".*", "regexGroup" -> "0"))
    prompt.getPostProcessing should be ("regex")
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
    val prompt = createPromptInstance(deploymentName)
    val testPrompt = prompt
      .setPromptTemplate("{text} rhymes with ")

    Seq(new TestObject(testPrompt, df))
  }

  override def reader: MLReadable[_] = OpenAIPrompt

  private def createPromptInstance(deploymentName: String): OpenAIPrompt = {
    new OpenAIPrompt()
      .setSubscriptionKey(openAIAPIKey)
      .setDeploymentName(deploymentName)
      .setCustomServiceName(openAIServiceName)
      .setOutputCol("outParsed")
      .setTemperature(0)
  }
}
