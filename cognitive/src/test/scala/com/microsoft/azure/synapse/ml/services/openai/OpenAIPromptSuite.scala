// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.Secrets.{AIFoundryApiKey, getAccessToken}
import com.microsoft.azure.synapse.ml.core.test.base.Flaky
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.scalactic.Equality
import com.microsoft.azure.synapse.ml.services.aifoundry.AIFoundryAPIKey

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class OpenAIPromptSuite extends TransformerFuzzing[OpenAIPrompt] with OpenAIAPIKey
  with AIFoundryAPIKey
  with Flaky {

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

  lazy val aiFoundryPrompt: OpenAIPrompt = new OpenAIPrompt()
    .setSubscriptionKey(aiFoundryAPIKey)
    .setApiVersion("2024-05-01-preview")
    .setModel(aiFoundryModelName)
    .setAIFoundryCustomServiceName(aiFoundryServiceName)
    .setOutputCol("outParsed")
    .setTemperature(0)

  lazy val df: DataFrame = Seq(
    ("apple", "fruits"),
    ("mercedes", "cars"),
    ("cake", "dishes")
  ).toDF("text", "category")

  test("createMessagesForRow generates contentParts for path columns when using Chat Completions API") {
    val prompt = new OpenAIPrompt()
    val tempFile = Files.createTempFile("synapseml-openai", ".txt")
    try {
      Files.write(tempFile, "example content".getBytes(StandardCharsets.UTF_8))
      val attachments = Map("filePath" -> tempFile.toString)
      val messages = prompt.createMessagesForRow("Summarize the file", attachments, Seq("filePath"))

      val systemMessage = messages.find(_.role == "system").get
      assert(systemMessage.content.nonEmpty)

      val userMessage = messages.find(_.role == "user").get
      val parts = userMessage.content
      assert(parts.head.getOrElse("type", "") == "text")
      assert(parts.head.getOrElse("text", "").contains("Summarize the file"))
      val textSection = parts.collectFirst {
        case part if part.getOrElse("type", "") == "text" && part.getOrElse("text", "").contains("example content") => part
      }
      assert(textSection.isDefined)
      assert(userMessage.content.nonEmpty)
    } finally {
      Files.deleteIfExists(tempFile)
    }
  }

  test("applyPathPlaceholders replaces path columns with attachment notice") {
    val prompt = new OpenAIPrompt()
    val template = "Describe {text} with reference to {filePath}"
    val updated = prompt.applyPathPlaceholders(template, Seq("filePath"))
    assert(updated.contains("Content for column 'filePath' will be provided later as an attachment."))
    assert(!updated.contains("{filePath}"))
    assert(updated.contains("{text}"))
  }

  test("RAI Usage") {
    val result = prompt
      .setDeploymentName(deploymentNameGpt4o)
      .setPromptTemplate("Tell me about a graphically disgusting " +
        "and violent movie in detail, " +
        "be very gory and NSFW in your description.")
      .transform(df)
      .where(col(prompt.getErrorCol).isNotNull)
      .select(prompt.getErrorCol)
      .collect().head.getAs[Row](0)
    assert(Option(result).nonEmpty)
  }

  test("Basic Usage") {
    val nonNullCount = prompt
      .setPromptTemplate("give me a comma separated list of 5 {category}, starting with {text} ")
      .setPostProcessing("csv")
      .transform(df)
      .select("outParsed")
      .collect()
      .count(r => Option(r.getSeq[String](0)).isDefined)
    assert(nonNullCount == 3)
  }

  test("Basic Usage AI Foundry") {
    val nonNullCount = aiFoundryPrompt
      .setPromptTemplate("give me a comma separated list of 5 {category}, starting with {text} ")
      .setPostProcessing("csv")
      .transform(df)
      .select("outParsed")
      .collect()
      .count(r => Option(r.getSeq[String](0)).isDefined)
    assert(nonNullCount == 3)
  }

    test("Basic Usage Responses API") {
    val nonNullCount = prompt
      .setPromptTemplate("give me a comma separated list of 5 {category}, starting with {text} ")
      .setApiType("responses")
      .setApiVersion("2025-04-01-preview")
      .setDeploymentName("gpt-4.1-mini")
      .setPostProcessing("csv")
      .transform(df)
      .select("outParsed")
      .collect()
      .count(r => Option(r.getSeq[String](0)).isDefined)
    assert(nonNullCount == 3)
  }

  test("Basic Usage JSON") {
    prompt.setPromptTemplate(
        """Split a word into prefix and postfix a respond in JSON.
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
      .setPromptTemplate("give me a comma separated list of 5 {category}, starting with {text} ")
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
      .setPromptTemplate(
        """Split a word into prefix and postfix
          |Cherry: {{"prefix": "Che", "suffix": "rry"}}
          |{text}:
          |""".stripMargin)
      .setResponseFormat("json_object")
      .setPostProcessingOptions(Map("jsonSchema" -> "prefix STRING, suffix STRING"))


    promptGpt4o.transform(df)
               .select("outParsed")
               .where(col("outParsed").isNotNull)
               .collect()
               .foreach(r => assert(r.getStruct(0).getString(0).nonEmpty))
  }

  test("if responseFormat is set then appropriate system prompt should be present") {
    val prompt = new OpenAIPrompt()
    prompt.setResponseFormat("json_object")
    val messages = prompt.getPromptsForMessage(Right("test"))
    assert(messages.nonEmpty)
    messages.exists(p => p.role == "system" && p.content.contains(OpenAIChatCompletionResponseFormat.JSON.prompt))
  }

  test("Take Multimodal Message") {
    val promptResponses = new OpenAIPrompt()
      .setSubscriptionKey(openAIAPIKey)
      .setDeploymentName(deploymentNameGpt4o)
      .setCustomServiceName(openAIServiceName)
      .setApiVersion("2025-04-01-preview")
      .setApiType("responses")
      .setColumnType("images", "path")
      .setOutputCol("outParsed")
      .setPromptTemplate("{questions}: {images}")
    
    val urlDF = Seq(
      (
        "What's in this document?",
        "https://mmlspark.blob.core.windows.net/datasets/OCR/paper.pdf"
      ),
      (
        "What's in this image?",
        "https://mmlspark.blob.core.windows.net/datasets/OCR/test2.png"
      )
    ).toDF("questions", "images")

    val keywordsForEachQuestions = List("knn", "sorry")

  promptResponses.transform(urlDF)
               .select("outParsed")
               .where(col("outParsed").isNotNull)
               .collect()
               .zip(keywordsForEachQuestions)
               .foreach { case (row, keyword) =>
                 assert(row.getString(0).toLowerCase.contains(keyword))
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

    customPromptGpt4.setPromptTemplate("give me a comma separated list of 5 {category}, starting with {text} ")
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
