// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.Secrets.{AIFoundryApiKey, getAccessToken}
import com.microsoft.azure.synapse.ml.core.test.base.Flaky
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
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

  private def usagePrompt(outputCol: String): OpenAIPrompt = {
    new OpenAIPrompt()
      .setSubscriptionKey(openAIAPIKey)
      .setDeploymentName(deploymentName)
      .setCustomServiceName(openAIServiceName)
      .setPromptTemplate("List two {category}, starting with {text}.")
      .setPostProcessing("csv")
      .setOutputCol(outputCol)
      .setTemperature(0)
  }

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
        case part if part.getOrElse("type", "") == "text"
        && part.getOrElse("text", "").contains("example content") => part
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
      .setDeploymentName(deploymentName)
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
      .setDeploymentName(deploymentName)
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

  test("returnUsage default (unset) keeps structured output") {
    val p = usagePrompt("usage_default")
    val result = p.transform(df.limit(1)).select("usage_default").collect().head
    val values = result.getSeq[String](0)
    assert(values.nonEmpty)
    val schema = p.transformSchema(df.schema)
    assert(schema(p.getOutputCol).dataType == ArrayType(StringType))
  }

  test("returnUsage set to false matches default behavior") {
    val p = usagePrompt("usage_false").setReturnUsage(false)
    val result = p.transform(df.limit(1)).select("usage_false").collect().head
    val values = result.getSeq[String](0)
    assert(values.nonEmpty)
    val schema = p.transformSchema(df.schema)
    assert(schema(p.getOutputCol).dataType == ArrayType(StringType))
  }

  test("returnUsage true emits structured response and usage map") {
    val p = usagePrompt("usage_true").setReturnUsage(true)
    val schema = p.transformSchema(df.schema)
    val structType = schema(p.getOutputCol).dataType.asInstanceOf[StructType]
    assert(structType.fieldNames.contains("response"))
    assert(structType.fieldNames.contains("usage"))

    val row = p.transform(df.limit(1)).select("usage_true").collect().head.getStruct(0)
    val usageRowOpt = Option(row.getAs[Row]("usage"))
    assert(usageRowOpt.isDefined)
    val usageRow = usageRowOpt.get
    val usageFields = usageRow.schema.fieldNames.toSet
    assert(usageFields.contains("input_tokens"))
    assert(usageFields.contains("output_tokens"))
    assert(usageFields.contains("total_tokens"))
    assert(usageFields.contains("input_token_details"))
    assert(usageFields.contains("output_token_details"))
    val inputTokens = usageRow.getAs[java.lang.Long]("input_tokens")
    assert(inputTokens == null || inputTokens.longValue() >= 0)
    val totalTokens = usageRow.getAs[java.lang.Long]("total_tokens")
    assert(totalTokens == null || totalTokens.longValue() >= 0)
    val inputDetails = usageRow.getAs[scala.collection.Map[String, Long]]("input_token_details")
    val outputDetails = usageRow.getAs[scala.collection.Map[String, Long]]("output_token_details")
    assert(inputDetails != null)
    assert(outputDetails != null)
  }

  test("null input returns null output with returnUsage false") {
    val dfWithNull = Seq(
      (Some("apple"), "fruits"),
      (None, "cars"),
      (Some("cake"), "dishes")
    ).toDF("text", "category")

    val p = usagePrompt("null_test")
    val results = p.transform(dfWithNull).select("null_test").collect()

    assert(results(0).getSeq[String](0) != null)
    assert(results(1).get(0) == null)
    assert(results(2).getSeq[String](0) != null)
  }

  test("null input returns null output with returnUsage true") {
    val dfWithNull = Seq(
      (Some("apple"), "fruits"),
      (None, "cars"),
      (Some("cake"), "dishes")
    ).toDF("text", "category")

    val p = usagePrompt("null_test_usage").setReturnUsage(true)
    val results = p.transform(dfWithNull).select("null_test_usage").collect()

    assert(results(0).getStruct(0) != null)
    assert(results(1).get(0) == null)
    assert(results(2).getStruct(0) != null)
  }

  test("Basic Usage JSON - without explicit post-processing") {
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

  test("Setting and Keeping Messages Col") {
    prompt.setMessagesCol("messages")
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

  test("json_object Response Format Usage") {
    val promptJSONObject: OpenAIPrompt = new OpenAIPrompt()
      .setSubscriptionKey(openAIAPIKey)
      .setDeploymentName(deploymentName)
      .setCustomServiceName(openAIServiceName)
      .setOutputCol("outParsed")
      .setTemperature(0)
      .setPromptTemplate(
        """Split a word into prefix and postfix in JSON format
          |Cherry: {{"prefix": "Che", "suffix": "rry"}}
          |{text}:
          |""".stripMargin)
      .setResponseFormat("json_object")
      .setPostProcessingOptions(Map("jsonSchema" -> "prefix STRING, suffix STRING"))


    promptJSONObject.transform(df)
               .select("outParsed")
               .where(col("outParsed").isNotNull)
               .collect()
               .foreach(r => assert(r.getStruct(0).getString(0).nonEmpty))
  }


  test("Take Multimodal Message") {
    val promptResponses = new OpenAIPrompt()
      .setSubscriptionKey(openAIAPIKey)
      .setDeploymentName(deploymentName)
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

    lazy val customPrompt: OpenAIPrompt = new OpenAIPrompt()
      .setCustomUrlRoot(customRootUrlValue)
      .setOutputCol("outParsed")
      .setTemperature(0)

    if (accessToken.isEmpty) {
      customPrompt.setSubscriptionKey(openAIAPIKey)
        .setDeploymentName(deploymentName)
        .setCustomServiceName(openAIServiceName)
    } else {
      customPrompt.setAADToken(accessToken)
        .setCustomHeaders(customHeadersValues)
    }

    customPrompt.setPromptTemplate("give me a comma separated list of 5 {category}, starting with {text} ")
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

  test("reject bare json_schema string in OpenAIPrompt responseFormat passthrough"){
    val p = new OpenAIPrompt()
    intercept[IllegalArgumentException] {
      p.setResponseFormat("json_schema")
    }
    // Ensure json_schema provided as Map still works
    p.setResponseFormat(Map(
      "type" -> "json_schema",
      "json_schema" -> Map(
        "name" -> "answer_schema",
        "schema" -> Map(
          "type" -> "object"
        )
      )
    ))
  }

  lazy val longInputDf: DataFrame = Seq(
    ("apple", "fruits"),
    (null, null), // scalastyle:ignore null
    ("Flying on the weekends is a lot of fun! " * 1000, "travel"),
    ("Flying is a lot of fun! " * 10000, "travel")
  ).toDF("text", "category")

  test("Long Input Handling") {
    val results = prompt
      .setPromptTemplate("Summarize the following text in 10 words or less: {text}")
      .setTimeout(120.0)
      .transform(longInputDf)
      .select("outParsed", prompt.getErrorCol)
      .collect()

    // Row 0: "apple" - normal input, should have valid output
    assert(Option(results(0).get(0)).isDefined)

    // Row 1: null input should return null output
    assert(results(1).get(0) == null)

    // Row 2: 1000 repetitions - may succeed or fail depending on model limits
    val row2HasOutput = Option(results(2).get(0)).isDefined
    val row2HasError = Option(results(2).getAs[Row](1)).isDefined
    assert(row2HasOutput || row2HasError, "Row 2 should have either output or error")

    // Row 3: 10000 repetitions - possible to exceed token limits
    val row3HasOutput = Option(results(3).get(0)).isDefined
    val row3HasError = Option(results(3).getAs[Row](1)).isDefined
    assert(row3HasOutput || row3HasError, "Row 3 should have either output or error")
  }

  test("Timeout Configuration") {
    // Test that timeout parameters are properly set and retrieved
    val promptWithTimeout = new OpenAIPrompt()
      .setSubscriptionKey(openAIAPIKey)
      .setDeploymentName(deploymentName)
      .setCustomServiceName(openAIServiceName)
      .setOutputCol("outParsed")
      .setApiTimeout(300.0)
      .setConnectionTimeout(10.0)
      .setTimeout(60.0)

    assert(promptWithTimeout.getApiTimeout == 300.0)
    assert(promptWithTimeout.getConnectionTimeout == 10.0)
    assert(promptWithTimeout.getTimeout == 60.0)
  }

  test("Short Timeout Returns Timeout Error") {
    val promptWithShortTimeout = new OpenAIPrompt()
      .setSubscriptionKey(openAIAPIKey)
      .setDeploymentName(deploymentName)
      .setCustomServiceName(openAIServiceName)
      .setOutputCol("outParsed")
      .setPromptTemplate("List 5 {category}")
      .setTimeout(0.001) // Very short timeout to force timeout error

    val results = promptWithShortTimeout
      .transform(df)
      .select("outParsed", promptWithShortTimeout.getErrorCol)
      .collect()

    // All rows should have timeout errors due to very short timeout
    results.foreach { row =>
      val errorRow = row.getAs[Row](1)
      assert(errorRow != null, "Should have error due to timeout")
      val errorResponse = errorRow.getAs[String]("response")
      assert(errorResponse.contains("exceeded the time limit") ||
        errorResponse.contains("timeout"),
        s"Error should mention timeout, got: $errorResponse")
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
