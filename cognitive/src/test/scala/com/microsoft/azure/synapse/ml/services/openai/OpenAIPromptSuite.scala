// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.Secrets.{AIFoundryApiKey, getAccessToken}
import com.microsoft.azure.synapse.ml.core.test.base.Flaky
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit}
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

  test("createMessagesForRow returns null when all path columns are null") {
    val prompt = new OpenAIPrompt()
    val attachments = Map("filePath" -> null.asInstanceOf[String])
    val messages = prompt.createMessagesForRow("Summarize the file", attachments, Seq("filePath"))
    assert(messages == null)
  }

  test("createMessagesForRow returns messages when at least one path column has value") {
    val prompt = new OpenAIPrompt()
    val tempFile = Files.createTempFile("synapseml-openai", ".txt")
    try {
      Files.write(tempFile, "example content".getBytes(StandardCharsets.UTF_8))
      val attachments = Map("filePath" -> null.asInstanceOf[String], "anotherPath" -> tempFile.toString)
      val messages = prompt.createMessagesForRow("Summarize", attachments, Seq("filePath", "anotherPath"))
      assert(messages != null)
      assert(messages.nonEmpty)
    } finally {
      Files.deleteIfExists(tempFile)
    }
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

  test("usageCol not set keeps structured output as array") {
    val p = usagePrompt("usage_default")
    val result = p.transform(df.limit(1)).select("usage_default").collect().head
    val values = result.getSeq[String](0)
    assert(values.nonEmpty)
    val schema = p.transformSchema(df.schema)
    assert(schema(p.getOutputCol).dataType == ArrayType(StringType))
    // Verify no usage column is added
    assert(!schema.fieldNames.contains("usage"))
  }

  test("usageCol set emits response in outputCol and usage in separate column") {
    val p = usagePrompt("usage_response").setUsageCol("usage")
    val schema = p.transformSchema(df.schema)

    // outputCol should be array type (from CSV parser)
    assert(schema(p.getOutputCol).dataType == ArrayType(StringType))

    // usageCol should exist and be a struct
    assert(schema.fieldNames.contains("usage"))
    val usageStructType = schema("usage").dataType.asInstanceOf[StructType]
    assert(usageStructType.fieldNames.contains("input_tokens"))
    assert(usageStructType.fieldNames.contains("output_tokens"))
    assert(usageStructType.fieldNames.contains("total_tokens"))

    val result = p.transform(df.limit(1))

    // Verify response is in outputCol
    val responseValues = result.select(p.getOutputCol).collect().head.getSeq[String](0)
    assert(responseValues.nonEmpty)

    // Verify usage is in separate column
    val usageRow = result.select("usage").collect().head.getAs[Row](0)
    assert(usageRow != null)
    val usageFields = usageRow.schema.fieldNames.toSet
    assert(usageFields.contains("input_tokens"))
    assert(usageFields.contains("output_tokens"))
    assert(usageFields.contains("total_tokens"))
    assert(usageFields.contains("input_token_details"))
    assert(usageFields.contains("output_token_details"))
    val totalTokens = usageRow.getAs[java.lang.Long]("total_tokens")
    assert(totalTokens == null || totalTokens.longValue() >= 0)
  }

  test("null input returns null output without usageCol") {
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

  test("null input returns null output with usageCol set") {
    val dfWithNull = Seq(
      (Some("apple"), "fruits"),
      (None, "cars"),
      (Some("cake"), "dishes")
    ).toDF("text", "category")

    val p = usagePrompt("null_test_usage").setUsageCol("usage_col")
    val results = p.transform(dfWithNull).select("null_test_usage", "usage_col").collect()

    // First row: both output and usage should be present
    assert(results(0).getSeq[String](0) != null)
    assert(results(0).getAs[Row](1) != null)

    // Second row: null input results in null output and null usage
    assert(results(1).get(0) == null)
    assert(results(1).get(1) == null)

    // Third row: both output and usage should be present
    assert(results(2).getSeq[String](0) != null)
    assert(results(2).getAs[Row](1) != null)
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

  promptResponses
    .transform(urlDF)
    .select("outParsed")
    .where(col("outParsed").isNotNull)
    .collect()
    .zip(keywordsForEachQuestions)
    .foreach { case (row, keyword) =>
      assert(row.getString(0).toLowerCase.contains(keyword))
    }
  }

  test("null path columns return null output") {
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
        null.asInstanceOf[String]
      ),
      (
        "What's in this image?",
        "https://mmlspark.blob.core.windows.net/datasets/OCR/test2.png"
      )
    ).toDF("questions", "images")

    val results = promptResponses
      .transform(urlDF)
      .select("outParsed")
      .collect()

    // First row: valid path, should have output
    assert(results(0).getString(0) != null)
    // Second row: null path, should have null output
    assert(results(1).get(0) == null)
    // Third row: valid path, should have output
    assert(results(2).getString(0) != null)
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

  test("store parameter defaults to false") {
    val p = new OpenAIPrompt()
    assert(!p.getStore)
  }

  test("store parameter can be set") {
    val p = new OpenAIPrompt()
      .setStore(true)
    assert(p.getStore)
  }

  test("previousResponseId parameter can be set") {
    val p = new OpenAIPrompt()
      .setPreviousResponseId("resp_test123")
    assert(p.getPreviousResponseId == "resp_test123")
  }

  test("previousResponseIdCol parameter can be set") {
    val p = new OpenAIPrompt()
      .setPreviousResponseIdCol("prev_id_column")
    assert(p.getPreviousResponseIdCol == "prev_id_column")
  }

  test("store=true with Responses API returns response in outputCol and id in auto-generated responseIdCol") {
    val storePrompt = new OpenAIPrompt()
      .setSubscriptionKey(openAIAPIKey)
      .setDeploymentName(deploymentName)
      .setCustomServiceName(openAIServiceName)
      .setApiType("responses")
      .setStore(true)
      .setPromptTemplate("What is {text}?")
      .setOutputCol("store_output")
      .setTemperature(0)

    val result = storePrompt.transform(df.limit(1))
    val schema = result.schema

    // Verify outputCol is a simple string (not a struct)
    assert(schema("store_output").dataType == StringType)

    // Verify auto-generated responseIdCol exists
    val responseIdColName = storePrompt.getResponseIdCol
    assert(schema.fieldNames.contains(responseIdColName))
    assert(schema(responseIdColName).dataType == StringType)

    val response = result.select("store_output").collect().head.getString(0)
    val id = result.select(responseIdColName).collect().head.getString(0)

    assert(response != null && response.nonEmpty)
    assert(id != null && id.startsWith("resp_"), s"Expected id starting with 'resp_', got: $id")
  }

  test("store=true with usageCol set returns response, usage, and id in separate columns") {
    val storeUsagePrompt = new OpenAIPrompt()
      .setSubscriptionKey(openAIAPIKey)
      .setDeploymentName(deploymentName)
      .setCustomServiceName(openAIServiceName)
      .setApiType("responses")
      .setStore(true)
      .setUsageCol("usage")
      .setPromptTemplate("What is {text}?")
      .setOutputCol("store_usage_output")
      .setTemperature(0)

    val result = storeUsagePrompt.transform(df.limit(1))
    val schema = result.schema

    // Verify outputCol is a simple string
    assert(schema("store_usage_output").dataType == StringType)

    // Verify usageCol exists
    assert(schema.fieldNames.contains("usage"))
    assert(schema("usage").dataType.isInstanceOf[StructType])

    // Verify responseIdCol exists
    val responseIdColName = storeUsagePrompt.getResponseIdCol
    assert(schema.fieldNames.contains(responseIdColName))
    assert(schema(responseIdColName).dataType == StringType)

    val response = result.select("store_usage_output").collect().head.getString(0)
    val id = result.select(responseIdColName).collect().head.getString(0)
    val usage = result.select("usage").collect().head.getAs[Row](0)

    assert(response != null && response.nonEmpty)
    assert(id != null && id.startsWith("resp_"))
    assert(usage != null)
  }

  test("store=true with custom responseIdCol uses specified column name") {
    val customIdPrompt = new OpenAIPrompt()
      .setSubscriptionKey(openAIAPIKey)
      .setDeploymentName(deploymentName)
      .setCustomServiceName(openAIServiceName)
      .setApiType("responses")
      .setStore(true)
      .setResponseIdCol("custom_id")
      .setPromptTemplate("What is {text}?")
      .setOutputCol("output")
      .setTemperature(0)

    val result = customIdPrompt.transform(df.limit(1))
    val schema = result.schema

    // Verify custom responseIdCol name is used
    assert(schema.fieldNames.contains("custom_id"))
    assert(schema("custom_id").dataType == StringType)

    val id = result.select("custom_id").collect().head.getString(0)
    assert(id != null && id.startsWith("resp_"))
  }

  test("store=false does not add responseIdCol") {
    val noStorePrompt = new OpenAIPrompt()
      .setSubscriptionKey(openAIAPIKey)
      .setDeploymentName(deploymentName)
      .setCustomServiceName(openAIServiceName)
      .setApiType("responses")
      .setStore(false)
      .setPromptTemplate("What is {text}?")
      .setOutputCol("no_store_output")
      .setTemperature(0)

    val result = noStorePrompt.transform(df.limit(1))
    val schema = result.schema

    // Output should be a string
    assert(schema("no_store_output").dataType == StringType)

    // responseIdCol should not exist
    val responseIdColName = noStorePrompt.getResponseIdCol
    assert(!schema.fieldNames.contains(responseIdColName))
  }

  test("store parameter throws error when apiType is not responses") {
    val promptWithStore = new OpenAIPrompt()
      .setSubscriptionKey(openAIAPIKey)
      .setDeploymentName(deploymentName)
      .setCustomServiceName(openAIServiceName)
      .setApiType("chat_completions")
      .setStore(true)
      .setPromptTemplate("What is {text}?")
      .setOutputCol("output")

    val exception = intercept[IllegalArgumentException] {
      promptWithStore.transform(df.limit(1))
    }
    assert(exception.getMessage.contains("store parameter requires apiType='responses'"))
  }

  test("previousResponseId parameter throws error when apiType is not responses") {
    val promptWithPrevId = new OpenAIPrompt()
      .setSubscriptionKey(openAIAPIKey)
      .setDeploymentName(deploymentName)
      .setCustomServiceName(openAIServiceName)
      .setApiType("chat_completions")
      .setPreviousResponseId("resp_test123")
      .setPromptTemplate("What is {text}?")
      .setOutputCol("output")

    val exception = intercept[IllegalArgumentException] {
      promptWithPrevId.transform(df.limit(1))
    }
    assert(exception.getMessage.contains("previousResponseId requires apiType='responses'"))
  }

  test("previousResponseIdCol parameter throws error when apiType is not responses") {
    val dfWithIds = df.withColumn("prev_id", lit("resp_test123"))
    val promptWithPrevIdCol = new OpenAIPrompt()
      .setSubscriptionKey(openAIAPIKey)
      .setDeploymentName(deploymentName)
      .setCustomServiceName(openAIServiceName)
      .setApiType("chat_completions")
      .setPreviousResponseIdCol("prev_id")
      .setPromptTemplate("What is {text}?")
      .setOutputCol("output")

    val exception = intercept[IllegalArgumentException] {
      promptWithPrevIdCol.transform(dfWithIds.limit(1))
    }
    assert(exception.getMessage.contains("previousResponseId requires apiType='responses'"))
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
