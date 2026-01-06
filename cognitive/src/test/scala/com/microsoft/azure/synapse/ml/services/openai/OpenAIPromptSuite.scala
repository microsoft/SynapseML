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

  test("store=true with Responses API returns struct with response and id") {
    val storePrompt = new OpenAIPrompt()
      .setSubscriptionKey(openAIAPIKey)
      .setDeploymentName(deploymentName)
      .setCustomServiceName(openAIServiceName)
      .setApiVersion("2025-04-01-preview")
      .setApiType("responses")
      .setStore(true)
      .setPromptTemplate("What is {text}?")
      .setOutputCol("store_output")
      .setTemperature(0)

    val result = storePrompt.transform(df.limit(1))
    val schema = result.schema
    val outputSchema = schema("store_output").dataType.asInstanceOf[StructType]

    // Verify output is a struct with response and id fields
    assert(outputSchema.fieldNames.contains("response"))
    assert(outputSchema.fieldNames.contains("id"))

    val row = result.select("store_output").collect().head.getStruct(0)
    val response = row.getAs[String]("response")
    val id = row.getAs[String]("id")

    assert(response != null && response.nonEmpty)
    assert(id != null && id.startsWith("resp_"), s"Expected id starting with 'resp_', got: $id")
  }

  test("store=true with returnUsage=true returns struct with response, usage, and id") {
    val storeUsagePrompt = new OpenAIPrompt()
      .setSubscriptionKey(openAIAPIKey)
      .setDeploymentName(deploymentName)
      .setCustomServiceName(openAIServiceName)
      .setApiVersion("2025-04-01-preview")
      .setApiType("responses")
      .setStore(true)
      .setReturnUsage(true)
      .setPromptTemplate("What is {text}?")
      .setOutputCol("store_usage_output")
      .setTemperature(0)

    val result = storeUsagePrompt.transform(df.limit(1))
    val schema = result.schema
    val outputSchema = schema("store_usage_output").dataType.asInstanceOf[StructType]

    // Verify output is a struct with response, usage, and id fields
    assert(outputSchema.fieldNames.contains("response"))
    assert(outputSchema.fieldNames.contains("usage"))
    assert(outputSchema.fieldNames.contains("id"))

    val row = result.select("store_usage_output").collect().head.getStruct(0)
    val response = row.getAs[String]("response")
    val id = row.getAs[String]("id")
    val usage = row.getAs[Row]("usage")

    assert(response != null && response.nonEmpty)
    assert(id != null && id.startsWith("resp_"))
    assert(usage != null)
  }

  test("store=false does not change output structure") {
    val noStorePrompt = new OpenAIPrompt()
      .setSubscriptionKey(openAIAPIKey)
      .setDeploymentName(deploymentName)
      .setCustomServiceName(openAIServiceName)
      .setApiVersion("2025-04-01-preview")
      .setApiType("responses")
      .setStore(false)
      .setPromptTemplate("What is {text}?")
      .setOutputCol("no_store_output")
      .setTemperature(0)

    val result = noStorePrompt.transform(df.limit(1))
    val schema = result.schema

    // Output should be a string, not a struct
    assert(schema("no_store_output").dataType == StringType)
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
    assert(exception.getMessage.contains("store parameter is only supported when apiType is 'responses'"))
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
    assert(exception.getMessage.contains(
      "previousResponseId/previousResponseIdCol parameters are only supported when apiType is 'responses'"
    ))
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
    assert(exception.getMessage.contains(
      "previousResponseId/previousResponseIdCol parameters are only supported when apiType is 'responses'"
    ))
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
