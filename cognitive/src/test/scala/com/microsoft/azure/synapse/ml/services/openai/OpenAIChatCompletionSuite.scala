// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.Flaky
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.io.http.{ErrorUtils, HTTPRequestData, HTTPResponseData, HTTPSchema,
  StatusLineData}
import com.microsoft.azure.synapse.ml.services.aifoundry.AIFoundryChatCompletion
import org.apache.commons.io.IOUtils
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.collection.JavaConverters._
import scala.util.Try

// Deterministic offline HTTP handler: canned 200 Chat reply, per-token request capture for wire/skip asserts.
object ChatOfflineHandler extends Serializable {
  val CannedResponse: String = """{"id":"c","object":"chat.completion","created":"1","model":"m",""" +
    """"choices":[{"message":{"role":"assistant","content":"ok","name":null},"index":0,""" +
    """"finish_reason":"stop"}],"system_fingerprint":null,"usage":null}"""

  private val Captures =
    new java.util.concurrent.ConcurrentHashMap[String, java.util.concurrent.ConcurrentLinkedQueue[String]]()

  def newToken(): String = {
    val token = java.util.UUID.randomUUID().toString
    Captures.put(token, new java.util.concurrent.ConcurrentLinkedQueue[String]())
    token
  }

  def requestBodies(token: String): Seq[String] = Captures.get(token).asScala.toList
  def wasInvoked(token: String): Boolean = !Captures.get(token).isEmpty

  def handlerFor(token: String): (CloseableHttpClient, HTTPRequestData) => HTTPResponseData =
    (_, request) => {
      request.entity.foreach(e => Captures.get(token).add(IOUtils.toString(e.content, "UTF-8")))
      HTTPSchema.stringToResponse(CannedResponse, 200, "OK")
    }
}

class OpenAIChatCompletionSuite extends TransformerFuzzing[OpenAIChatCompletion] with OpenAIAPIKey with Flaky {
  override val compareDataInSerializationTest: Boolean = false

  import spark.implicits._

  lazy val completion: OpenAIChatCompletion = new OpenAIChatCompletion()
    .setDeploymentName(deploymentName)
    .setCustomServiceName(openAIServiceName)
    .setMaxCompletionTokens(5000)
    .setOutputCol("out")
    .setMessagesCol("messages")
    .setSubscriptionKey(openAIAPIKey)

  lazy val goodDf: DataFrame = Seq(
    Seq(
      OpenAIMessage("system", "You are an AI chatbot with red as your favorite color"),
      OpenAIMessage("user", "Whats your favorite color")
    ),
    Seq(
      OpenAIMessage("system", "You are very excited"),
      OpenAIMessage("user", "How are you today")
    ),
    Seq(
      OpenAIMessage("system", "You are very excited"),
      OpenAIMessage("user", "How are you today"),
      OpenAIMessage("system", "Better than ever"),
      OpenAIMessage("user", "Why?")
    )
  ).toDF("messages")

  lazy val badDf: DataFrame = Seq(
    Seq(),
    Seq(
      OpenAIMessage("system", "You are very excited"),
      OpenAIMessage("user", null) //scalastyle:ignore null
    ),
    null //scalastyle:ignore null
  ).toDF("messages")

  lazy val slowDf: DataFrame = Seq(
    Seq(
      OpenAIMessage("system", "You help summarize content"),
      OpenAIMessage("user",
        """
        Given the following list of Article and their descriptions:
            Article ID: 13d98f40-517a-4c95-bd84-27df6ea6a2a1
            Article Description: Pride and Prejudice
            Article ID: 6a8a8c95-ef38-4e7b-99b4-81756f602c36
            Article Description: Romeo and Juliet
            Article ID: 4b7b3c82-6c3e-45cf-9f6b-982a45f1dbad
            Article Description: Calculus Made Easy
            Article ID: 8f9d1f15-1aef-4a35-8301-ec8aa869f7b3
            Article Description: Moby Dick; Or, The Whale
            Article ID: 2d8a6e74-7b58-4a63-bc8d-3f5e90ab5b19
            Article Description: The Scarlet Letter
            Article ID: f8c0cfea-42e6-49e2-9f67-dc5ea20197c5
            Article Description: A Christmas Carol in Prose
            Article ID: 1a743f5b-98b3-43b5-b27e-9f631e7c625e
            Article Description: Alice's Adventures in Wonderland
            Article ID: 649da1a1-4df4-41e8-b4c2-943d8d1c47b7
            Article Description: The Eyes Have It
            Article ID: d35b7e0b-6fc5-4bf5-9d27-96d4894950b9
            Article Description: Dracula
            Article ID: a32faffd-d3f3-4f79-b9a6-47c1f6f8e299
            Article Description: The Great Gatsby
            Article ID: 9ea09862-0e44-4a58-81c1-c3449c7644c0
            Article Description: A Doll's House : a play
            Article ID: 3a6f64b8-3aa4-4f6e-bc6c-7df16ff0ce28
            Article Description: The Picture of Dorian Gray
            Article ID: 815b62c6-4c5f-43a1-844c-65d73a32162a
            Article Description: A Modest Proposal
            Article ID: d8e51b1a-1ea8-4a71-8ea3-ff4a2b2a00cf
            Article Description: The Importance of Being Earnest: A Trivial Comedy for Serious People
            Article ID: 682cfd7c-4a6e-4c87-88ae-9a2e7a61bf6b
            Article Description: Metamorphosis
            Article ID: 9a26b5c7-7438-45f3-b9da-8fc7ef3dd84c
            Article Description: The Complete Works of William Shakespeare
            Article ID: 6b2e7e26-2540-4a3a-810c-7d72fc4ec2cf
            Article Description: The Strange Case of Dr. Jekyll and Mr. Hyde
            Article ID: f7112843-719d-4a20-81a2-8767d7a18f15
            Article Description: Middlemarch
            Article ID: b25e40b0-5634-48da-b4fc-15dbbb3a20db
            Article Description: A Room with a View
            Article ID: a13b5f45-8cf2-4f16-8edf-751b3b4b29b8
            Article Description: A Tale of Two Cities
            Article ID: c8c34f96-b2c4-4dab-8d6d-b042e8c9031f
            Article Description: The Yellow Wallpaper
            Article ID: 45d8d855-e129-4c10-8690-34d5fbcff2d9
            Article Description: Little Women; Or, Meg, Jo, Beth, and Amy
            Article ID: 9a887890-1c48-4be0-b796-c542bfb7f3db
            Article Description: The Adventures of Sherlock Holmes
            Article ID: f26c78da-1e57-4d5e-a7dd-42e64d90f804
            Article Description: Jane Eyre: An Autobiography
            Article ID: b1d14760-1c6d-4b91-b58c-48a907f891b3
            Article Description: Great Expectations
            Article ID: 57396190-8a5d-4a2b-90ab-9c3bc02e7fb6
            Article Description: The Enchanted April
            Article ID: 704132cb-0bf4-46de-8b04-2d8c2ad4a409
            Article Description: Adventures of Huckleberry Finn
            Article ID: bcf666b2-803b-45a2-a36b-38a8f5641e20
            Article Description: The Blue Castle: a novel
            Article ID: 0c13bf68-7d10-4a02-8d1a-24290b623db4
            Article Description: The Prince
            Article ID: e13d8a92-7152-4c0c-8e45-1ff6485e3f92
            Article Description: Cranford
            Classify the articles into one of the article classes listed below:
            Fantasy, Historical Fiction, Thriller, Romance, Science Fiction,
            Mystery, Poetry, Drama, Classics, Humor, Religion, Philosophy, Psychology, Business, Other
            For each article, please ensure that you do the following:
            Ignore the numbers in the description.
            Provide a rating between 0 and 10 of how confident you are with the classification
            as well as a short justification.
            Please provide your response in pure JSON syntax for all articles in the list as shown below
            [
                {"article_id": value,
                "article_name": value,
                "article_class": value,
                "article_description": value,
                "confidence": value,
                "justification": value
                }
            ]
            Please do not include any other contextual words
            other than proper JSON object for each article.""".stripMargin)
    )
  ).toDF("messages")

  test("Basic Usage") {
    testCompletion(completion, goodDf)
    testCompletion(completion, slowDf)
  }

  test("Reasoning model with maxCompletionTokens") {
    val reasoningCompletion = new OpenAIChatCompletion()
      .setDeploymentName(deploymentNameMini)
      .setCustomServiceName(openAIServiceName)
      .setMaxCompletionTokens(500)
      .setOutputCol("out")
      .setMessagesCol("messages")
      .setSubscriptionKey(openAIAPIKey)

    val df = Seq(
      Seq(
        OpenAIMessage("user", "What is 2+2?")
      )
    ).toDF("messages")

    testCompletion(reasoningCompletion, df, requiredLength = 0)
  }

  test("Deprecated maxTokens remapped for reasoning model") {
    // Library must remap max_tokens -> max_completion_tokens for reasoning models (the CI break scenario).
    val reasoningCompletion = new OpenAIChatCompletion()
      .setDeploymentName(deploymentNameMini)
      .setCustomServiceName(openAIServiceName)
      .setMaxTokens(500)
      .setOutputCol("out")
      .setMessagesCol("messages")
      .setSubscriptionKey(openAIAPIKey)

    val df = Seq(
      Seq(
        OpenAIMessage("user", "What is 2+2?")
      )
    ).toDF("messages")

    testCompletion(reasoningCompletion, df, requiredLength = 0)
  }

  test("Robustness to bad inputs") {
    val results = completion.transform(badDf).collect()
    assert(Option(results.head.getAs[Row](completion.getErrorCol)).isDefined)
    assert(Option(results.apply(1).getAs[Row](completion.getErrorCol)).isDefined)
    assert(Option(results.apply(2).getAs[Row](completion.getErrorCol)).isEmpty)
    assert(Option(results.apply(2).getAs[Row]("out")).isEmpty)
  }

  private val testImageUrl = "https://mmlspark.blob.core.windows.net/datasets/OCR/test2.png"
  private def offlineChat(token: String, outputCol: String = "out",
                          errorCol: String = "error"): OpenAIChatCompletion =
    new OpenAIChatCompletion().setDeploymentName("d").setCustomServiceName("s").setSubscriptionKey("k")
      .setMessagesCol("messages").setOutputCol(outputCol).setErrorCol(errorCol)
      .setHandler(ChatOfflineHandler.handlerFor(token))
  private def imageUrlOnWire(token: String): String = {
    val JsArray(msgs) = ChatOfflineHandler.requestBodies(token).head.parseJson.asJsObject.fields("messages")
    val JsArray(content) = msgs.head.asJsObject.fields("content")
    content.find(_.asJsObject.fields("type").convertTo[String] == "image_url")
      .get.asJsObject.fields("image_url").asJsObject.fields("url").convertTo[String]
  }
  private def assertRowLocalErrorAndSkip(df: DataFrame): Unit = {  // ErrorSchema error, null out, zero HTTP
    val token = ChatOfflineHandler.newToken()
    val chat = offlineChat(token)
    val transformed = chat.transform(df)
    assert(transformed.schema(chat.getErrorCol).dataType == ErrorUtils.ErrorSchema)  // ErrorSchema shape
    transformed.collect().foreach { r =>
      assert(Option(r.getAs[Row]("error")).exists(_.getString(0).nonEmpty))  // row-local error text
      assert(Option(r.getAs[Row]("out")).isEmpty)                            // null output
    }
    assert(!ChatOfflineHandler.wasInvoked(token))                            // zero HTTP
  }
  private def comp(parts: Seq[Map[String, String]]): DataFrame =
    Seq(Seq(OpenAICompositeMessage("user", parts))).toDF("messages")
  // A driver-local (LocalRelation) DataFrame with an explicit Array(Struct(...)) message schema.
  private def typed(msgSchema: StructType, msg: Row): DataFrame =
    spark.createDataFrame(Seq(Row(Seq(msg))).asJava,
      StructType(Seq(StructField("messages", ArrayType(msgSchema, containsNull = true)))))
  test("offline typed nested image_url Struct(url,detail) locks exact wire payload and restores messagesCol") {
    val token = ChatOfflineHandler.newToken()
    val urlSchema = StructType(Seq(StructField("url", StringType), StructField("detail", StringType)))
    val partSchema = StructType(Seq(StructField("type", StringType), StructField("text", StringType),
      StructField("image_url", urlSchema)))
    val msgSchema = StructType(Seq(StructField("role", StringType), StructField("content", ArrayType(partSchema))))
    val dfSchema = StructType(Seq(StructField("messages", ArrayType(msgSchema))))
    val row = Row(Seq(Row("user", Seq(Row("text", "Describe the image", null), //scalastyle:ignore null
      Row("image_url", null, Row(testImageUrl, "high")))))) //scalastyle:ignore null
    val df = spark.createDataFrame(Seq(row).asJava, dfSchema)  // driver-local so HTTP capture is deterministic
    val transformed = offlineChat(token).transform(df)
    val out = transformed.collect()  // force the HTTP call before reading the captured request bodies
    assert(transformed.schema("messages").dataType == dfSchema("messages").dataType) // schema unchanged
    val JsArray(msgs) = ChatOfflineHandler.requestBodies(token).head.parseJson.asJsObject.fields("messages")
    val JsArray(content) = msgs.head.asJsObject.fields("content")
    assert(content.head.asJsObject.fields ==
      Map("type" -> JsString("text"), "text" -> JsString("Describe the image")))
    assert(content(1).asJsObject.fields == Map("type" -> JsString("image_url"),
      "image_url" -> JsObject("url" -> JsString(testImageUrl), "detail" -> JsString("high"))))
    assert(out.head.getAs[Seq[Row]]("messages") ==
      df.collect().head.getAs[Seq[Row]]("messages"))  // messagesCol restored deep-equal (out came from HTTP)
  }
  test("offline malformed content matrix routes each invalid shape to errorCol and skips HTTP") {
    val role = StructField("role", StringType)
    val textPart = StructType(Seq(StructField("type", StringType), StructField("text", StringType)))
    val imgPart = StructType(Seq(StructField("type", StringType), StructField("text", StringType),
      StructField("image_url", StringType)))
    val intImagePart = StructType(Seq(StructField("type", StringType), StructField("image_url", IntegerType)))
    val nestedIntImagePart = StructType(Seq(StructField("type", StringType),
      StructField("image_url", StructType(Seq(StructField("url", IntegerType))))))
    val cases = Seq(
      comp(Seq(Map("type" -> " image_url ", "image_url" -> testImageUrl))), // padded type rejected, not trimmed
      typed(StructType(Seq(role)), Row("user")),                            // no content field
      typed(StructType(Seq(role, StructField("content",                     // one-field (arity) struct part
        ArrayType(StructType(Seq(StructField("type", StringType))))))), Row("user", Seq(Row("image_url")))),
      typed(StructType(Seq(role, StructField("content", ArrayType(StringType)))), Row("user", Seq("loose"))),
      typed(StructType(Seq(role, StructField("content", ArrayType(StructType(Seq(  // extra leaf aborts the job
        StructField("type", StringType), StructField("image_url", StringType),
        StructField("count", LongType))))))), Row("user", Seq(Row("image_url", testImageUrl, 3L)))),
      Seq(Seq(null, OpenAICompositeMessage("user",  //scalastyle:ignore null
        Seq(Map("type" -> "text", "text" -> "hi"))))).toDF("messages"),     // null message element
      typed(StructType(Seq(role, StructField("content", StringType))), Row(null, "hi")), //scalastyle:ignore null
      typed(StructType(Seq(role, StructField("content", StringType))), Row("user", null)), //scalastyle:ignore null
      typed(StructType(Seq(role, StructField("content", ArrayType(textPart)))),
        Row("user", null)), //scalastyle:ignore null
      typed(StructType(Seq(role, StructField("content", ArrayType(intImagePart)))),
        Row("user", Seq(Row("image_url", 25)))),
      typed(StructType(Seq(role, StructField("content", ArrayType(nestedIntImagePart)))),
        Row("user", Seq(Row("image_url", Row(25))))),
      typed(StructType(Seq(role, StructField("content", ArrayType(textPart)))),  // struct null text (Finding 2)
        Row("user", Seq(Row("text", null)))), //scalastyle:ignore null
      typed(StructType(Seq(role, StructField("content", ArrayType(imgPart)))),    // null text beside an image
        Row("user", Seq(Row("text", null, null), Row("image_url", null, testImageUrl))))) //scalastyle:ignore null
    cases.foreach(assertRowLocalErrorAndSkip)
    val nullToken = ChatOfflineHandler.newToken()
    val rawNull = Seq[Seq[OpenAIMessage]](null).toDF("messages")  //scalastyle:ignore null
    val nullRes = offlineChat(nullToken).transform(rawNull).collect()
    assert(Option(nullRes.head.getAs[Row]("error")).isEmpty && Option(nullRes.head.getAs[Row]("out")).isEmpty)
    assert(!ChatOfflineHandler.wasInvoked(nullToken))  // AC-005 raw-null messages: tolerated skip, zero HTTP
  }
  test("offline OpenAIPrompt blank systemPrompt reaches HTTP, not a validation skip") {
    val token = ChatOfflineHandler.newToken()  // exact empty-string payload is locked in OpenAICoreOfflineSuite
    val messages = new OpenAIPrompt().setSystemPrompt("").getPromptsForMessage(Right("Hello"))
    val results = offlineChat(token).transform(Seq(messages).toDF("messages")).collect()
    assert(ChatOfflineHandler.wasInvoked(token) && Option(results.head.getAs[Row]("error")).isEmpty)
  }
  test("offline transform preserves a pre-existing errorCol via coalesce") {
    val token = ChatOfflineHandler.newToken()
    val base = Seq(Seq(OpenAICompositeMessage("user", Seq(
      Map("type" -> "image_url", "image_url" -> ""))))).toDF("messages")   // malformed row
    val df = base.withColumn("preErr", struct(lit("preset upstream error").as("response"),
      lit(null).cast(StatusLineData.schema).as("status")).cast(ErrorUtils.ErrorSchema)) //scalastyle:ignore null
    val results = offlineChat(token, errorCol = "PREERR").transform(df).collect()
    assert(results.head.getAs[Row]("PREERR").getString(0) == "preset upstream error")
    assert(Option(results.head.getAs[Row]("out")).isEmpty)
    assert(!ChatOfflineHandler.wasInvoked(token))  // malformed row still skips HTTP
  }
  test("offline transformSchema rejects colliding column names and a non-string role") {
    val token = ChatOfflineHandler.newToken()
    val schema = Seq(Seq(OpenAIMessage("user", "hi"))).toDF("messages").schema
    def df: DataFrame = Seq(Seq(OpenAIMessage("user", "hi"))).toDF("messages")
    offlineChat(token).transformSchema(schema)  // a valid role:String schema passes deterministically
    assertThrows[IllegalArgumentException](offlineChat(token, outputCol = "MESSAGES").transformSchema(schema))
    assertThrows[IllegalArgumentException](offlineChat(token, outputCol = "MESSAGES").transform(df))
    assertThrows[IllegalArgumentException](offlineChat(token, errorCol = "MeSsAgEs").transformSchema(schema))
    assertThrows[IllegalArgumentException](offlineChat(token, errorCol = "MeSsAgEs").transform(df))
    // Finding 1: a deterministically incompatible non-string role also fails fast (no per-row abort).
    val intRole = StructType(Seq(StructField("MESSAGES", ArrayType(StructType(Seq(
      StructField("ROLE", IntegerType), StructField("content", StringType)))))))
    assertThrows[IllegalArgumentException](offlineChat(token).transformSchema(intRole))
  }

  test("offline AIFoundryChatCompletion inherits the multimodal Chat fix") {
    val token = ChatOfflineHandler.newToken()
    val df = Seq(
      Seq(OpenAICompositeMessage("user", Seq(Map("type" -> "text", "text" -> "What is in this image?"),
        Map("type" -> "image_url", "image_url" -> testImageUrl)))),
      Seq(OpenAICompositeMessage("user", Seq(Map("type" -> "image_url", "image_url" -> ""))))  // malformed
    ).toDF("messages")
    val chat = new AIFoundryChatCompletion().setCustomServiceName("dummy-foundry").setModel("dummy-model")
      .setSubscriptionKey("k").setMessagesCol("messages").setOutputCol("out")
      .setHandler(ChatOfflineHandler.handlerFor(token))
    val results = chat.transform(df).collect()  // valid row: image survives + output present (inherited)
    assert(imageUrlOnWire(token) == testImageUrl && Option(results.head.getAs[Row]("out")).isDefined)
    assert(Option(results(1).getAs[Row](chat.getErrorCol)).isDefined && Option(results(1).getAs[Row]("out")).isEmpty)
  }

  test("offline copy and save/load round-trip preserve params (MLReadable)") {
    val stage = new OpenAIChatCompletion().setDeploymentName("d").setCustomServiceName("s")
      .setSubscriptionKey("k").setMessagesCol("messages").setOutputCol("out").setErrorCol("error")
    assert(stage.copy(new org.apache.spark.ml.param.ParamMap()).asInstanceOf[OpenAIChatCompletion]
      .getMessagesCol == "messages")
    val path = java.nio.file.Files.createTempDirectory("chat-mlio").resolve("stage").toString
    stage.write.overwrite().save(path)
    val loaded = OpenAIChatCompletion.load(path)
    assert(loaded.getMessagesCol == "messages" && loaded.getOutputCol == "out")
    assert(loaded.getErrorCol == "error" && loaded.getDeploymentName == "d")
  }

  private def openAICredentialsAvailable: Boolean =  // credential-gated: no key => cancelled, not failed
    Try(openAIAPIKey).toOption.exists(k => k != null && k.nonEmpty) //scalastyle:ignore null

  test("live multimodal Chat smoke") {
    assume(openAICredentialsAvailable, "OpenAI credentials unavailable; skipping live multimodal smoke")
    val df = Seq(Seq(OpenAICompositeMessage("user", Seq(
      Map("type" -> "text", "text" -> "Describe the image in one word."),
      Map("type" -> "image_url", "image_url" -> testImageUrl))))).toDF("messages")
    val chat = new OpenAIChatCompletion().setDeploymentName(deploymentName)
      .setCustomServiceName(openAIServiceName).setMaxCompletionTokens(500)
      .setMessagesCol("messages").setOutputCol("out").setSubscriptionKey(openAIAPIKey)
    val results = chat.transform(df).collect()
    val fromRow = ChatModelResponse.makeFromRowConverter
    assert(Option(results.head.getAs[Row]("out")).isDefined)
    assert(fromRow(results.head.getAs[Row]("out")).choices.head.message.content.nonEmpty)
  }

  test("getOptionalParam should include responseFormat"){
    val completion = new OpenAIChatCompletion()
      .setDeploymentName(deploymentName)

    def validateResponseFormat(params: Map[String, Any], responseFormat: String): Unit = {
      val responseFormatPayloadName = this.completion.responseFormat.payloadName
      assert(params.contains(responseFormatPayloadName))
      val responseFormatMap = params(responseFormatPayloadName).asInstanceOf[Map[String, Any]]
      assert(responseFormatMap.contains("type"))
      assert(responseFormatMap("type") == responseFormat)
    }

    val messages: Seq[Row] = Seq(
      OpenAIMessage("user", "Whats your favorite color")
    ).toDF("role", "content", "name").collect()

    val optionalParams: Map[String, Any] = completion.getOptionalParams(messages.head)
    assert(!optionalParams.contains("response_format"))

    completion.setResponseFormat("")
    val optionalParams0: Map[String, Any] = completion.getOptionalParams(messages.head)
    assert(!optionalParams0.contains("response_format"))

    completion.setResponseFormat("json_object")
    val optionalParams1: Map[String, Any] = completion.getOptionalParams(messages.head)
    validateResponseFormat(optionalParams1, "json_object")

    completion.setResponseFormat("text")
    val optionalParams2: Map[String, Any] = completion.getOptionalParams(messages.head)
    validateResponseFormat(optionalParams2, "text")

    completion.setResponseFormat(Map("type" -> "json_object"))
    val optionalParams3: Map[String, Any] = completion.getOptionalParams(messages.head)
    validateResponseFormat(optionalParams3, "json_object")

    completion.setResponseFormat("text")
    val optionalParams4: Map[String, Any] = completion.getOptionalParams(messages.head)
    validateResponseFormat(optionalParams4, "text")
  }

  test("optional params include configured numeric sampling values") {
    // Simulated usage: numeric sampling parameters only
    val completion = new OpenAIChatCompletion()
      .setDeploymentName("sampling-compatible-model")

    val messages: Seq[Row] = Seq(
      OpenAIMessage("user", "Sample prompt")
    ).toDF("role", "content", "name").collect()

    completion.setTemperature(0.2).setTopP(0.5).setSeed(123)
    val params = completion.getOptionalParams(messages.head)

    assert(params.contains("temperature"))
    assert(params.contains("top_p"))
    assert(params.contains("seed"))
    assert(!params.contains("verbosity"))
    assert(!params.contains("reasoning_effort"))
  }

  test("optional params for gpt-5-mini include reasoning controls only") {
    // Simulated usage: verbosity / reasoning_effort only (no temperature/top_p/seed)
    val completion = new OpenAIChatCompletion()
      .setDeploymentName(deploymentNameMini)

    val messages: Seq[Row] = Seq(
      OpenAIMessage("user", "Hello reasoning")
    ).toDF("role", "content", "name").collect()

    completion.setVerbosity("high").setReasoningEffort("low")
    val params = completion.getOptionalParams(messages.head)

    assert(params.contains("verbosity"))
    assert(params("verbosity") == "high")
    assert(params.contains("reasoning_effort"))
    assert(params("reasoning_effort") == "low")
    assert(!params.contains("temperature"))
    assert(!params.contains("top_p"))
    assert(!params.contains("seed"))
  }

  test("verbosity and reasoning_effort getters and setters") {
    val completion = new OpenAIChatCompletion()
      .setDeploymentName(deploymentNameMini)

    // Test verbosity
    completion.setVerbosity("low")
    assert(completion.getVerbosity == "low")

    completion.setVerbosity("medium")
    assert(completion.getVerbosity == "medium")

    completion.setVerbosity("high")
    assert(completion.getVerbosity == "high")

    // Test reasoning_effort
    completion.setReasoningEffort("minimal")
    assert(completion.getReasoningEffort == "minimal")

    completion.setReasoningEffort("none")
    assert(completion.getReasoningEffort == "none")

    completion.setReasoningEffort("low")
    assert(completion.getReasoningEffort == "low")

    completion.setReasoningEffort("medium")
    assert(completion.getReasoningEffort == "medium")

    completion.setReasoningEffort("high")
    assert(completion.getReasoningEffort == "high")

    completion.setReasoningEffort("xhigh")
    assert(completion.getReasoningEffort == "xhigh")
  }

  test("verbosity parameter correctly serialized in payload") {
    val completion = new OpenAIChatCompletion()
      .setDeploymentName(deploymentName)
      .setVerbosity("high")

    val messages: Seq[Row] = Seq(
      OpenAIMessage("user", "Test message")
    ).toDF("role", "content", "name").collect()

    val params = completion.getOptionalParams(messages.head)
    assert(params.contains("verbosity"))
    assert(params("verbosity") == "high")
  }

  test("reasoning_effort parameter correctly serialized in payload") {
    val completion = new OpenAIChatCompletion()
      .setDeploymentName(deploymentName)
      .setReasoningEffort("low")

    val messages: Seq[Row] = Seq(
      OpenAIMessage("user", "Test message")
    ).toDF("role", "content", "name").collect()

    val params = completion.getOptionalParams(messages.head)
    assert(params.contains("reasoning_effort"))
    assert(params("reasoning_effort") == "low")
  }

  test("both verbosity and reasoning_effort serialized together") {
    val completion = new OpenAIChatCompletion()
      .setDeploymentName(deploymentName)
      .setVerbosity("low")
      .setReasoningEffort("low")

    val messages: Seq[Row] = Seq(
      OpenAIMessage("user", "Test message")
    ).toDF("role", "content", "name").collect()

    val params = completion.getOptionalParams(messages.head)
    assert(params.contains("verbosity"))
    assert(params("verbosity") == "low")
    assert(params.contains("reasoning_effort"))
    assert(params("reasoning_effort") == "low")
  }

  test("verbosity accepts custom string values") {
    val completion = new OpenAIChatCompletion()
      .setDeploymentName(deploymentName)
      .setVerbosity("custom_value")

    val messages: Seq[Row] = Seq(
      OpenAIMessage("user", "Test message")
    ).toDF("role", "content", "name").collect()

    val params = completion.getOptionalParams(messages.head)
    assert(params.contains("verbosity"))
    assert(params("verbosity") == "custom_value")
  }

  test("reasoning_effort accepts custom string values") {
    val completion = new OpenAIChatCompletion()
      .setDeploymentName(deploymentName)
      .setReasoningEffort("custom_reasoning")

    val messages: Seq[Row] = Seq(
      OpenAIMessage("user", "Test message")
    ).toDF("role", "content", "name").collect()

    val params = completion.getOptionalParams(messages.head)
    assert(params.contains("reasoning_effort"))
    assert(params("reasoning_effort") == "custom_reasoning")
  }

  test("parameters not included when not set") {
    val completion = new OpenAIChatCompletion()
      .setDeploymentName(deploymentName)
      // Don't set verbosity or reasoning_effort

    val messages: Seq[Row] = Seq(
      OpenAIMessage("user", "Test message")
    ).toDF("role", "content", "name").collect()

    val params = completion.getOptionalParams(messages.head)
    assert(!params.contains("verbosity"))
    assert(!params.contains("reasoning_effort"))
  }

  test("maxCompletionTokens sends max_completion_tokens on wire") {
    val completion = new OpenAIChatCompletion()
      .setDeploymentName(deploymentName)
      .setMaxCompletionTokens(500)

    val messages: Seq[Row] = Seq(
      OpenAIMessage("user", "Test message")
    ).toDF("role", "content", "name").collect()

    val params = completion.getOptionalParams(messages.head)
    assert(params.contains("max_completion_tokens"))
    assert(!params.contains("max_tokens"))
    assert(params("max_completion_tokens") == 500)
  }

  test("deprecated maxTokens remapped to max_completion_tokens on wire") {
    val completion = new OpenAIChatCompletion()
      .setDeploymentName(deploymentName)
      .setMaxTokens(200)

    val messages: Seq[Row] = Seq(
      OpenAIMessage("user", "Test message")
    ).toDF("role", "content", "name").collect()

    val params = completion.getOptionalParams(messages.head)
    assert(params.contains("max_completion_tokens"))
    assert(!params.contains("max_tokens"))
    assert(params("max_completion_tokens") == 200)
  }

  test("setting both maxTokens and maxCompletionTokens throws") {
    val completion = new OpenAIChatCompletion()
      .setDeploymentName(deploymentName)
      .setMaxTokens(100)
      .setMaxCompletionTokens(200)

    val messages: Seq[Row] = Seq(
      OpenAIMessage("user", "Test message")
    ).toDF("role", "content", "name").collect()

    assertThrows[IllegalArgumentException] {
      completion.getOptionalParams(messages.head)
    }
  }

  test("setResponseFormat should throw exception if invalid format"){
    val completion = new OpenAIChatCompletion()
      .setDeploymentName(deploymentName)

    val messages: Seq[Row] = Seq(
      OpenAIMessage("user", "test")
    ).toDF("role", "content", "name").collect()

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

    completion.setResponseFormat(schemaMap)
    val optionalParams = completion.getOptionalParams(messages.head)
    val rf = optionalParams(this.completion.responseFormat.payloadName).asInstanceOf[Map[String, Any]]
    assert(rf("type") == "json_schema")
    val js = rf("json_schema").asInstanceOf[Map[String, Any]]
    assert(js("name") == "answer_schema")
  }

  // Removed JSON string parsing test: json_schema must be provided as Map

  test("setResponseFormat should throw exception if json_schema JSON string missing type") {
    val completion = new OpenAIChatCompletion()
      .setDeploymentName(deploymentName)
    val badJson =
      """{
        |  "json_schema": {
        |    "name": "answer_schema"
        |  }
        |}""".stripMargin
    assertThrows[IllegalArgumentException] {
      completion.setResponseFormat(badJson)
    }
  }

  // Removed invalid format rejection test: unknown types are passed through to service.

  test("reject bare json_schema string in setResponseFormat"){
    val completion = new OpenAIChatCompletion()
      .setDeploymentName(deploymentNameMini)
    assertThrows[IllegalArgumentException] {
      completion.setResponseFormat("json_schema")
    }
  }

  test("validate that gpt-5-mini accepts json_object response format") {
    val goodDf: DataFrame = Seq(
      Seq(
        OpenAIMessage("system", "Respond with JSON. You are an AI chatbot with red as your favorite color"),
        OpenAIMessage("user", "Whats your favorite color")
      ),
      Seq(
        OpenAIMessage("system", "Respond with JSON. You are very excited"),
        OpenAIMessage("user", "How are you today")
      ),
      Seq(
        OpenAIMessage("system", "Respond with JSON. You are very excited"),
        OpenAIMessage("user", "How are you today"),
        OpenAIMessage("system", "Better than ever"),
        OpenAIMessage("user", "Why?")
      )
    ).toDF("messages")

    val completion = new OpenAIChatCompletion()
      .setDeploymentName(deploymentName)
      .setCustomServiceName(openAIServiceName)
      .setMaxCompletionTokens(500)
      .setOutputCol("out")
      .setMessagesCol("messages")
      .setSubscriptionKey(openAIAPIKey)
      .setResponseFormat("json_object")

    testCompletion(completion, goodDf)
  }

  test("validate that gpt-5-mini accepts text response format") {
    val completion = new OpenAIChatCompletion()
      .setDeploymentName(deploymentName)
      .setCustomServiceName(openAIServiceName)
      .setMaxCompletionTokens(5000)
      .setOutputCol("out")
      .setMessagesCol("messages")
      .setSubscriptionKey(openAIAPIKey)
      .setResponseFormat("text")

    testCompletion(completion, goodDf)
  }

  test("content filtering identifies empty assistant content") {
    val responseJson =
      """{
        |  "id":"chatcmpl_test",
        |  "object":"chat.completion",
        |  "created":"1",
        |  "model":"gpt-5.1",
        |  "choices":[
        |    {
        |      "message":{"role":"assistant","content":null,"name":null},
        |      "index":0,
        |      "finish_reason":"content_filter"
        |    }
        |  ],
        |  "system_fingerprint":null,
        |  "usage":null
        |}""".stripMargin

    val outputRow = spark.read
      .schema(ChatModelResponse.schema)
      .json(Seq(responseJson).toDS)
      .collect()
      .head
    val completion = new OpenAIChatCompletion()
    assert(completion.isContentFiltered(outputRow))
    assert(completion.getFilterReason(outputRow) == "content_filter")
  }

  ignore("Custom EndPoint") {
    lazy val accessToken: String = sys.env.getOrElse("CUSTOM_ACCESS_TOKEN", "")
    lazy val customRootUrlValue: String = sys.env.getOrElse("CUSTOM_ROOT_URL", "")
    lazy val customHeadersValues: Map[String, String] = Map("X-ModelType" -> "gpt-5.1-chat-completions")

    val customEndpointCompletion = new OpenAIChatCompletion()
      .setCustomUrlRoot(customRootUrlValue)
      .setOutputCol("out")
      .setMessagesCol("messages")

    if (accessToken.isEmpty) {
      customEndpointCompletion.setSubscriptionKey(openAIAPIKey)
        .setDeploymentName(deploymentName)
        .setCustomServiceName(openAIServiceName)
    } else {
      customEndpointCompletion.setAADToken(accessToken)
        .setCustomHeaders(customHeadersValues)
    }

    testCompletion(customEndpointCompletion, goodDf)
  }

  def testCompletion(completion: OpenAIChatCompletion, df: DataFrame, requiredLength: Int = 10): Unit = {
    val fromRow = ChatModelResponse.makeFromRowConverter
    completion.transform(df).collect().foreach(r =>
      fromRow(r.getAs[Row]("out")).choices.foreach(c =>
        assert(c.message.content.length > requiredLength)))
  }

  override def testObjects(): Seq[TestObject[OpenAIChatCompletion]] =
    Seq(new TestObject(completion, goodDf))

  override def reader: MLReadable[_] = OpenAIChatCompletion

}
