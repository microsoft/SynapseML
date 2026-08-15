// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class OpenAIPromptParserSuite extends TestBase {

  import spark.implicits._

  test("PassThroughParser returns input text and string schema") {
    val parser = new PassThroughParser()
    val parsed = Seq(" keep spacing ").toDF("response")
      .select(parser.parse(col("response")).alias("parsed"))
      .head()
      .getString(0)

    assert(parsed == " keep spacing ")
    assert(parser.outputSchema == StringType)
  }

  test("DelimiterParser trims outer whitespace and splits values") {
    val parser = new DelimiterParser(",")
    val parsed = Seq(" apple, banana ,carrot ").toDF("response")
      .select(parser.parse(col("response")).alias("parsed"))
      .head()
      .getSeq[String](0)

    assert(parsed == Seq("apple", " banana ", "carrot"))
    assert(parser.outputSchema == ArrayType(StringType))
  }

  test("JsonParser removes code fences and parses JSON by schema") {
    val schema = "name STRING, value INT"
    val parser = new JsonParser(schema, Map.empty)
    val parsed = Seq(
      """```json
        |{"name":"alpha","value":7}
        |```""".stripMargin
    ).toDF("response")
      .select(parser.parse(col("response")).alias("parsed"))
      .head()
      .getAs[Row]("parsed")

    assert(parsed.getAs[String]("name") == "alpha")
    assert(parsed.getAs[Int]("value") == 7)
    assert(parser.outputSchema == DataType.fromDDL(schema))
  }

  test("RegexParser extracts configured group and uses string schema") {
    val parser = new RegexParser("score=(\\d+)", 1)
    val parsed = Seq("score=42 done").toDF("response")
      .select(parser.parse(col("response")).alias("parsed"))
      .head()
      .getString(0)

    assert(parsed == "42")
    assert(parser.outputSchema == StringType)
  }

  test("stringMessageWrapper changes text type for responses API") {
    val prompt = new OpenAIPrompt()
    assert(prompt.stringMessageWrapper("hello") == Map("type" -> "text", "text" -> "hello"))

    prompt.setApiType("responses")
    assert(prompt.stringMessageWrapper("hello") == Map("type" -> "input_text", "text" -> "hello"))
  }

  test("createMessagesForRow returns null when path attachments are empty") {
    val prompt = new OpenAIPrompt()
    val messages = prompt.createMessagesForRow("Summarize", Map("filePath" -> "   "), Seq("filePath"))
    assert(messages == null)
  }

  test("createMessagesForRow includes local text file contents for chat completions") {
    val prompt = new OpenAIPrompt()
    val tempFile = Files.createTempFile("synapseml-openai-local", ".txt")

    try {
      Files.write(tempFile, "example content".getBytes(StandardCharsets.UTF_8))

      val messages = prompt.createMessagesForRow(
        "Summarize",
        Map("filePath" -> tempFile.toString),
        Seq("filePath")
      )
      val userParts = messages.find(_.role == "user").get.content

      assert(userParts.head == Map("type" -> "text", "text" -> "Summarize"))
      assert(userParts(1).get("type").contains("text"))
      assert(userParts(1).get("text").exists(_.contains("Content: example content")))
    } finally {
      Files.deleteIfExists(tempFile)
    }
  }

  test("createMessagesForRow formats text file content for responses API") {
    val prompt = new OpenAIPrompt().setApiType("responses")
    val tempFile = Files.createTempFile("synapseml-openai-local", ".txt")

    try {
      Files.write(tempFile, "response content".getBytes(StandardCharsets.UTF_8))

      val messages = prompt.createMessagesForRow(
        "Summarize",
        Map("filePath" -> tempFile.toString),
        Seq("filePath")
      )
      val systemParts = messages.find(_.role == "system").get.content
      val userParts = messages.find(_.role == "user").get.content

      assert(systemParts.head.get("type").contains("input_text"))
      assert(userParts.head == Map("type" -> "input_text", "text" -> "Summarize"))
      assert(userParts(1) == Map("type" -> "input_text", "text" -> "response content"))
    } finally {
      Files.deleteIfExists(tempFile)
    }
  }
}
