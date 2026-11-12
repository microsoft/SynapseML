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

  test("JSON postprocessing diagnostics share explicit read options and nesting limits") {
    val data = Seq("{'value':1.0}", """{"value":NaN}""", """{"value":2.0,"ignored":[[0]]}""").toDF("text")
    Seq(true, false).foreach { enabled =>
      val parser = new JsonParser("value DOUBLE", Map(
        "allowSINGLEQuotes" -> enabled.toString, "allowNonNumericNumbers" -> enabled.toString,
        "maxNestingDepth" -> "3"))
      val rows = data.select(parser.parse(col("text")).as("answer"),
        parser.hasParseError(col("text")).as("invalid")).collect()
      assert(rows.take(2).forall(_.getAs[Boolean]("invalid") == !enabled))
      assert(!rows.last.getAs[Boolean]("invalid"))
      assert(rows.last.getAs[Row]("answer").getDouble(0) == 2.0)
    }
  }

  test("JSON postprocessing preserves trailing line comments when comments are enabled") {
    val data = Seq(
      """{"value":1} // trailing comment""",
      """{"value":"wrong"} // trailing comment""",
      "{\"value\":1} // trailing comment\n{\"value\":2}").toDF("text")
    Seq(true, false).foreach { enabled =>
      val parser = new JsonParser("value INT", Map("allowComments" -> enabled.toString))
      val rows = data.select(parser.parse(col("text")).as("answer"),
        parser.hasParseError(col("text")).as("invalid")).collect()
      assert(rows.head.getAs[Row]("answer").getInt(0) == 1)
      assert(rows.head.getAs[Boolean]("invalid") == !enabled)
      assert(rows.tail.forall(_.getAs[Boolean]("invalid")))
    }
  }

  test("JSON postprocessing diagnoses escaped surrogates in nested and ignored tokens without changing syntax") {
    val high = "\\" + "ud800"
    val low = "\\" + "udc00"
    val pair = "\\" + "ud83d" + "\\" + "ude00"
    val data = Seq(
      s"""{"value":"$high"}""", s"""{"value":"$low"}""",
      s"""{"value":"ordinary","ignored":[{"$high":"other"}]}""",
      s"""{"value":"ordinary","ignored":[["$low"]]}""",
      s"""{value:'$pair',ignored:['$pair']} // permitted""").toDF("text")
    val parser = new JsonParser("value STRING", Map("allowUnquotedFieldNames" -> "true",
      "allowSingleQuotes" -> "true", "allowComments" -> "true"))
    val rows = data.select(parser.parse(col("text")).as("answer"),
      parser.hasParseError(col("text")).as("invalid")).collect()
    assert(rows.take(4).forall(_.getAs[Boolean]("invalid")))
    assert(!rows.last.getAs[Boolean]("invalid"))
    assert(rows.last.getAs[Row]("answer").getString(0) == "\uD83D\uDE00")
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
