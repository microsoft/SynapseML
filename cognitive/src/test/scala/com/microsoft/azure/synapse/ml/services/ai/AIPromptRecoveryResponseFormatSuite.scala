// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{ArrayType, BinaryType, IntegerType, MapType, StringType, StructType, TimestampType}
import spray.json._

import java.io.File
import java.nio.file.Files
import java.util.Collections
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.util.Using

class AIPromptRecoveryResponseFormatSuite extends AIPromptOutputTestBase {
  private val replies = Seq(
    "chat_completions" -> """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}""",
    "responses" -> """{"output":[{"type":"message","role":"assistant",
      |"content":[{"type":"output_text","text":"answer"}]}]}""".stripMargin)

  private def format(body: String, api: String): JsObject = {
    val fields = AIJson.objectString(body).fields
    if (api == "responses") fields("text").asJsObject.fields("format").asJsObject
    else fields("response_format").asJsObject
  }

  private def responseWithEcho(body: String): String = JsObject(
    replies.toMap.apply("responses").parseJson.asJsObject.fields ++ Map(
      "text" -> body.parseJson.asJsObject.fields("text"),
      "id" -> JsString("echoed-schema"),
      "usage" -> JsObject("total_tokens" -> JsNumber(7)))).compactPrint

  private def verifyEcho(row: Row, reply: String): Unit = {
    assert(row.getAs[Row]("error") == null && row.getAs[String]("answer") == "answer")
    assert(row.getAs[String]("response_id") == "echoed-schema")
    assert(row.getAs[Row]("usage").getLong(2) == 7L && row.getAs[String]("raw") == reply)
  }

  test("response format map schemas reject non-JSON SQL types before storage or HTTP") {
    val struct = new StructType().add("value", StringType)
    val unsupported = Seq(MapType(IntegerType, StringType), MapType(StringType, struct),
      MapType(StringType, ArrayType(struct)), MapType(StringType, MapType(IntegerType, StringType)),
      MapType(StringType, TimestampType), MapType(StringType, BinaryType),
      MapType(StringType, ArrayType(BinaryType)), MapType(StringType, MapType(StringType, BinaryType)))
    Using.resource(new AIRecordingServer((_, _) => 200 -> replies.head._2)) { server =>
      unsupported.foreach { formatType =>
        val input = spark.createDataFrame(Collections.emptyList[Row](),
          new StructType().add("text", StringType).add("format", formatType))
        Seq(true).foreach { durable =>
          val root = path()
          val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
            .setResponseFormatCol("format")
          if (durable) stage.setOutputPath(root)
          Seq[() => Any](() => stage.transformSchema(input.schema), () => stage.transform(input)).foreach { call =>
            val error = intercept[IllegalArgumentException](call())
            assert(error.getMessage.contains("responseFormatCol has an unsupported data type"))
          }
          assert(!new File(root).exists() && server.requests.isEmpty)
        }
      }
    }
  }

  private val selfContainedSchema = Map("type" -> "object", "properties" -> Map(
    "file_id" -> Map("type" -> "string"), "image_url" -> Map("type" -> "string")))
  private val selfContainedFormat: Map[String, Any] =
    Map("type" -> "json_schema", "name" -> "self_contained", "schema" -> selfContainedSchema)

  replies.foreach { case (api, reply) =>
    test(s"durable $api typed response formats check resources outside output schemas") {
      import spark.implicits._
      val input = Seq("hello").toDF("text")
      val restricted = Seq[Map[String, Any]](
        Map("type" -> "json_object", "file_id" -> "provider-file"),
        Map("type" -> "json_schema", "json_schema" ->
          ((selfContainedFormat - "type") + ("tools" -> Seq("provider-tool")))),
        selfContainedFormat + ("image_url" -> "https://example.invalid/media"))
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val root = path()
        val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
          .setInputCol("text").setOutputPath(root)
        restricted.foreach { value =>
          intercept[IllegalArgumentException](stage.setResponseFormat(value).transformSchema(input.schema))
          stage.setResponseFormatCol("format")
          intercept[org.apache.spark.SparkException] {
            stage.validateDurableRows(input.withColumn("format", lit(AIJson.fromAny(value).compactPrint)))
          }
        }
        val maps = Seq(("hello", Map("type" -> "json_object", "file_id" -> "provider-file"))).toDF("text", "format")
        intercept[org.apache.spark.SparkException](stage.validateDurableRows(maps))
        stage.setResponseFormat(selfContainedFormat).transformSchema(input.schema)
        stage.setResponseFormatCol("format").validateDurableRows(input.withColumn("format",
          lit(AIJson.fromAny(selfContainedFormat).compactPrint)))
        stage.validateDurableRows(Seq("not json", """{"type":42}""", Option.empty[String].orNull).toDF("format"))
        assert(!new File(root).exists() && server.requests.isEmpty)
      }
    }

    test(s"durable $api typed response format resources fail before inference and valid rows resume") {
      delta()
      import spark.implicits._
      val resource = Map("type" -> "json_object", "file_id" -> "provider-file")
      val invalid = Seq(("hello", resource)).toDF("text", "format")
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        def stage: AIPrompt = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error").setOutputPath(path())
        val scalar = stage.setResponseFormat(resource)
        intercept[IllegalArgumentException](scalar.transform(invalid))
        assert(!new File(scalar.getOutputPath).exists())
        val column = stage.setResponseFormatCol("format")
        intercept[org.apache.spark.SparkException](column.transform(invalid))
        assert(AIPromptDelta.control(spark, AIPromptDelta.qualify(spark, column.getOutputPath)).state == "initializing")
        intercept[IllegalArgumentException](column.resume(spark))
        assert(server.requests.isEmpty)
        val valid = stage.setResponseFormatCol("format").setOutputBatchSize(2)
        val input = Seq(("ok", AIJson.fromAny(selfContainedFormat).compactPrint), ("bad", "not json"))
          .toDF("text", "format")
        val transformed = valid.transform(input)
        Seq(transformed, valid.resume(spark)).foreach { output =>
          assert(output.schema == valid.transformSchema(input.schema))
          val rows = output.collect().map(row => row.getAs[String]("text") -> row).toMap
          assert(rows("ok").getAs[String]("answer") == "answer" && rows("ok").getAs[Row]("error") == null)
          assert(rows("bad").getAs[Row]("error").getString(0).contains("invalid_request"))
        }
        val status = AIPrompt.getOutputStatus(spark, valid.getOutputPath)
        assert(status.isComplete && status.rowErrorCount == 1 && server.requests.size() == 1)
        assert(format(server.requests.peek()._2, api) == AIRequestAdapter.responseFormat(selfContainedFormat, api))
      }
    }
  }
  private def deepSchema(height: Int): Map[String, Any] =
    (1 until height).foldLeft(Map[String, Any]("type" -> "string")) { (value, _) =>
      Map("type" -> "array", "items" -> value)
    }

  private def bindSchema(stage: AIPrompt, schema: Map[String, Any], binding: String): AIPrompt = {
    val flat = Map("type" -> "json_schema", "name" -> "response_schema", "schema" -> schema)
    binding match {
      case "convenience" => stage.setResponseSchema(schema)
      case "raw" => stage.setResponseFormat(schema)
      case "flat" => stage.setResponseFormat(flat)
      case "nested" => stage.setResponseFormat(Map("type" -> "json_schema", "json_schema" -> (flat - "type")))
    }
  }

  Seq(true).foreach { durable =>
    test(s"responses column schema depth boundary retains echoed metadata with durable=$durable") {
      if (durable) delta()
      import spark.implicits._
      def flat(height: Int): Map[String, Any] = Map("type" -> "json_schema",
        "name" -> "response_schema", "schema" -> deepSchema(height))
      val formats = Seq(
        deepSchema(AIJson.MaxDepth - 1),
        flat(AIJson.MaxDepth - 1),
        Map("type" -> "json_schema", "json_schema" -> (flat(AIJson.MaxDepth - 2) - "type")),
        deepSchema(AIJson.MaxDepth))
      val input = formats.zipWithIndex.map { case (value, index) =>
        (index, "hello", AIJson.fromAny(value).compactPrint)
      }.toDF("index", "text", "format")
      Using.resource(new AIRecordingServer((_, body) => 200 -> responseWithEcho(body))) { server =>
        val stage = new AIPrompt().setApiType("responses").setUrl(server.url + "/v1").setModel("model")
          .setInputCol("text").setResponseFormatCol("format").setOutputCol("answer").setErrorCol("error")
          .setUsageCol("usage").setResponseIdCol("response_id").setRawResponseCol("raw")
        if (durable) stage.setOutputPath(path()).setOutputBatchSize(4)
        def verify(rows: Array[Row]): Unit = {
          val ordered = rows.sortBy(_.getAs[Int]("index"))
          assert(ordered.length == 4)
          ordered.take(3).foreach { row =>
            val expected = AIRequestAdapter.responseFormat(row.getAs[String]("format"), "responses")
            val reply = responseWithEcho(JsObject("text" -> JsObject("format" -> expected)).compactPrint)
            verifyEcho(row, reply)
          }
          assert(ordered.last.getAs[String]("answer") == null)
          assert(ordered.last.getAs[Row]("error").getString(0).contains("invalid_request"))
        }
        val output = stage.transform(input)
        assert(output.schema == stage.transformSchema(input.schema))
        verify(output.collect())
        assert(server.requests.size() == 3)
        if (durable) {
          verify(stage.resume(spark).collect())
          val status = AIPrompt.getOutputStatus(spark, stage.getOutputPath)
          assert(status.isComplete && status.rowErrorCount == 1 && server.requests.size() == 3)
        }
      }
    }
  }

  replies.foreach { case (api, reply) =>

    Seq(false, true).foreach { perRow =>
      test(s"$api preserves bare response format order through persistence and HTTP with perRow=$perRow") {
        val fields = Map[String, Any](
          "name" -> "ordered", "description" -> "Extract the answer", "strict" -> true,
          "schema" -> Map("type" -> "object", "properties" -> Map("answer" -> Map("type" -> "string"))))
        val names = Vector("description", "strict", "schema", "name")
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
            .setInputCol("text").setOutputCol("answer").setErrorCol("error")
          val fingerprints = Seq(names, names.reverse).map { order =>
            val bare = ListMap(order.map(name => name -> fields(name)): _*)
            val input = spark.range(1).withColumn("text", lit("hello"))
              .withColumn("format", lit(AIJson.fromAny(bare).compactPrint))
            if (perRow) stage.setResponseFormatCol("format") else stage.setResponseFormat(bare)
            val saved = Files.createTempDirectory(tmpDir, "aiprompt-bare-order").resolve("stage").toString
            stage.copy(ParamMap.empty).write.session(spark).save(saved)
            val loaded = AIPrompt.load(saved)
            val outputSchema = stage.transformSchema(input.schema)
            val fingerprint = AIPromptOutput.fingerprint(stage, input.schema, outputSchema, spark)
            assert(AIPromptOutput.fingerprint(loaded, input.schema, outputSchema, spark) == fingerprint)
            val output = loaded.transform(input)
            assert(output.schema == outputSchema)
            val row = output.head()
            assert(row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null)
            val sent = format(server.requests.asScala.last._2, api)
            val details = if (api == "responses") sent else sent.fields("json_schema").asJsObject
            assert(details.fields.keys.toVector.filterNot(_ == "type") == order)
            assert(details.fields - "type" == AIJson.fromAny(bare).asJsObject.fields)
            fingerprint
          }
          if (!perRow) assert(fingerprints.distinct.size == 2)
          assert(server.requests.size() == 2)
        }
      }
    }
    Seq(true).foreach { durable =>
      test(s"$api preserves response format descriptions and extensions with durable=$durable") {
        if (durable) delta()
        val details = Map[String, Any]("name" -> "answer", "description" -> "Extract the answer",
          "schema" -> Map("type" -> "object", "description" -> "Inner schema",
            "properties" -> Map("answer" -> Map("type" -> "string"))),
          "strict" -> true, "provider_extension" -> Map("weight" -> BigDecimal("0.1234567890123456789")))
        val nested = Map[String, Any]("type" -> "json_schema", "json_schema" -> details)
        val flat = details + ("type" -> "json_schema")
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          Seq(false, true).foreach { perRow =>
            server.requests.clear()
            val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
              .setInputCol("text").setOutputCol("answer").setErrorCol("error")
            val input = spark.range(1).withColumn("text", lit("hello"))
              .withColumn("format", lit(AIJson.fromAny(flat).compactPrint))
            if (perRow) stage.setResponseFormatCol("format") else stage.setResponseFormat(nested)
            if (durable) stage.setOutputPath(path()).setOutputBatchSize(1)
            val saved = Files.createTempDirectory(tmpDir, "aiprompt-format").resolve("stage").toString
            stage.copy(ParamMap.empty).write.session(spark).save(saved)
            val loaded = AIPrompt.load(saved)
            val result = loaded.transform(input)
            assert(result.schema == loaded.transformSchema(input.schema))
            val row = result.head()
            assert(row.getAs[Row]("error") == null && row.getAs[String]("answer") == "answer")
            assert(server.requests.size() == 1)
            assert(format(server.requests.peek()._2, api) == AIJson.fromAny(
              if (api == "responses") flat else nested))
            if (durable) {
              assert(loaded.resume(spark).count() == 1 && server.requests.size() == 1)
              assert(AIPrompt.getOutputStatus(spark, loaded.getOutputPath).allSucceeded)
              if (!perRow) {
                loaded.setResponseFormat(Map("type" -> "json_schema",
                  "json_schema" -> (details + ("description" -> "Different instructions"))))
                intercept[IllegalArgumentException](loaded.resume(spark))
                assert(server.requests.size() == 1)
              }
            }
          }
        }
      }
    }

  }
}
