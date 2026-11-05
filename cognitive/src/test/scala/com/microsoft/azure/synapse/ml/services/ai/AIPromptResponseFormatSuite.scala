// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{ArrayType, BinaryType, IntegerType, MapType, StringType, StructType, TimestampType}
import spray.json._

import java.nio.file.Files
import java.util.Collections
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.util.Using

class AIPromptResponseFormatSuite extends AIPromptTestBase {
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

  test("response format map schemas reject non-JSON SQL types before HTTP") {
    val struct = new StructType().add("value", StringType)
    val unsupported = Seq(MapType(IntegerType, StringType), MapType(StringType, struct),
      MapType(StringType, ArrayType(struct)), MapType(StringType, MapType(IntegerType, StringType)),
      MapType(StringType, TimestampType), MapType(StringType, BinaryType),
      MapType(StringType, ArrayType(BinaryType)), MapType(StringType, MapType(StringType, BinaryType)))
    Using.resource(new AIRecordingServer((_, _) => 200 -> replies.head._2)) { server =>
      unsupported.foreach { formatType =>
        val input = spark.createDataFrame(Collections.emptyList[Row](),
          new StructType().add("text", StringType).add("format", formatType))
        val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
          .setResponseFormatCol("format")
        Seq[() => Any](() => stage.transformSchema(input.schema), () => stage.transform(input)).foreach { call =>
          val error = intercept[IllegalArgumentException](call())
          assert(error.getMessage.contains("responseFormatCol has an unsupported data type"))
        }
        assert(server.requests.isEmpty)
      }
    }
  }

  test("schema convenience rejects invalid values without changing scalar or column bindings") {
    val stage = new AIPrompt().setResponseFormat(Map("type" -> "json_object"))
    val configured = stage.get(stage.responseFormat)
    Seq[Any](new Object(), Double.NaN, Double.PositiveInfinity).foreach { value =>
      intercept[IllegalArgumentException](stage.setResponseSchema(Map("maximum" -> value)))
      assert(stage.get(stage.responseFormat) == configured)
    }
    stage.setResponseFormatCol("format")
    intercept[IllegalArgumentException](stage.setResponseSchema(Map.empty[String, Any]))
    intercept[IllegalArgumentException](stage.setResponseSchema(Map("type" -> "object"), "invalid name"))
    intercept[IllegalArgumentException](stage.setResponseSchema(Map("maximum" -> new Object())))
    assert(stage.getResponseFormatCol == "format")
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

  test("normalized response format depth is rejected before changing scalar or column bindings") {
    for {
      api <- replies.map(_._1)
      binding <- Seq("convenience", "raw", "flat", "nested")
      height <- Seq(AIJson.MaxDepth - 1, AIJson.MaxDepth)
      perRow <- Seq(false, true)
    } {
      val stage = new AIPrompt().setApiType(api).setResponseFormat(Map("type" -> "json_object"))
      if (perRow) stage.setResponseFormatCol("format")
      val original = stage.get(stage.responseFormat)
      withClue(s"$api $binding height=$height perRow=$perRow: ") {
        val error = intercept[IllegalArgumentException](bindSchema(stage, deepSchema(height), binding))
        assert(error.getMessage.contains("nesting"))
        assert(stage.get(stage.responseFormat) == original)
      }
    }
  }

    test("responses column schema depth boundary retains echoed metadata") {
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
      }
    }

  replies.foreach { case (api, reply) =>
    test(s"$api response format depth boundary survives persistence and HTTP") {
      val schema = deepSchema(AIJson.MaxDepth - 2)
      Using.resource(new AIRecordingServer((_, body) =>
        200 -> (if (api == "responses") responseWithEcho(body) else reply))) { server =>
        Seq("convenience", "raw", "flat", "nested").foreach { binding =>
          val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
            .setInputCol("text").setOutputCol("answer").setErrorCol("error")
            .setUsageCol("usage").setResponseIdCol("response_id").setRawResponseCol("raw")
          bindSchema(stage, schema, binding)
          val saved = Files.createTempDirectory(tmpDir, "aiprompt-depth").resolve("stage").toString
          stage.copy(ParamMap.empty).write.session(spark).save(saved)
          val loaded = AIPrompt.load(saved)
          val input = spark.range(1).withColumn("text", lit("hello"))
          val result = loaded.transform(input)
          assert(result.schema == loaded.transformSchema(input.schema))
          val row = result.head()
          val sent = server.requests.remove()._2
          if (api == "responses") verifyEcho(row, responseWithEcho(sent))
          else assert(row.getAs[Row]("error") == null && row.getAs[String]("answer") == "answer")
          val body = sent.parseJson.asJsObject
          val details = if (api == "responses") body.fields("text").asJsObject.fields("format").asJsObject
            else body.fields("response_format").asJsObject.fields("json_schema").asJsObject
          assert(details.fields("schema") == AIJson.fromAny(schema))
        }
        assert(server.requests.isEmpty)
      }
    }

    test(s"$api schema convenience preserves exact numbers and JSON containers through persistence and HTTP") {
      val decimal = BigDecimal("0.12345678901234567890123456789")
      val integer = new java.math.BigInteger("1234567890123456789012345678901234567890")
      val javaProperty = new java.util.LinkedHashMap[String, Any]()
      javaProperty.put("type", "number")
      javaProperty.put("minimum", decimal.bigDecimal)
      val properties = scala.collection.immutable.ListMap[String, Any](
        "scala_decimal" -> Map("type" -> "number", "minimum" -> decimal),
        "large_integer" -> Map("type" -> "integer", "maximum" -> integer),
        "java_decimal" -> javaProperty,
        "json_value" -> JsObject("type" -> JsString("string"),
          "enum" -> JsArray(JsString("yes"), JsString("no"))))
      val schema = scala.collection.immutable.ListMap[String, Any](
        "type" -> "object", "properties" -> properties,
        "required" -> properties.keys.toArray, "additionalProperties" -> false)
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error")
          .setResponseSchema(schema, "precise", strict = false)
        val saved = Files.createTempDirectory(tmpDir, "aiprompt-schema").resolve("stage").toString
        stage.copy(ParamMap.empty).write.session(spark).save(saved)
        val row = AIPrompt.load(saved).transform(spark.range(1).withColumn("text", lit("hello"))).head()
        assert(row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null)
        assert(server.requests.size() == 1)
        val sent = format(server.requests.peek()._2, api)
        val details = if (api == "responses") sent else sent.fields("json_schema").asJsObject
        assert(details.fields("schema") == AIJson.fromAny(schema))
        assert(details.fields("schema").asJsObject.fields("properties").asJsObject.fields.keys.toSeq ==
          properties.keys.toSeq)
        assert(details.fields("name") == JsString("precise") && details.fields("strict") == JsBoolean(false))
      }
    }

    Seq(false, true).foreach { perRow =>
      test(s"$api preserves bare response format order through persistence and HTTP with perRow=$perRow") {
        val fields = Map[String, Any](
          "name" -> "ordered", "description" -> "Extract the answer", "strict" -> true,
          "schema" -> Map("type" -> "object", "properties" -> Map("answer" -> Map("type" -> "string"))))
        val names = Vector("description", "strict", "schema", "name")
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
            .setInputCol("text").setOutputCol("answer").setErrorCol("error")
          Seq(names, names.reverse).foreach { order =>
            val bare = ListMap(order.map(name => name -> fields(name)): _*)
            val input = spark.range(1).withColumn("text", lit("hello"))
              .withColumn("format", lit(AIJson.fromAny(bare).compactPrint))
            if (perRow) stage.setResponseFormatCol("format") else stage.setResponseFormat(bare)
            val saved = Files.createTempDirectory(tmpDir, "aiprompt-bare-order").resolve("stage").toString
            stage.copy(ParamMap.empty).write.session(spark).save(saved)
            val loaded = AIPrompt.load(saved)
            val outputSchema = stage.transformSchema(input.schema)
            assert(loaded.transformSchema(input.schema) == outputSchema)
            val output = loaded.transform(input)
            assert(output.schema == outputSchema)
            val row = output.head()
            assert(row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null)
            val sent = format(server.requests.asScala.last._2, api)
            val details = if (api == "responses") sent else sent.fields("json_schema").asJsObject
            assert(details.fields.keys.toVector.filterNot(_ == "type") == order)
            assert(details.fields - "type" == AIJson.fromAny(bare).asJsObject.fields)
          }
          assert(server.requests.size() == 2)
        }
      }
    }

    test(s"$api preserves string and nested-array response format map bindings") {
      val cases = Seq(
        MapType(StringType, StringType) -> Map("type" -> "json_object"),
        MapType(StringType, MapType(StringType, ArrayType(StringType))) ->
          Map("items" -> Map("enum" -> Array("yes", "no"))))
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        cases.zipWithIndex.foreach { case ((formatType, value), index) =>
          val input = spark.createDataFrame(Collections.singletonList(Row("hello", value)),
            new StructType().add("text", StringType).add("format", formatType))
          val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
            .setInputCol("text").setResponseFormatCol("format").setOutputCol("answer").setErrorCol("error")
          val row = stage.transform(input).head()
          assert(row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null)
          val sent = format(server.requests.remove()._2, api)
          if (index == 0) assert(sent.fields("type") == JsString("json_object"))
          else {
            val details = if (api == "responses") sent else sent.fields("json_schema").asJsObject
            assert(details.fields("schema") ==
              JsObject("items" -> JsObject("enum" -> JsArray(JsString("yes"), JsString("no")))))
          }
        }
      }
    }

      test(s"$api preserves response format descriptions and extensions") {
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
          }
        }
      }
    test(s"$api preserves text and JSON format extensions without relocating raw schema descriptions") {
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        Seq(
          Map[String, Any]("type" -> "text", "provider_extension" -> "value"),
          Map[String, Any]("type" -> "json_object", "provider_extension" -> "value"),
          Map[String, Any]("type" -> "object", "description" -> "Inner schema", "properties" -> Map.empty),
          Map[String, Any]("description" -> "Inner schema", "properties" -> Map.empty)
        ).foreach { configured =>
          Seq(false, true).foreach { perRow =>
            val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
              .setInputCol("text").setOutputCol("answer").setErrorCol("error")
            val input = spark.range(1).withColumn("text", lit("hello"))
              .withColumn("format", lit(AIJson.fromAny(configured).compactPrint))
            if (perRow) stage.setResponseFormatCol("format") else stage.setResponseFormat(configured)
            assert(stage.transform(input).head().getAs[Row]("error") == null)
            val sent = format(server.requests.asScala.last._2, api)
            if (configured.get("type").forall(_ == "object")) {
              val details = if (api == "responses") sent else sent.fields("json_schema").asJsObject
              assert(details.fields("schema") == AIJson.fromAny(configured))
              assert(!details.fields.contains("description"))
            } else assert(sent == AIJson.fromAny(configured))
          }
        }
        assert(server.requests.size() == 8)
      }
    }

    test(s"$api rejects ambiguous response format wrappers instead of dropping their fields") {
      import spark.implicits._
      val details = Map[String, Any]("name" -> "answer", "schema" -> Map("type" -> "object"))
      val nested = Map[String, Any]("type" -> "json_schema", "json_schema" -> details)
      val invalid = Seq(nested + ("description" -> "Outside the schema wrapper"),
        nested + ("json_schema" -> (details + ("type" -> "text"))),
        nested ++ details)
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
          .setInputCol("text").setOutputCol("answer").setErrorCol("error")
        invalid.foreach(value => intercept[IllegalArgumentException](stage.setResponseFormat(value)))
        val input = (invalid :+ nested).zipWithIndex.map { case (value, index) =>
          (index, "hello", AIJson.fromAny(value).compactPrint)
        }.toDF("id", "text", "format")
        val rows = stage.setResponseFormatCol("format").transform(input).collect().sortBy(_.getAs[Int]("id"))
        assert(rows.take(invalid.size).forall(_.getAs[Row]("error").getString(0).contains("invalid_request")))
        assert(rows.last.getAs[Row]("error") == null && rows.last.getAs[String]("answer") == "answer")
        assert(server.requests.size() == 1)
      }
    }
  }
}
