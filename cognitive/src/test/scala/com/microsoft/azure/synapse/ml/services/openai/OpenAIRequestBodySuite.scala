// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.commons.io.FileUtils
import org.apache.http.entity.{AbstractHttpEntity, StringEntity}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import spray.json._

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, Executors, TimeUnit}
import java.nio.file.Files
import scala.collection.immutable.ListMap

object OpenAIRequestBodySuite {
  private class CountingValues(counter: AtomicInteger)
    extends scala.collection.immutable.Seq[String] with Serializable {
    private val values = Vector("first", "second")

    override def length: Int = values.length

    override def apply(index: Int): String = values(index)

    override def iterator: Iterator[String] = {
      counter.incrementAndGet()
      values.iterator
    }
  }
}

class OpenAIRequestBodySuite extends TestBase {
  import OpenAIRequestBodySuite._

  private val messageType = new StructType().add("role", StringType).add("content", StringType)
  private val inputType = new StructType().add("messages", ArrayType(messageType))

  private def input(text: String): Row = {
    val message = new GenericRowWithSchema(Array[Any]("user", text), messageType)
    new GenericRowWithSchema(Array[Any](Seq(message)), inputType)
  }

  private def schema(values: scala.collection.Seq[String]): Map[String, Any] = ListMap(
    "type" -> "object",
    "properties" -> ListMap("answer" -> Map("type" -> "string", "enum" -> values)),
    "required" -> Seq("answer"),
    "additionalProperties" -> false
  )

  private def format(value: Map[String, Any], name: String = "answer"): Map[String, Any] =
    Map("name" -> name, "strict" -> true, "schema" -> value)

  private def body(prepare: Row => Option[AbstractHttpEntity], row: Row): JsObject =
    StreamUtilities.using(prepare(row).get.getContent) { stream =>
      scala.io.Source.fromInputStream(stream, "UTF-8").mkString.parseJson.asJsObject
    }.get

  private def chat: OpenAIChatCompletion = new OpenAIChatCompletion()
    .setUrl("http://localhost/openai/v1")
    .setDeploymentName("test")
    .setMessagesCol("messages")

  private def responses: OpenAIResponses = new OpenAIResponses()
    .setUrl("http://localhost/openai/v1")
    .setDeploymentName("test")
    .setMessagesCol("messages")

  Seq("chat", "responses").foreach { api =>
    test(s"$api serializes an unchanged static response format once per stage instance") {
      val counter = new AtomicInteger()
      val responseFormat = format(schema(new CountingValues(counter)))
      val prepare = if (api == "chat") {
        chat.setResponseFormat(responseFormat).prepareEntity
      } else {
        responses.setResponseFormat(responseFormat).prepareEntity
      }
      assert(counter.get() == 0, "Preparing an unused builder must not serialize the format")
      (1 to 5).foreach { index =>
        val payload = body(prepare, input(s"row-$index"))
        val actual = if (api == "chat") {
          payload.fields("response_format").asJsObject.fields("json_schema").asJsObject
        } else {
          payload.fields("text").asJsObject.fields("format").asJsObject
        }
        assert(actual.fields("name") == JsString("answer"))
        val answer = actual.fields("schema").asJsObject.fields("properties").asJsObject.fields("answer")
        assert(answer.asJsObject.fields("enum") == JsArray(JsString("first"), JsString("second")))
        val messages = payload.fields(if (api == "chat") "messages" else "input")
        assert(messages.compactPrint.contains(s"row-$index"))
      }
      assert(counter.get() == 2, "Only the initial immutability check and serialization may traverse the schema")
    }
  }

  test("Changing a scalar response format does not reuse an earlier cached value") {
    val stage = chat.setResponseFormat(format(schema(Seq("old")), "old_name"))
    val prepare = stage.prepareEntity
    assert(body(prepare, input("one")).compactPrint.contains("old_name"))
    stage.setResponseFormat(format(schema(Seq("new")), "new_name"))
    val updated = body(prepare, input("two")).compactPrint
    assert(updated.contains("new_name"))
    assert(!updated.contains("old_name"))
    assert(body(stage.prepareEntity, input("three")).compactPrint.contains("new_name"))
  }

  test("Responses caches its format without freezing row-specific verbosity") {
    val counter = new AtomicInteger()
    val stage = responses.setResponseFormat(format(schema(new CountingValues(counter))))
    stage.setVectorParam(stage.verbosity, "verbosity")
    val prepare = stage.prepareEntity
    val rowType = inputType.add("verbosity", StringType)
    Seq("low", "medium", "high").foreach { verbosity =>
      val row = new GenericRowWithSchema(Array[Any](input("hello").get(0), verbosity), rowType)
      val text = body(prepare, row).fields("text").asJsObject
      assert(text.fields("verbosity") == JsString(verbosity))
      assert(text.fields("format").asJsObject.fields("name") == JsString("answer"))
    }
    assert(counter.get() == 2)
  }

  test("Row-dependent Chat response formats remain row-dependent") {
    val stage = chat
    stage.setVectorParam(stage.responseFormat, "row_format")
    val prepare = stage.prepareEntity
    val rowType = inputType.add("row_format", MapType(StringType, StringType))
    Seq("text", "json_object").foreach { token =>
      val row = new GenericRowWithSchema(Array[Any](input("hello").get(0), Map("type" -> token)), rowType)
      assert(body(prepare, row).fields("response_format").asJsObject.fields("type") == JsString(token))
    }
  }

  test("Row-dependent Responses formats remain row-dependent") {
    val stage = responses
    stage.setVectorParam(stage.responseFormat, "row_format")
    val prepare = stage.prepareEntity
    val rowType = inputType.add("row_format", MapType(StringType, MapType(StringType, StringType)))
    Seq("text", "json_object").foreach { token =>
      val value = Map("format" -> Map("type" -> token))
      val row = new GenericRowWithSchema(Array[Any](input("hello").get(0), value), rowType)
      val actual = body(prepare, row).fields("text").asJsObject.fields("format").asJsObject
      assert(actual.fields("type") == JsString(token))
    }
  }

  test("Cached schema fragments cannot inject fields or alter message escaping") {
    val text = "quote: \"; braces: },{; newline:\n unicode:\u263a"
    val stage = chat.setResponseFormat(format(schema(Seq(text))))
    val payload = body(stage.prepareEntity, input(text))
    assert(payload.fields.keySet == Set("messages", "model", "response_format"))
    val JsArray(messages) = payload.fields("messages")
    assert(messages.head.asJsObject.fields("content") == JsString(text))
    val responseFormat = payload.fields("response_format").asJsObject.fields("json_schema").asJsObject
    val answer = responseFormat.fields("schema").asJsObject.fields("properties").asJsObject.fields("answer")
    assert(answer.asJsObject.fields("enum") == JsArray(JsString(text)))
  }

  test("Concurrent requests initialize one cached format without sharing mutable request state") {
    val counter = new AtomicInteger()
    val prepare = chat.setResponseFormat(format(schema(new CountingValues(counter)))).prepareEntity
    val executor = Executors.newFixedThreadPool(4)
    try {
      val tasks = (1 to 20).map { index =>
        executor.submit(new Callable[JsObject] {
          override def call(): JsObject = body(prepare, input(s"row-$index"))
        })
      }
      tasks.zipWithIndex.foreach { case (task, index) =>
        val payload = task.get(10, TimeUnit.SECONDS)
        val JsArray(messages) = payload.fields("messages")
        assert(messages.head.asJsObject.fields("content") == JsString(s"row-${index + 1}"))
      }
      assert(counter.get() == 2)
    } finally {
      executor.shutdownNow()
    }
  }

  test("Executor-side stage copies rebuild transient caches and reuse them across request builders") {
    for (api <- Seq("chat", "responses"); warm <- Seq(false, true)) {
      val counter = new AtomicInteger()
      val inner = format(schema(new CountingValues(counter)))
      val stage = if (api == "chat") chat.setResponseFormat(inner)
        else responses.setResponseFormat(inner)
      def prepare(value: OpenAIServicesBase): Row => Option[AbstractHttpEntity] = value match {
        case value: OpenAIChatCompletion => value.prepareEntity
        case value: OpenAIResponses => value.prepareEntity
        case other => fail(s"Unexpected stage type: ${other.getClass.getName}")
      }
      if (warm) body(prepare(stage), input("warm"))
      val serializer = new JavaSerializer(spark.sparkContext.getConf).newInstance()
      val serialized = serializer.serialize((stage, counter))
      val (copiedStage, copiedCounter) =
        serializer.deserialize[(OpenAIServicesBase, AtomicInteger)](serialized)
      (1 to 5).foreach { _ =>
        assert(body(prepare(copiedStage), input("copied")).compactPrint.contains("\"name\":\"answer\""))
      }
      val expectedTraversals = if (warm) 4 else 2
      assert(copiedCounter.get() == expectedTraversals, s"$api warm=$warm must rebuild and reuse its cache")
    }
  }

  test("The bounded cache replaces changed formats and ignores column-dependent values") {
    val creations = new AtomicInteger()
    val cache = new OpenAIRequestBodyCache(value => {
      creations.incrementAndGet()
      OpenAIRequestBody.chat(value)
    })
    val before = chat.setResponseSchema(schema(Seq("before")), "before").getResponseFormat
    val after = chat.setResponseSchema(schema(Seq("after")), "after").getResponseFormat
    val initial: Either[Map[String, Any], String] = Left(before)
    val changed: Either[Map[String, Any], String] = Left(after)
    Seq(initial, initial, changed, changed, initial, initial).foreach { value =>
      val selected = value match {
        case Left(format) => format
        case Right(_) => fail("Expected a scalar format")
      }
      val payload = Map[String, Any]("response_format" -> selected)
      val expected = OpenAIRequestBody.Uncached.encode(payload).parseJson
      assert(cache.encode(payload, Some(value)).parseJson == expected)
    }
    assert(creations.get() == 3)
    val column: Either[Map[String, Any], String] = Right("formats")
    Seq(before, after).foreach { selected =>
      val payload = Map[String, Any]("response_format" -> selected)
      val expected = OpenAIRequestBody.Uncached.encode(payload).parseJson
      assert(cache.encode(payload, Some(column)).parseJson == expected)
    }
    assert(creations.get() == 4)
    assert(cache.encode(Map("messages" -> Seq.empty), None).parseJson == JsObject("messages" -> JsArray()))
  }

  test("Prepared requests preserve Chat and Responses request-helper overrides") {
    val chatCalls = new AtomicInteger()
    val responsesCalls = new AtomicInteger()
    val chatStage = new OpenAIChatCompletion() {
      override private[openai] def getStringEntity(
          messages: Seq[Row], optionalParams: Map[String, Any]): StringEntity = {
        chatCalls.incrementAndGet()
        super.getStringEntity(messages, optionalParams)
      }
    }.setUrl("http://localhost/openai/v1").setDeploymentName("test")
      .setMessagesCol("messages").setResponseFormat("text")
    val responsesStage = new OpenAIResponses() {
      override private[openai] def getStringEntity(
          messages: Seq[Row], optionalParams: Map[String, Any]): StringEntity = {
        responsesCalls.incrementAndGet()
        super.getStringEntity(messages, optionalParams)
      }
    }.setUrl("http://localhost/openai/v1").setDeploymentName("test")
      .setMessagesCol("messages").setResponseFormat("text")
    body(chatStage.prepareEntity, input("chat"))
    body(responsesStage.prepareEntity, input("responses"))
    assert(chatCalls.get() == 1)
    assert(responsesCalls.get() == 1)
  }

  test("Cached encoding preserves serialization failures rather than returning a stale successful body") {
    val stage = chat.setResponseFormat(format(schema(Seq("valid"))))
    val prepare = stage.prepareEntity
    body(prepare, input("valid"))
    stage.setResponseFormat(format(Map("type" -> "object", "unsupported" -> new Object())))
    intercept[IllegalArgumentException](body(prepare, input("invalid")))
    intercept[IllegalArgumentException](body(stage.prepareEntity, input("invalid")))
  }

  Seq("chat", "responses").foreach { api =>
    test(s"$api does not cache formats containing mutable sequences") {
      val values = scala.collection.mutable.ArrayBuffer("before")
      val responseFormat = format(schema(values))
      val prepare = if (api == "chat") {
        chat.setResponseFormat(responseFormat).prepareEntity
      } else {
        responses.setResponseFormat(responseFormat).prepareEntity
      }
      assert(body(prepare, input("one")).compactPrint.contains("\"enum\":[\"before\"]"))
      values(0) = "after"
      val updated = body(prepare, input("two")).compactPrint
      assert(updated.contains("\"enum\":[\"after\"]"))
      assert(!updated.contains("before"))
    }
  }

  test("Cached text and JSON-object selectors remain plain selectors in outgoing requests") {
    Seq("text", "json_object").foreach { token =>
      val chatPrepare = chat.setResponseFormat(token).prepareEntity
      val responsesPrepare = responses.setResponseFormat(token).prepareEntity
      (1 to 3).foreach { index =>
        val expected = JsObject("type" -> JsString(token))
        assert(body(chatPrepare, input(s"row-$index")).fields("response_format") == expected)
        val text = body(responsesPrepare, input(s"row-$index")).fields("text").asJsObject
        assert(text.fields("format") == expected)
      }
    }
  }

  test("An unset response format does not create a format or remove Responses verbosity") {
    assert(!body(chat.prepareEntity, input("chat")).fields.contains("response_format"))
    assert(!body(responses.prepareEntity, input("responses")).fields.contains("text"))
    val stage = responses.setVerbosity("low")
    assert(body(stage.prepareEntity, input("verbosity")).fields("text") ==
      JsObject("verbosity" -> JsString("low")))
  }

  test("Responses parameter replacement and copy do not reuse an earlier cached format") {
    val stage = responses.setResponseSchema(schema(Seq("before")), "before")
    val prepare = stage.prepareEntity
    body(prepare, input("warm"))
    stage.setResponseSchema(schema(Seq("updated")), "updated")
    val updated = body(prepare, input("updated")).compactPrint
    assert(updated.contains("\"name\":\"updated\""))
    assert(!updated.contains("before"))
    val copied = stage.copy(ParamMap.empty) match {
      case value: OpenAIResponses => value.setResponseSchema(schema(Seq("copied")), "copied")
      case other => fail(s"Unexpected copied stage type: ${other.getClass.getName}")
    }
    assert(body(copied.prepareEntity, input("copy")).compactPrint.contains("\"name\":\"copied\""))
    assert(body(stage.prepareEntity, input("original")).compactPrint.contains("\"name\":\"updated\""))
    val directory = Files.createTempDirectory("openai-responses-cached-format").toFile
    try {
      val path = new java.io.File(directory, "responses").toString
      copied.write.session(spark).save(path)
      val loaded = OpenAIResponses.read.session(spark).load(path)
      assert(body(loaded.prepareEntity, input("loaded")).compactPrint.contains("\"name\":\"copied\""))
      assert(loaded.getResponseFormat == copied.getResponseFormat)
    } finally {
      FileUtils.deleteDirectory(directory)
    }
  }

  test("Copy and save-load rebuild request caches from the current format") {
    val stage = chat.setResponseSchema(schema(Seq("before")), "before")
    body(stage.prepareEntity, input("warm"))
    val copied = stage.copy(ParamMap.empty) match {
      case value: OpenAIChatCompletion => value.setResponseSchema(schema(Seq("after")), "after")
      case other => fail(s"Unexpected copied stage type: ${other.getClass.getName}")
    }
    assert(body(copied.prepareEntity, input("copy")).compactPrint.contains("\"name\":\"after\""))
    assert(body(stage.prepareEntity, input("original")).compactPrint.contains("\"name\":\"before\""))
    val directory = Files.createTempDirectory("openai-cached-format").toFile
    try {
      val path = new java.io.File(directory, "chat").toString
      copied.write.session(spark).save(path)
      val loaded = OpenAIChatCompletion.read.session(spark).load(path)
      assert(body(loaded.prepareEntity, input("loaded")).compactPrint.contains("\"name\":\"after\""))
      assert(loaded.getResponseFormat == copied.getResponseFormat)
    } finally {
      FileUtils.deleteDirectory(directory)
    }
  }
}
