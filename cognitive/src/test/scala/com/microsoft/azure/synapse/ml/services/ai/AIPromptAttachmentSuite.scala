// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.io.http.HTTPTransformer
import com.microsoft.azure.synapse.ml.services.openai.OpenAIPrompt
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSInputStream, FileSystem, Path, RawLocalFileSystem}
import org.apache.spark.SparkException
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{StringType, StructType}
import spray.json._

import java.io.File
import java.net.URI
import java.util.concurrent.{CancellationException, ConcurrentLinkedQueue}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._
import scala.util.Using

class AIPromptCancelledAttachmentFileSystem extends RawLocalFileSystem {
  override def getUri: URI = URI.create("aipromptcancel:///")

  override def open(path: Path, bufferSize: Int): FSDataInputStream =
    if (path.getName == "without-message.png") throw new CancellationException()
    else throw new CancellationException("private cancellation details")
}

class TestableAIPromptAttachmentDefaults(override val uid: String) extends AIPrompt(uid) {
  def this() = this(Identifiable.randomUID("TestableAIPromptAttachmentDefaults"))
  def defaultLimit(value: Double): this.type = setDefault(fileSizeLimitMB -> value)
}

class TestableLegacyAttachmentDefaults(override val uid: String) extends OpenAIPrompt(uid) {
  def this() = this(Identifiable.randomUID("TestableLegacyAttachmentDefaults"))
  setDefault(fileSizeLimitMB -> 0.000001)
}

class AIPromptBoundedAttachmentFileSystem extends RawLocalFileSystem {
  val bytesRead = new AtomicInteger()
  val closed = new AtomicInteger()
  override def getUri: URI = URI.create("aipromptbounded:///")

  override def open(path: Path, bufferSize: Int): FSDataInputStream = new FSDataInputStream(new FSInputStream {
    private val size = path.getName.stripSuffix(".png").toInt
    private var position = 0
    override def getPos: Long = position
    override def seek(value: Long): Unit = throw new UnsupportedOperationException()
    override def seekToNewSource(value: Long): Boolean = false
    override def read(): Int = if (position == size) -1 else {
      position += 1
      bytesRead.incrementAndGet()
      1
    }
    override def read(buffer: Array[Byte], offset: Int, length: Int): Int = {
      if (length == 0) 0 else if (position == size) -1 else {
        val count = math.min(length, size - position)
        java.util.Arrays.fill(buffer, offset, offset + count, 1.toByte)
        position += count
        bytesRead.addAndGet(count)
        count
      }
    }
    override def close(): Unit = {
      closed.incrementAndGet()
      super.close()
    }
  })
}

object AIPromptSessionAttachmentFileSystem {
  val Instances = new ConcurrentLinkedQueue[AIPromptSessionAttachmentFileSystem]()
}

class AIPromptSessionAttachmentFileSystem extends AIPromptBoundedAttachmentFileSystem {
  override def getUri: URI = URI.create("aipromptsession:///")
  override def initialize(uri: URI, conf: Configuration): Unit = {
    require(conf.get("aiprompt.test.attachment.marker") == "session-only", "Missing session filesystem configuration")
    super.initialize(uri, conf)
    AIPromptSessionAttachmentFileSystem.Instances.add(this)
  }
}

class AIPromptAttachmentSuite extends AIPromptTestBase {
  Seq("chat_completions", "responses").foreach { api =>
    test(s"$api attachment size limits reject invalid Param bindings before HTTP") {
      val reply = if (api == "chat_completions")
        """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
      else """{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}"""
      val data = spark.createDataFrame(Seq(Row("data:text/plain;base64,aGVsbG8=")).asJava,
        new StructType().add("asset", StringType))
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val base = new TestableAIPromptAttachmentDefaults()
          .setApiType(api).setUrl(server.url + "/v1").setModel("model")
          .setPromptTemplate("{asset}").setColumnTypes(Map("asset" -> "path")).setErrorCol("error")
        for {
          limit <- Seq(0.0, -1.0, Double.NaN)
          binding <- Seq("direct", "copy", "default")
        } {
          val stage = binding match {
            case "copy" => base.copy(ParamMap(base.fileSizeLimitMB -> limit))
            case "default" => base.defaultLimit(limit).copy(ParamMap.empty)
            case "direct" =>
              val direct = base.copy(ParamMap.empty)
              direct.set(direct.fileSizeLimitMB, limit)
          }
          assert(intercept[IllegalArgumentException](stage.transformSchema(data.schema))
            .getMessage.contains("fileSizeLimitMB must be positive"))
          assert(intercept[IllegalArgumentException](stage.transform(data))
            .getMessage.contains("fileSizeLimitMB must be positive"))
          assert(server.requests.isEmpty)
        }
        assert(base.setFileSizeLimitMB(0.001).transform(data).head().getAs[Row]("error") == null)
        val oversized = base.setFileSizeLimitMB(0.000001).transform(data).head().getAs[Row]("error")
        assert(AIJson.objectString(oversized.getString(0)).fields("code") == JsString("invalid_request"))
        assert(server.requests.size() == 1)
      }
    }

    test(s"$api applies default attachment limits without making them explicit") {
      import spark.implicits._
      val data = Seq("data:text/plain;base64,aGVsbG8=").toDF("asset")
      val reply = if (api == "chat_completions")
        """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
      else """{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}"""
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        val stage = new TestableAIPromptAttachmentDefaults().defaultLimit(0.000001)
          .setApiType(api).setUrl(server.url + "/v1").setModel("model")
          .setPromptTemplate("{asset}").setColumnTypes(Map("asset" -> "path")).setErrorCol("error")
        val error = stage.transform(data).head().getAs[Row]("error")
        assert(error != null && AIJson.objectString(error.getString(0)).fields("code") == JsString("invalid_request"))
        assert(!stage.isSet(stage.fileSizeLimitMB) && server.requests.isEmpty)
        val explicit = stage.copy(ParamMap(stage.fileSizeLimitMB -> 0.001))
        assert(explicit.transform(data).head().getAs[Row]("error") == null)
        assert(stage.defaultLimit(Double.PositiveInfinity).transform(data).head().getAs[Row]("error") == null)
        val legacy = new TestableLegacyAttachmentDefaults().setApiType(api).setUrl(server.url + "/v1")
          .setDeploymentName("model").setSubscriptionKey("unused")
          .setPromptTemplate("{asset}").setColumnTypes(Map("asset" -> "path")).setErrorCol("error")
        assert(legacy.transform(data).head().getAs[Row]("error") == null && server.requests.size() == 3)
      }
    }

    test(s"$api bounds filesystem attachment reads to the configured bytes plus one sentinel") {
      import spark.implicits._
      val location = URI.create("aipromptbounded:///")
      val conf = new Configuration()
      conf.setClass("fs.aipromptbounded.impl", classOf[AIPromptBoundedAttachmentFileSystem], classOf[FileSystem])
      val reply = if (api == "chat_completions")
        """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
      else """{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}"""
      Using.resource(FileSystem.get(location, conf)) { fs =>
        val recorded = fs.asInstanceOf[AIPromptBoundedAttachmentFileSystem]
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          Seq(false, true).foreach { default =>
            val stage = new TestableAIPromptAttachmentDefaults()
              .setApiType(api).setUrl(server.url + "/v1").setModel("model")
              .setPromptTemplate("{asset}").setColumnTypes(Map("asset" -> "path")).setErrorCol("error")
            val limit = 16.0 / (1024 * 1024)
            if (default) stage.defaultLimit(limit) else stage.setFileSizeLimitMB(limit)
            Seq(32, 16).foreach { size =>
              recorded.bytesRead.set(0)
              recorded.closed.set(0)
              val error = stage.transform(Seq(s"${location}$size.png").toDF("asset")).head().getAs[Row]("error")
              assert(recorded.bytesRead.get() == math.min(size, 17) && recorded.closed.get() == 1)
              if (size == 32) {
                assert(error != null)
                val response = error.getString(0)
                assert(AIJson.objectString(response).fields("code") == JsString("invalid_request"))
                assert(!response.contains(location.toString) && !response.contains("32.png"))
                assert(response.contains("16-byte limit") && !response.contains("0.00MB"))
              } else assert(error == null)
            }
          }
          assert(server.requests.size() == 2)
        }
      }
    }

    Seq(None, Some(16.0 / (1024 * 1024)), Some(Double.PositiveInfinity)).foreach { limit =>
      test(s"$api reads cold filesystems from session configuration with limit=$limit") {
        val session = spark.newSession()
        import session.implicits._
        session.conf.set("fs.aipromptsession.impl", classOf[AIPromptSessionAttachmentFileSystem].getName)
        session.conf.set("aiprompt.test.attachment.marker", "session-only")
        val location = s"aipromptsession://${java.util.UUID.randomUUID()}/"
        val reply = if (api == "chat_completions")
          """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
        else """{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}"""
        try {
          assert(AIPromptSessionAttachmentFileSystem.Instances.isEmpty)
          Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
            val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
              .setPromptTemplate("{asset}").setColumnTypes(Map("asset" -> "path")).setErrorCol("error")
            limit.foreach(stage.setFileSizeLimitMB)
            Seq(16, 32).foreach { size =>
              AIPromptSessionAttachmentFileSystem.Instances.asScala.foreach { fs =>
                fs.bytesRead.set(0)
                fs.closed.set(0)
              }
              val result = stage.transform(Seq(s"${location}$size.png").toDF("asset"))
              assert(AIPromptSessionAttachmentFileSystem.Instances.asScala.forall(_.bytesRead.get() == 0))
              session.conf.set("aiprompt.test.attachment.marker", "changed-after-transform")
              val error = try result.head().getAs[Row]("error") finally {
                session.conf.set("aiprompt.test.attachment.marker", "session-only")
              }
              assert(AIPromptSessionAttachmentFileSystem.Instances.size() == 1)
              val fs = AIPromptSessionAttachmentFileSystem.Instances.peek()
              val overflow = limit.exists(value => !value.isInfinity) && size == 32
              assert(fs.bytesRead.get() == (if (overflow) 17 else size) && fs.closed.get() == 1)
              if (overflow) {
                assert(error != null && AIJson.objectString(error.getString(0)).fields("code") ==
                  JsString("invalid_request"))
              } else assert(error == null)
            }
            assert(server.requests.size() == (if (limit.exists(value => !value.isInfinity)) 1 else 2))
          }
        } finally {
          AIPromptSessionAttachmentFileSystem.Instances.asScala.foreach(_.close())
          AIPromptSessionAttachmentFileSystem.Instances.clear()
        }
      }
    }

    test(s"$api buffers generated attachment messages only when retained in output") {
      val image = "data:image/png;base64,AQID"
      val data = spark.createDataFrame(Seq(Row(image)).asJava, new StructType().add("asset", StringType))
      val reply = if (api == "chat_completions")
        """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
      else """{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}"""
      Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
        Seq(false, true).foreach { drop =>
          val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
            .setPromptTemplate("Review {asset}").setColumnTypes(Map("asset" -> "path"))
            .setOutputCol("answer").setErrorCol("error").setDropPrompt(drop)
          val http = new HTTPTransformer() {
            override private[ml] def transformCancellable(dataset: Dataset[_]): DataFrame = {
              val generated = dataset.select(AIColumnPlan.column(s"${stage.uid}_generated_messages")).head()
              assert(generated.isNullAt(0) == drop)
              if (!drop) assert(generated.getSeq[Row](0).exists(_.toString.contains(image)))
              super.transformCancellable(dataset)
            }
          }
          val result = stage.infer(data, http)
          assert(result.schema == stage.transformSchema(data.schema))
          val row = result.head()
          assert(row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null)
          if (!drop) assert(row.getAs[Seq[Row]](stage.getMessagesCol).exists(_.toString.contains(image)))
          else assert(!result.columns.contains(stage.getMessagesCol))
        }
        val bodies = server.requests.asScala.map(_._2).toVector
        assert(bodies.size == 2 && bodies.distinct.size == 1 && bodies.head.contains(image))
      }
    }

    test(s"$api rejects resolver-equivalent columnTypes keys before HTTP") {
      val caseSensitive = spark.conf.get("spark.sql.caseSensitive")
      val schema = new StructType().add("asset", StringType)
      val data = spark.createDataFrame(Seq(Row("data:image/png;base64,AQID")).asJava, schema)
      try {
        spark.conf.set("spark.sql.caseSensitive", "false")
        Using.resource(new AIRecordingServer((_, _) => 200 -> "{}")) { server =>
          val base = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
            .setPromptTemplate("Review {asset}").setOutputCol("answer").setErrorCol("error")
          Seq("path", "text").foreach { kind =>
            val stage = base.copy(ParamMap.empty).setColumnTypes(Map("asset" -> kind, "ASSET" -> "path"))
            Seq[() => Any](() => stage.transformSchema(schema), () => stage.transform(data)).foreach { call =>
              val error = intercept[IllegalArgumentException](call())
              assert(error.getMessage.contains("Ambiguous columnTypes keys"))
              assert(server.requests.isEmpty)
            }
          }
        }
      } finally spark.conf.set("spark.sql.caseSensitive", caseSensitive)
    }

    test(s"$api preserves distinct case-sensitive columnTypes keys and attachment order") {
      val caseSensitive = spark.conf.get("spark.sql.caseSensitive")
      val first = "data:image/png;base64,AQID"
      val second = "data:image/png;base64,BAUG"
      val schema = new StructType().add("asset", StringType).add("ASSET", StringType)
      val data = spark.createDataFrame(Seq(Row(first, second)).asJava, schema)
      val reply = if (api == "chat_completions")
        """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
      else """{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}"""
      try {
        spark.conf.set("spark.sql.caseSensitive", "true")
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
            .setPromptTemplate("Review {asset} and {ASSET}")
            .setColumnTypes(Map("ASSET" -> "path", "asset" -> "path"))
            .setOutputCol("answer").setErrorCol("error")
          val result = stage.transform(data)
          assert(result.schema == stage.transformSchema(schema))
          val row = result.head()
          assert(row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null)
          assert(server.requests.size() == 1)
          val key = if (api == "chat_completions") "messages" else "input"
          val imageType = if (api == "chat_completions") "image_url" else "input_image"
          val content = AIJson.objectString(server.requests.peek()._2).fields(key).asInstanceOf[JsArray]
            .elements.last.asJsObject.fields("content").asInstanceOf[JsArray].elements
          val images = content.map(_.asJsObject.fields).filter(_("type") == JsString(imageType))
            .map(fields => if (api == "chat_completions") fields("image_url").asJsObject.fields("url")
              else fields("image_url"))
          assert(images == Seq(JsString(first), JsString(second)))
        }
      } finally spark.conf.set("spark.sql.caseSensitive", caseSensitive)
    }
  }

  Seq("chat_completions", "responses").foreach { api =>
    Seq("private-attachment.png", "without-message.png").foreach { filename =>
      test(s"legacy attachment CancellationException in $api for $filename remains sanitized") {
        import spark.implicits._
        val location = URI.create(s"aipromptcancel:///$filename")
        val conf = new Configuration()
        conf.setClass("fs.aipromptcancel.impl", classOf[AIPromptCancelledAttachmentFileSystem], classOf[FileSystem])
        Using.resource(FileSystem.get(location, conf)) { _ =>
          Using.resource(new AIRecordingServer((_, _) => 200 -> "{}")) { server =>
            val stage = new OpenAIPrompt().setApiType(api).setUrl(server.url + "/v1")
              .setDeploymentName("model").setSubscriptionKey("unused")
              .setPromptTemplate("Review {asset}").setColumnTypes(Map("asset" -> "path"))
              .setOutputCol("answer").setErrorCol("error")
            val row = stage.transform(Seq(location.toString).toDF("asset")).head()
            val error = row.getAs[Row]("error")
            assert(error != null)
            assert(error.getAs[String]("response") == s"Unable to read attachment '$filename'")
            assert(row.getAs[String]("answer") == null && server.requests.isEmpty)
          }
        }
      }
    }

    test(s"$api propagates attachment CancellationException instead of committing a row error") {
      import spark.implicits._
      val location = URI.create("aipromptcancel:///private-attachment.png")
      val conf = new Configuration()
      conf.setClass("fs.aipromptcancel.impl", classOf[AIPromptCancelledAttachmentFileSystem], classOf[FileSystem])
      Using.resource(FileSystem.get(location, conf)) { _ =>
        Using.resource(new AIRecordingServer((_, _) => 200 -> "{}")) { server =>
          val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
            .setPromptTemplate("Review {asset}").setColumnTypes(Map("asset" -> "path"))
            .setOutputCol("answer").setErrorCol("error")
          val failure = intercept[SparkException] {
            stage.transform(Seq(location.toString).toDF("asset")).collect()
          }
          val causes = Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null).toVector
          assert(causes.exists(_.isInstanceOf[CancellationException]))
          assert(!causes.exists(error => Option(error.getMessage).exists(_.contains("private cancellation details"))))
          val cancelled = causes.collectFirst { case error: CancellationException => error }.get
          assert(cancelled.getMessage == "Attachment read canceled" && cancelled.getCause == null)
          assert(server.requests.isEmpty)
        }
      }
    }

    test(s"$api distinguishes attachment IO from invalid content without leaking paths or sending requests") {
      import spark.implicits._
      val missing = new File(path(), "private-attachment.png").toURI.toString
      val data = Seq("missing" -> missing, "invalid" -> "data:image/png;base64,@@@",
        "wrong_fs" -> "file://invalid-host/private-attachment.png").toDF("kind", "asset")
      Using.resource(new AIRecordingServer((_, _) => 200 -> "{}")) { server =>
        val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
          .setPromptTemplate("Review {asset}").setColumnTypes(Map("asset" -> "path"))
          .setOutputCol("answer").setErrorCol("error")
        val rows = stage.transform(data).collect().map(row => row.getAs[String]("kind") -> row).toMap
        Seq("missing", "wrong_fs").foreach { kind =>
          val readError = rows(kind).getAs[Row]("error").getString(0)
          assert(AIJson.objectString(readError).fields("code") == JsString("input_io_error"))
          assert(!readError.contains(missing) && !readError.contains("private-attachment"))
        }
        assert(AIJson.objectString(rows("invalid").getAs[Row]("error").getString(0))
          .fields("code") == JsString("invalid_request"))
        assert(rows.values.forall(_.getAs[String]("answer") == null) && server.requests.isEmpty)
      }
    }
  }

    Seq("chat_completions", "responses").foreach { api =>
      test(s"missing attachment placeholders preserve usable $api inputs") {
        val inline = "data:image/png;base64,AQID"
        val absent = Option.empty[String].orNull
        val rows = Seq(absent, "", " \t ").flatMap { missing =>
          Seq(Row("hello", inline, missing), Row("hello", missing, missing))
        } :+ Row(absent, inline, absent)
        val schema = new StructType().add("text", StringType).add("asset", StringType)
          .add("optional", StringType)
        val data = spark.createDataFrame(rows.asJava, schema)
        val reply = if (api == "chat_completions")
          """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"}}]}"""
        else """{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}"""
        Using.resource(new AIRecordingServer((_, _) => 200 -> reply)) { server =>
          val stage = new AIPrompt().setApiType(api).setUrl(server.url + "/v1").setModel("model")
            .setPromptTemplate("Review {asset}; optional={optional}; text={text}")
            .setColumnTypes(Map("asset" -> "path", "optional" -> "path"))
            .setOutputCol("answer").setErrorCol("error")
          val result = stage.transform(data).collect()
          assert(result.length == 7 && result.count(_.getAs[String]("answer") == "answer") == 3)
          val errors = result.flatMap(row => Option(row.getAs[Row]("error")))
          assert(errors.length == 4 && errors.forall(error =>
            AIJson.objectString(error.getString(0)).fields("code") == JsString("missing_input")))
          assert(server.requests.size() == 3)
          server.requests.asScala.foreach { case (_, body) =>
            val key = if (api == "chat_completions") "messages" else "input"
            val content = AIJson.objectString(body).fields(key).asInstanceOf[JsArray].elements.last
              .asJsObject.fields("content").asInstanceOf[JsArray].elements
            assert(content.exists(_.asJsObject.fields.get("text").contains(
              JsString("Review attachment.png; optional=; text=hello"))))
          }
      }
    }
  }
}
