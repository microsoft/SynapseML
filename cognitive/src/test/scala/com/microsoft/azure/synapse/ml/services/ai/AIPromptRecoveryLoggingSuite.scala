// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.io.http.{HTTPTransformer, HandlingUtils}
import com.microsoft.azure.synapse.ml.services.openai.{OpenAIChatCompletion, OpenAIDecisions, OpenAIResponses}
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.{Filter, LogEvent, LoggerContext}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.apache.spark.ml.{Pipeline, PipelineModel, Serializer}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}

import java.io.{PrintWriter, StringWriter}
import java.net.InetSocketAddress
import java.nio.file.Files
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._
import scala.util.Using

class AIPromptRecoveryLoggingSuite extends AIPromptOutputTestBase {
  private val chatReply =
    """{"choices":[{"index":0,"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}]}"""

  private def input: DataFrame = spark.createDataFrame(
    Seq(Row("hello")).asJava, new StructType().add("text", StringType))

  private def withLogs(action: ConcurrentLinkedQueue[String] => Unit): Unit = {
    val logged = new ConcurrentLinkedQueue[String]()
    val layout = PatternLayout.newBuilder().withPattern("%m%n%throwable").build()
    val noFilter = Option.empty[Filter].orNull
    val appender = new AbstractAppender("aiprompt-test", noFilter, layout, false, Array.empty[Property]) {
      override def append(event: LogEvent): Unit = logged.add(layout.toSerializable(event))
    }
    val root = LogManager.getContext(false).asInstanceOf[LoggerContext].getRootLogger
    val loggers = Seq(classOf[AIPrompt], classOf[OpenAIChatCompletion], classOf[OpenAIResponses],
      classOf[HTTPTransformer])
      .map(cls => LogManager.getLogger(cls).asInstanceOf[org.apache.logging.log4j.core.Logger])
    val levels = loggers.map(logger => logger -> logger.getLevel)
    appender.start()
    root.addAppender(appender)
    loggers.foreach(_.setLevel(Level.INFO))
    try action(logged) finally {
      levels.foreach { case (logger, level) => logger.setLevel(level) }
      root.removeAppender(appender)
      appender.stop()
    }
  }

  test("durable batches construct one HTTP stage per public run without sharing it between runs") {
    delta()
    val data = spark.createDataFrame(
      Seq(Row("first"), Row("second"), Row("third")).asJava, new StructType().add("text", StringType))
    withServer { server =>
      val stage = prompt(path(), server).setOutputBatchSize(1)
      Seq(1, 2).foreach { concurrency =>
        stage.setOutputPath(path()).setConcurrency(concurrency)
        withLogs { logged =>
          val saved = stage.transform(data)
          assert(saved.count() == 3 && saved.filter("error IS NOT NULL").count() == 0)
          val constructors = logged.asScala.count(message =>
            message.contains(".HTTPTransformer") && message.contains("\"method\":\"constructor\""))
          assert(constructors == 1, s"Expected one HTTP constructor per run, observed $constructors")
          assert(AIPrompt.getOutputStatus(spark, stage.getOutputPath).pendingCount == 0)
        }
      }
      assert(server.requests.get() == 6)
    }
  }
  test("resume failures use the public recovery execution logger") {
    val stage = new AIPrompt()
    withLogs { logged =>
      val error = intercept[IllegalArgumentException](stage.resume(spark))
      assert(error.getMessage.contains("resume requires outputPath") &&
        error.getMessage.contains("original run directory"))
      assert(logged.asScala.exists(_.contains("\"method\":\"resume\"")))
    }
  }

  for (method <- Seq("resume"); succeeds <- Seq(false, true)) {
    test(s"public $method logging uses the execution session with succeeds=$succeeds") {
      if (method == "resume" && succeeds) delta()
      val original = SparkSession.getActiveSession
      try {
        val previous = spark.newSession()
        val execution = spark.newSession()
        previous.conf.set("trident.artifact.id", "synthetic-previous-artifact")
        execution.conf.set("trident.artifact.id", "synthetic-execution-artifact")
        val data = execution.createDataFrame(
          Seq(Row("hello")).asJava, new StructType().add("text", StringType))
        withServer { server =>
          Seq(Some(previous), None).foreach { prior =>
            val stage = prompt(path(), server)
            if (method == "transform") {
              stage.clear(stage.outputPath)
              if (!succeeds) stage.setInputCol("missing")
            } else if (succeeds) {
              assert(stage.transform(data).count() == 1)
            } else stage.clear(stage.outputPath)
            prior match {
              case Some(session) => SparkSession.setActiveSession(session)
              case None => SparkSession.clearActiveSession()
            }
            withLogs { logged =>
              def invoke(): DataFrame = if (method == "transform") stage.transform(data) else stage.resume(execution)
              if (succeeds) {
                val result = invoke()
                assert(SparkSession.getActiveSession == prior && result.sparkSession == execution)
                assert(result.count() == 1)
              } else {
                intercept[IllegalArgumentException](invoke())
                assert(SparkSession.getActiveSession == prior)
              }
              val messages = logged.asScala.filter(message =>
                message.contains(s""""modelUid":"${stage.uid}"""") && message.contains(s""""method":"$method""""))
              assert(messages.size == 1)
              assert(messages.head.contains(""""artifactId":"synthetic-execution-artifact""""))
            }
          }
        }
      } finally {
        original match {
          case Some(session) => SparkSession.setActiveSession(session)
          case None => SparkSession.clearActiveSession()
        }
      }
    }
  }
  test("bounded durable recovery never depends on response cookies or resends committed rows") {
    delta()
    val session = spark.newSession()
    session.conf.set("spark.sql.shuffle.partitions", "1")
    val data = session.createDataFrame((1 to 4).map(number => Row(s"row-$number")).asJava,
      new StructType().add("text", StringType)).coalesce(1)
    Using.resource(new AIRecordingServer((_, _) => 200 -> chatReply,
      responseHeaders = Map("Set-Cookie" -> "provider-session=synthetic; Path=/"))) { server =>
      val root = path()
      val stage = new AIPrompt().setUrl(server.url + "/v1").setModel("model").setInputCol("text")
        .setOutputCol("answer").setErrorCol("error").setConcurrency(1).setSubscriptionKey("synthetic-first")
        .setOutputPath(root).setOutputBatchSize(2)
      intercept[IllegalStateException] {
        AIPromptOutput.transformObserved(stage, data, event =>
          if (event == "batch_committed") throw new IllegalStateException("stop after committed batch"))
      }
      val status = AIPrompt.getOutputStatus(session, root)
      assert(status.committedCount == 2 && status.pendingCount == 2 && server.requests.size() == 2)
      AIPrompt.releaseOutputOwner(session, root, status.ownerToken.get, confirmStopped = true)
      stage.setSubscriptionKey("synthetic-rotated")
      val rows = stage.resume(session).collect()
      assert(rows.length == 4 && rows.forall(row =>
        row.getAs[String]("answer") == "answer" && row.getAs[Row]("error") == null))
      assert(stage.resume(session).count() == 4 && server.requests.size() == 4)
      assert(server.requests.asScala.map(_._2).toSet.size == 4)
      val headers = server.headers.asScala.toVector
      assert(headers.map(_("api-key")) ==
        Vector.fill(2)("synthetic-first") ++ Vector.fill(2)("synthetic-rotated"))
      assert(headers.forall(!_.contains("cookie")))
      assert(AIPrompt.getOutputStatus(session, root).pendingCount == 0)
    }
  }
}
