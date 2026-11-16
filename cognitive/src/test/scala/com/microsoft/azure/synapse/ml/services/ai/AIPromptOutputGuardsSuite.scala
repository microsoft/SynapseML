// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

import java.nio.file.{Files, Paths}
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}
import scala.util.Try

class AIPromptOutputGuardsSuite extends AIPromptOutputTestBase {
  private def input: DataFrame = {
    val session = spark
    import session.implicits._
    Seq("first", "second").toDF("text")
  }

  for (resume <- Seq(false, true)) {
    test(s"concurrent local owners arbitrate before inference with resume=$resume") {
      delta()
      withServer { server =>
        val root = path()
        val data = input
        def stage: AIPrompt = prompt(root, server).setOutputBatchSize(1)
        if (resume) {
          AIPromptFabricRecovery.stopAfterFirstBatch(stage, data)
          val partial = AIPrompt.getOutputStatus(spark, root)
          assert(partial.committedCount == 1 && partial.pendingCount == 1 && server.requests.get() == 1)
          AIPrompt.releaseOutputOwner(spark, root, partial.ownerToken.get, confirmStopped = true)
        }
        val arrived = new CountDownLatch(2)
        val proceed = new CountDownLatch(1)
        val pool = Executors.newFixedThreadPool(2)
        val futures = Seq.fill(2)(pool.submit(new Callable[Try[Long]] {
          override def call(): Try[Long] = Try {
            val event: String => Unit = boundary => {
              if (boundary == (if (resume) "before_acquire" else "before_control_create")) {
                arrived.countDown()
                assert(proceed.await(180, TimeUnit.SECONDS), "Timed out waiting for the competing owner")
              }
            }
            val output = if (resume) AIPromptOutput.resumeObserved(stage, spark, event)
              else AIPromptOutput.transformObserved(stage, data, event)
            output.count()
          }
        }))
        try {
          assert(arrived.await(180, TimeUnit.SECONDS), "Both owners must reach the same transaction boundary")
          proceed.countDown()
          val outcomes = futures.map(_.get(180, TimeUnit.SECONDS))
          assert(outcomes.count(_.isSuccess) == 1, outcomes.toString)
          assert(outcomes.filter(_.isSuccess).map(_.get) == Seq(2L))
          val error = outcomes.find(_.isFailure).get.failed.get
          val diagnostic = if (resume) "AIPrompt ownership conflict" else "AIPrompt initial ownership conflict"
          assert(error.isInstanceOf[IllegalArgumentException] && error.getMessage.contains(diagnostic))
          val status = AIPrompt.getOutputStatus(spark, root)
          assert(status.state == "complete" && status.ownerToken.isEmpty && status.allSucceeded)
          assert(server.requests.get() == 2, "The losing owner must not send or replay any model request")
        } finally {
          proceed.countDown()
          futures.foreach(_.cancel(true))
          pool.shutdownNow()
          assert(pool.awaitTermination(60, TimeUnit.SECONDS), "Competing owners did not stop")
        }
      }
    }
  }

  for (mutation <- Seq("delete", "foreign-key", "version")) {
    test(s"active committed ledger rejects $mutation before another request and retains its owner") {
      delta()
      withServer { server =>
        val root = path()
        val stage = prompt(root, server).setOutputBatchSize(1)
        val boundary = if (mutation == "version") "before_ledger_version_check" else "batch_committed"
        var owner = Option.empty[String]
        var changed = false
        val error = intercept[IllegalArgumentException] {
          AIPromptOutput.transformObserved(stage, input, event => {
            if (event == boundary && !changed) {
              changed = true
              val manifest = AIPromptDelta.control(spark, root)
              owner = manifest.owner
              val table = AIPromptDelta.table(root, "results")
              mutation match {
                case "delete" => spark.sql(s"DELETE FROM $table")
                case "foreign-key" =>
                  val location = AIPromptDelta.child(root, "results")
                  spark.read.format("delta").load(location)
                    .withColumn(manifest.keyColumn, lit(Long.MaxValue))
                    .write.format("delta").mode("append").save(location)
                case "version" => spark.sql(s"ALTER TABLE $table SET TBLPROPERTIES ('aiprompt.test' = 'changed')")
              }
            }
          })
        }
        val expected = mutation match {
          case "delete" => "AIPrompt committed batch cardinality does not match the selected input keys"
          case "foreign-key" => "AIPrompt results contain unknown row keys"
          case "version" => "AIPrompt committed outcomes changed outside the current owner"
        }
        assert(changed && error.getMessage.contains(expected))
        assert(owner.isDefined && AIPromptDelta.control(spark, root).owner == owner)
        assert(server.requests.get() == (if (mutation == "version") 0 else 1))
        if (mutation == "foreign-key") {
          val manifest = AIPromptDelta.control(spark, root)
          spark.sql(s"DELETE FROM ${AIPromptDelta.table(root, "results")} " +
            s"WHERE `${manifest.keyColumn}` = ${Long.MaxValue}")
        }
        val partial = AIPrompt.getOutputStatus(spark, root)
        assert(partial.committedCount == (if (mutation == "foreign-key") 1 else 0))
        assert(AIPrompt.loadOutput(spark, root).count() == partial.committedCount)
        assert(server.requests.get() == (if (mutation == "version") 0 else 1))
        AIPrompt.releaseOutputOwner(spark, root, owner.get, confirmStopped = true)
        assert(stage.resume(spark).count() == 2)
        val status = AIPrompt.getOutputStatus(spark, root)
        assert(status.state == "complete" && status.ownerToken.isEmpty && status.allSucceeded)
        assert(server.requests.get() == (if (mutation == "delete") 3 else 2),
          "Recovery must send only the rows missing from the repaired committed ledger")
      }
    }
  }

  test("durable runtime rejects unqualified schemes and a missing catalog without accessing storage") {
    delta()
    Seq("s3a", "s3", "gs", "dbfs").foreach { scheme =>
      val error = intercept[IllegalArgumentException] {
        AIPromptDelta.requireRuntime(spark, s"$scheme://synthetic-bucket/run")
      }
      assert(error.getMessage.contains("Other filesystem/LogStore configurations are not qualified"))
    }
    val missingCatalog = spark.newSession()
    missingCatalog.conf.unset("spark.sql.catalog.spark_catalog")
    val root = path()
    val error = intercept[IllegalArgumentException] {
      AIPromptDelta.requireRuntime(missingCatalog, Paths.get(root).toUri.toString)
    }
    assert(error.getMessage.contains("Configure the Delta Spark extension and catalog"))
    assert(!Files.exists(Paths.get(root)))
  }

  test("durable System One JSON state checks recursive resources without a storage dependency") {
    import spark.implicits._
    val stage = new AISystemOne().setStateCol("state").setStateMode("json")
    val external = Seq(
      """{"nested":[{"file_id":"private-state-value"}]}""" -> "restricted fields: file_id",
      """{"image_url":"https://example.invalid/private-state-value"}""" -> "Durable media must be inline",
      """{"previous_response_id":"private-state-value"}""" -> "restricted fields: previous_response_id",
      """{"tools":[{"type":"web_search","query":"private-state-value"}]}""" -> "restricted fields: tools")
    external.foreach { case (state, diagnostic) =>
      val error = intercept[org.apache.spark.SparkException] {
        stage.validateDurableRows(Seq(state).toDF("state"))
      }
      val causes = Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).toVector
      assert(causes.exists(cause => cause.isInstanceOf[IllegalArgumentException] &&
        Option(cause.getMessage).exists(_.contains(diagnostic))))
      assert(causes.forall(cause => !Option(cause.getMessage).exists(_.contains("private-state-value"))))
    }
    stage.validateDurableRows(Seq("not json", "null", """{"file_id":null}""",
      """{"text":"https://example.invalid/inert","image_url":"data:image/png;base64,AQID"}""").toDF("state"))
    stage.setStateMode("text").validateDurableRows(external.map(_._1).toDF("state"))
  }

  test("public durable transform rejects a streaming input before storage or HTTP") {
    withServer { server =>
      val root = path()
      val data = spark.readStream.format("rate").load().selectExpr("CAST(value AS STRING) AS text")
      val error = intercept[IllegalArgumentException](prompt(root, server).transform(data))
      assert(error.getMessage.contains("AIPrompt durable output does not support streaming inputs"))
      assert(!Files.exists(Paths.get(root)) && server.requests.get() == 0)
    }
  }
}
