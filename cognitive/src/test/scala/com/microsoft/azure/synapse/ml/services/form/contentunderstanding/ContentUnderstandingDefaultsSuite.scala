// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.form.contentunderstanding

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.services.ai.{AIDefaults, AIDefaultsScope}
import com.microsoft.azure.synapse.ml.services.contentunderstanding.ContentUnderstanding
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import spray.json._

import java.nio.file.Files

class ContentUnderstandingDefaultsSuite extends TestBase {
  import ContentUnderstandingFixtures._
  import ContentUnderstandingStub.withReplies

  private def withDefaults(test: AIDefaultsScope => Unit): Unit = {
    val defaults = AIDefaults.forApi("content_understanding")
    val restore = Seq(
      defaults.getEndpoint.map(v => () => defaults.setEndpoint(v)),
      defaults.getApiKey.map(v => () => defaults.setApiKey(v)),
      defaults.getApiVersion.map(v => () => defaults.setApiVersion(v)),
      defaults.getAnalyzerId.map(v => () => defaults.setAnalyzerId(v)),
      defaults.getModelDeployments.map(v => () => defaults.setModelDeployments(v)),
      defaults.getStringEncoding.map(v => () => defaults.setStringEncoding(v)),
      defaults.getProcessingLocation.map(v => () => defaults.setProcessingLocation(v)),
      defaults.getAllowInputTruncation.map(v => () => defaults.setAllowInputTruncation(v)))
    defaults.reset()
    try test(defaults) finally {
      defaults.reset()
      restore.flatten.foreach(_())
    }
  }

  private def input = spark.range(1).selectExpr("cast(id as string) as id")

  private def stage = new ContentUnderstanding().setDocumentBytes(Array[Byte](1))
    .setOutputCol("analysis").setErrorCol("error").setPollingDelay(0)

  test("transform snapshots scoped defaults without changing the configured stage") {
    withDefaults { defaults =>
      withReplies(Seq(ContentUnderstandingStubReply(200, Succeeded))) { server =>
        defaults.setEndpoint(server.endpoint).setApiKey("scoped-key").setAnalyzerId("custom-analyzer")
          .setApiVersion("2026-06-01-preview").setModelDeployments(Map("gpt-4.1" -> "deployment"))
          .setStringEncoding("utf8").setProcessingLocation("global").setAllowInputTruncation(false)
        val analyzer = stage
        val schema = analyzer.transformSchema(input.schema)
        val result = analyzer.transform(input)
        defaults.setAnalyzerId("later-analyzer").setApiKey("later-key")
        assert(result.schema == schema)
        assert(result.collect().head.getAs[Row]("analysis").getAs[String]("status") == "Succeeded")
        val request = server.requests.head
        assert(request.path.endsWith("/custom-analyzer:analyze"))
        assert(request.headers("ocp-apim-subscription-key") == "scoped-key")
        assert(request.query.contains("api-version=2026-06-01-preview"))
        assert(request.query.contains("allowInputTruncation=false"))
        assert(request.query.contains("stringEncoding=utf8"))
        assert(request.query.contains("processingLocation=global"))
        assert(request.body.parseJson.asJsObject.fields("modelDeployments") ==
          JsObject("gpt-4.1" -> JsString("deployment")))
        assert(!analyzer.isSet(analyzer.url) && !analyzer.isSet(analyzer.subscriptionKey))
        assert(!analyzer.isSet(analyzer.analyzerId) && !analyzer.isSet(analyzer.modelDeployments))
      }
    }
  }

  test("explicit credentials and analyzer columns override scoped defaults") {
    withDefaults { defaults =>
      withReplies(Seq(ContentUnderstandingStubReply(200, Succeeded))) { server =>
        defaults.setEndpoint(server.endpoint).setApiKey("unused-key").setAnalyzerId("unused-analyzer")
        stage.setAADToken("explicit-token").setAnalyzerIdCol("analyzer")
          .transform(input.selectExpr("*", "'row-analyzer' as analyzer")).collect()
        val request = server.requests.head
        assert(request.path.endsWith("/row-analyzer:analyze"))
        assert(request.headers("authorization") == "Bearer explicit-token")
        assert(!request.headers.contains("ocp-apim-subscription-key"))
      }
    }
  }

  test("scoped BYO credentials replace the implicit Fabric endpoint without looking up Fabric auth") {
    withDefaults { defaults =>
      withReplies(Seq(ContentUnderstandingStubReply(200, Succeeded))) { server =>
        defaults.setEndpoint(server.endpoint).setApiKey("scoped-key")
        val analyzer = new TestableFabricContentUnderstanding("https://fabric.example", rejectTokenLookup = true)
          .setDocumentBytes(Array[Byte](1)).setOutputCol("analysis")
        assert(!analyzer.isSet(analyzer.url) && analyzer.get(analyzer.url).isEmpty)
        assert(analyzer.getUrl.startsWith("https://fabric.example/"))
        assert(defaults.applyTo(analyzer).getSubscriptionKey == "scoped-key")
        assert(analyzer.transform(input).head().getAs[Row]("analysis").getAs[String]("status") == "Succeeded")
        assert(server.requests.head.headers("ocp-apim-subscription-key") == "scoped-key")
        assert(!server.requests.head.headers.contains("authorization"))
        assert(!analyzer.isSet(analyzer.url) && !analyzer.isSet(analyzer.subscriptionKey))
      }
    }
  }

  test("save and load retain unset parameters while applyTo explicitly persists a snapshot") {
    withDefaults { defaults =>
      withReplies(Seq(ContentUnderstandingStubReply(200, Succeeded))) { server =>
        val directory = Files.createTempDirectory("cu-defaults-").toFile
        try {
          defaults.setEndpoint(server.endpoint).setApiKey("first-key").setAnalyzerId("first-analyzer")
          val analyzer = stage
          val unboundPath = directory.toPath.resolve("unbound").toString
          val snapshotPath = directory.toPath.resolve("snapshot").toString
          analyzer.copy(ParamMap.empty).write.save(unboundPath)
          defaults.applyTo(analyzer).write.save(snapshotPath)
          defaults.setApiKey("second-key").setAnalyzerId("second-analyzer")
          val unbound = ContentUnderstanding.load(unboundPath)
          assert(!unbound.isSet(unbound.subscriptionKey) && !unbound.isSet(unbound.analyzerId))
          unbound.transform(input).collect()
          ContentUnderstanding.load(snapshotPath).transform(input).collect()
          assert(server.requests.map(_.headers("ocp-apim-subscription-key")) == Seq("second-key", "first-key"))
          assert(server.requests.map(_.path.split("/").last) ==
            Seq("second-analyzer:analyze", "first-analyzer:analyze"))
        } finally FileUtils.deleteDirectory(directory)
      }
    }
  }

  test("an explicit endpoint on another origin never receives the scoped key") {
    withDefaults { defaults =>
      withReplies(Seq(ContentUnderstandingStubReply(200, Succeeded))) { server =>
        defaults.setEndpoint("https://another-resource.example").setApiKey("unused-key")
        stage.setEndpoint(server.endpoint).transform(input).collect()
        assert(!server.requests.head.headers.contains("ocp-apim-subscription-key"))
        assert(!server.requests.head.headers.contains("authorization"))
      }
    }
  }

  test("invalid scoped request settings fail before creating a journal or contacting the service") {
    withDefaults { defaults =>
      withReplies(Seq(Accepted)) { server =>
        val directory = Files.createTempDirectory("cu-defaults-invalid-").toFile
        val path = directory.toPath.resolve("results")
        try {
          defaults.setEndpoint(server.endpoint).setApiVersion("invalid-version")
          val error = intercept[IllegalArgumentException] {
            stage.writeToPath(input, "id", path.toString, "parquet", 1)
          }
          assert(error.getMessage.contains("apiVersion"))
          assert(!Files.exists(path))
          assert(server.requests.isEmpty)
        } finally FileUtils.deleteDirectory(directory)
      }
    }
  }

  test("durable writes use scoped defaults and resume without submitting completed documents again") {
    withDefaults { defaults =>
      withReplies(Seq(Accepted, ContentUnderstandingStubReply(200, Succeeded))) { server =>
        val directory = Files.createTempDirectory("cu-defaults-journal-").toFile
        val path = directory.toPath.resolve("results").toString
        try {
          defaults.setEndpoint(server.endpoint).setApiKey("scoped-key")
          val analyzer = stage
          analyzer.writeToPath(input, "id", path, "parquet", 1)
          assert(analyzer.writeToPath(input, "id", path, "parquet", 1).head().getAs[String]("status") == "Succeeded")
          assert(server.requests.map(_.method) == Seq("POST", "GET"))
          assert(server.requests.forall(_.headers("ocp-apim-subscription-key") == "scoped-key"))
          assert(!analyzer.isSet(analyzer.url) && !analyzer.isSet(analyzer.subscriptionKey))
        } finally FileUtils.deleteDirectory(directory)
      }
    }
  }

  test("driver analyzer management resolves scoped defaults without changing resource defaults") {
    withDefaults { defaults =>
      withReplies(Seq(ContentUnderstandingStubReply(200, """{"status":"ready"}"""))) { server =>
        defaults.setEndpoint(server.endpoint).setApiKey("scoped-key").setAnalyzerId("custom-analyzer")
        val analyzer = stage
        analyzer.getAnalyzer()
        analyzer.createAnalyzer("""{"baseAnalyzerId":"prebuilt-read"}""", allowReplace = false)
        assert(server.requests.map(_.method) == Seq("GET", "PUT"))
        assert(server.requests.forall(_.path.endsWith("/custom-analyzer")))
        assert(server.requests.forall(_.headers("ocp-apim-subscription-key") == "scoped-key"))
        assert(!analyzer.isSet(analyzer.url) && !analyzer.isSet(analyzer.subscriptionKey))
      }
    }
  }
}
