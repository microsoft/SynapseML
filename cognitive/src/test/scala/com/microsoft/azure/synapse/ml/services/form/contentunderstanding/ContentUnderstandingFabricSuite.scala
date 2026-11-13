// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.form.contentunderstanding

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.services.contentunderstanding.{
  ContentUnderstanding, ContentUnderstandingException, ContentUnderstandingResponse}
import org.apache.commons.io.FileUtils
import org.apache.http.HttpStatus
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructType}

import java.io.File
import java.util.UUID
import scala.collection.JavaConverters._

private[contentunderstanding] class TestableFabricContentUnderstanding(
    trustedRoot: String,
    rejectTokenLookup: Boolean = false,
    rejectTokenRefresh: Boolean = false) extends ContentUnderstanding("fabric-cu-test") {
  setDefaultInternalEndpoint(trustedRoot)

  override protected def fabricEndpoint: Option[String] =
    Some(trustedRoot.stripSuffix("/") + "/cognitive/contentunderstanding/analyzers")

  override protected def currentFabricAuthHeader: String = {
    require(!rejectTokenLookup, "Unexpected Fabric token lookup.")
    "MwcToken test-token"
  }

  override protected def refreshFabricAuthHeader(rejectedHeader: String): String = {
    require(!rejectTokenLookup && !rejectTokenRefresh, "Unexpected Fabric token refresh.")
    "MwcToken refreshed-test-token"
  }
}

class ContentUnderstandingFabricSuite extends TestBase {
  import ContentUnderstandingFixtures.{
    Accepted => accepted, LocationPath => locationPath, Succeeded => succeeded, _}
  import ContentUnderstandingStub.withReplies

  private val fabricRootPath =
    "/webapi/capacities/capacity-1/workloads/ML/AI/Automatic/workspaceid/workspace-1"
  private val fabricGateway = fabricRootPath + "/cognitive"

  private def input: DataFrame =
    spark.createDataFrame(Seq(Row("doc")).asJava, new StructType().add("id", StringType)).coalesce(1)

  private def resultOf(transformer: ContentUnderstanding): ContentUnderstandingResponse =
    ContentUnderstandingResponse.makeFromRowConverter(transformer.transform(input).collect().head.getAs[Row]("result"))

  private def fabricStage(server: ContentUnderstandingStub, rejectTokenLookup: Boolean = false): ContentUnderstanding =
    new TestableFabricContentUnderstanding(server.endpoint + fabricRootPath, rejectTokenLookup)
      .setDocumentBytes(Array[Byte](1)).setPollingDelay(0).setMaxPollAttempts(3)
      .setOutputCol("result").setErrorCol("error")

  private def fabricAccepted: ContentUnderstandingStubReply =
    accepted.copy(headers = accepted.headers.updated("Operation-Location", "$ROOT" + fabricGateway + locationPath))

  test("Fabric defaults preserve the workspace route and implicit authentication through analysis and polling") {
    withReplies(Seq(fabricAccepted, ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded))) { server =>
      val transformer = fabricStage(server).setConcurrency(2)
      assert(!transformer.isSet(transformer.url))
      assert(resultOf(transformer).status == "Succeeded")
      assert(server.requests.map(_.path) ==
        Seq(fabricGateway + AnalyzersPath + "/prebuilt-read:analyze", fabricGateway + ResultsPath + "op-1"))
      assert(server.requests.forall(_.headers("authorization") == "MwcToken test-token"))
      assert(server.requests.forall(!_.headers.contains("x-synapseml-implicit-fabric-auth")))
    }
  }

  test("Fabric refreshes implicit authentication once for a rejected submission and once for a poll") {
    val unauthorized = ContentUnderstandingStubReply(HttpStatus.SC_UNAUTHORIZED, """{"error":{"code":"Expired"}}""")
    withReplies(Seq(unauthorized, fabricAccepted, unauthorized,
      ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded))) { server =>
      assert(resultOf(fabricStage(server)).status == "Succeeded")
      assert(server.requests.map(_.method) == Seq("POST", "POST", "GET", "GET"))
      assert(server.requests.map(_.headers("authorization")) ==
        Seq("MwcToken test-token", "MwcToken refreshed-test-token",
          "MwcToken test-token", "MwcToken refreshed-test-token"))
    }
    withReplies(Seq(unauthorized)) { server =>
      assert(resultOf(fabricStage(server)).status == "Rejected")
      assert(server.requests.size == 2)
    }
    withReplies(Seq(unauthorized.copy(headers = fabricAccepted.headers))) { server =>
      val result = resultOf(fabricStage(server).setOperationMode("submit"))
      assert(result.operationLocation.contains(server.endpoint + fabricGateway + locationPath))
      assert(server.requests.size == 1)
    }
  }

  test("Fabric never replaces or refreshes explicit credentials, including credentials in custom headers") {
    val configurations: Seq[ContentUnderstanding => ContentUnderstanding] = Seq(
      _.setSubscriptionKey("explicit-key"),
      _.setAADToken("explicit-token"),
      _.setCustomAuthHeader("Bearer explicit-custom"),
      _.setCustomHeaders(Map("Authorization" -> "MwcToken explicit-mwc")))
    configurations.foreach { configure =>
      withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_UNAUTHORIZED, "{}"))) { server =>
        val transformer = configure(fabricStage(server, rejectTokenLookup = true))
        assert(resultOf(transformer).status == "Rejected")
        assert(server.requests.size == 1)
      }
    }
  }

  test("a failed Fabric token refresh preserves accepted handles for transform and writer recovery") {
    withReplies(Seq(fabricAccepted, ContentUnderstandingStubReply(HttpStatus.SC_UNAUTHORIZED, "{}"))) { server =>
      val transformer = new TestableFabricContentUnderstanding(server.endpoint + fabricRootPath,
        rejectTokenRefresh = true).setDocumentBytes(Array[Byte](1)).setPollingDelay(0)
        .setOutputCol("result").setErrorCol("error")
      val result = resultOf(transformer)
      assert(result.status == "Unknown")
      assert(result.error.exists(_.contains("FabricAuthenticationError")))
      assert(result.operationLocation.contains(server.endpoint + fabricGateway + locationPath))
      assert(server.requests.map(_.method) == Seq("POST", "GET"))
    }
    withReplies(Seq(fabricAccepted, ContentUnderstandingStubReply(HttpStatus.SC_UNAUTHORIZED, "{}"),
      ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded))) { server =>
      val destination = new File("cu-fabric-auth-test-" + UUID.randomUUID().toString)
      try {
        val transformer = new TestableFabricContentUnderstanding(server.endpoint + fabricRootPath,
          rejectTokenRefresh = true).setDocumentBytes(Array[Byte](1)).setPollingDelay(0)
          .setOutputCol("result").setErrorCol("error")
        val failure = intercept[ContentUnderstandingException] {
          transformer.writeToPath(input, "id", destination.getAbsolutePath, "parquet", 1)
        }
        assert(failure.response.status == "Unknown")
        assert(failure.response.error.exists(_.contains("FabricAuthenticationError")))
        assert(failure.response.operationLocation.contains(server.endpoint + fabricGateway + locationPath))
        val pending = transformer.readPath(spark, destination.getAbsolutePath, "parquet").head()
        assert(pending.getAs[String]("status") == "Running")
        assert(pending.getAs[String]("operationLocation") == server.endpoint + fabricGateway + locationPath)
        val saved = fabricStage(server).writeToPath(input, "id", destination.getAbsolutePath, "parquet", 1)
        assert(saved.select("status").head().getString(0) == "Succeeded")
        assert(server.requests.map(_.method) == Seq("POST", "GET", "GET"))
      } finally {
        FileUtils.deleteDirectory(destination)
      }
    }
  }

  test("BYO endpoints and overridden defaults never inherit Fabric credentials") {
    withReplies(Seq(accepted)) { server =>
      val transformer = fabricStage(server, rejectTokenLookup = true).setEndpoint(server.endpoint)
        .setOperationMode("submit")
      assert(resultOf(transformer).status == "Running")
      assert(!server.requests.head.headers.contains("authorization"))
    }
    withReplies(Seq(ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded))) { server =>
      val otherRoot = (server.endpoint + fabricRootPath).replace("workspace-1", "workspace-2")
      val transformer = fabricStage(server, rejectTokenLookup = true).setDefaultInternalEndpoint(otherRoot)
      assert(resultOf(transformer).status == "Succeeded")
      assert(server.requests.head.path.contains("/workspace-2/"))
      assert(!server.requests.head.headers.contains("authorization"))
    }
  }

  test("Fabric rejects out-of-scope operation URLs before preparing authentication") {
    withReplies(Seq(fabricAccepted)) { server =>
      val valid = server.endpoint + fabricGateway + locationPath
      val locations = Seq(
        valid.replace("workspace-1", "workspace-2"),
        valid.replace("capacity-1", "capacity-2"),
        valid.replace("/AI/", "/ML/"),
        server.endpoint + locationPath,
        valid.replace("/cognitive/", "/cognitive/../cognitive/"),
        valid.replace("/cognitive/", "/cognitive/%2e%2e/cognitive/"),
        valid.replace("127.0.0.1", "127.0.0.2"))
      locations.foreach { location =>
        val transformer = fabricStage(server, rejectTokenLookup = true)
          .setOperationMode("poll").setOperationLocation(location)
        intercept[IllegalArgumentException](transformer.transformSchema(input.schema))
      }
      assert(server.requests.isEmpty)
    }
  }

  test("Fabric durable writing resumes saved handles without dropping the gateway prefix") {
    withReplies(Seq(fabricAccepted, ContentUnderstandingStubReply(HttpStatus.SC_OK, succeeded))) { server =>
      val destination = new File("cu-fabric-writer-test-" + UUID.randomUUID().toString)
      try {
        val transformer = fabricStage(server).setOperationMode("submit")
        transformer.writeToPath(input, "id", destination.getAbsolutePath, "parquet", 1)
        transformer.setOperationMode("analyze").writeToPath(input, "id", destination.getAbsolutePath, "parquet", 1)
        val saved = transformer.writeToPath(input, "id", destination.getAbsolutePath, "parquet", 1)
        assert(saved.select("status").head().getString(0) == "Succeeded")
        assert(server.requests.map(_.method) == Seq("POST", "GET"))
        assert(server.requests.forall(_.path.startsWith(fabricGateway + "/contentunderstanding/")))
        assert(server.requests.forall(_.headers("authorization") == "MwcToken test-token"))
      } finally {
        FileUtils.deleteDirectory(destination)
      }
    }
  }

  test("Fabric analyzer management fails before authentication or network access") {
    withReplies(Seq(accepted)) { server =>
      val transformer = fabricStage(server, rejectTokenLookup = true)
      val error = intercept[IllegalArgumentException](transformer.getAnalyzer())
      assert(error.getMessage.contains("not analyzer management"))
      intercept[IllegalArgumentException](transformer.createAnalyzer("{}", allowReplace = false))
      assert(server.requests.isEmpty)
    }
  }
}
