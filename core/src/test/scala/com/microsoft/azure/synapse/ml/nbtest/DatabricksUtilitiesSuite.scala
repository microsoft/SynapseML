// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import com.microsoft.azure.synapse.ml.Secrets.ExpiringAccessToken
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers
import com.sun.net.httpserver.HttpServer
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.scalatest.funsuite.AnyFunSuite
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.collection.mutable

class DatabricksUtilitiesSuite extends AnyFunSuite {

  test("Accept only the trusted Databricks AAD workspace") {
    val environment = Map(
      "MML_ADB_WORKSPACE_HOST" -> DatabricksUtilities.AadWorkspaceHost,
      "MML_ADB_WORKSPACE_RESOURCE_ID" -> DatabricksUtilities.AadWorkspaceResourceId
    )

    assert(DatabricksUtilities.aadWorkspaceConfig(environment) ===
      DatabricksUtilities.WorkspaceConfig(
        DatabricksUtilities.AadWorkspaceHost,
        DatabricksUtilities.AadWorkspaceResourceId
      ))

    val missingHost = intercept[IllegalArgumentException] {
      DatabricksUtilities.aadWorkspaceConfig(environment - "MML_ADB_WORKSPACE_HOST")
    }
    assert(missingHost.getMessage.contains("MML_ADB_WORKSPACE_HOST must be set"))

    val untrustedHost = intercept[IllegalArgumentException] {
      DatabricksUtilities.aadWorkspaceConfig(
        environment.updated("MML_ADB_WORKSPACE_HOST", "untrusted.example.com"))
    }
    assert(untrustedHost.getMessage.contains("restricted to the trusted SynapseML build workspace"))

    val untrustedResource = intercept[IllegalArgumentException] {
      DatabricksUtilities.aadWorkspaceConfig(
        environment.updated("MML_ADB_WORKSPACE_RESOURCE_ID", "/subscriptions/untrusted"))
    }
    assert(untrustedResource.getMessage.contains("restricted to the trusted SynapseML build workspace"))
  }

  test("Build Databricks AAD headers without exposing mutable destinations") {
    val headers = DatabricksUtilities.aadAuthHeaderValues(
      "databricks-token",
      "management-token",
      DatabricksUtilities.AadWorkspaceResourceId
    ).toMap

    assert(headers("Authorization") === "Bearer databricks-token")
    assert(headers("X-Databricks-Azure-SP-Management-Token") === "management-token")
    assert(headers("X-Databricks-Azure-Workspace-Resource-Id") ===
      DatabricksUtilities.AadWorkspaceResourceId)
  }

  test("Reuse tokens with more than five minutes remaining") {
    val now = Instant.parse("2026-07-24T12:00:00Z")

    assert(DatabricksUtilities.hasSufficientTokenLifetime(now.plusSeconds(301), now))
  }

  test("Refresh tokens at the five minute buffer") {
    val now = Instant.parse("2026-07-24T12:00:00Z")

    assert(!DatabricksUtilities.hasSufficientTokenLifetime(now.plusSeconds(300), now))
    assert(!DatabricksUtilities.hasSufficientTokenLifetime(now.minusSeconds(1), now))
  }

  test("Cache AAD headers until the earlier token nears expiration") {
    val now = Instant.parse("2026-07-24T12:00:00Z")
    val requestedResources = mutable.ArrayBuffer.empty[String]
    val config = DatabricksUtilities.WorkspaceConfig(
      DatabricksUtilities.AadWorkspaceHost,
      DatabricksUtilities.AadWorkspaceResourceId
    )
    val cache = new DatabricksUtilities.AadHeaderCache(
      resource => {
        requestedResources += resource
        val expiresAt = if (resource == DatabricksUtilities.DatabricksAadResource) {
          now.plusSeconds(1800)
        } else {
          now.plusSeconds(1200)
        }
        ExpiringAccessToken(s"token-$resource", expiresAt)
      },
      () => config,
      () => now
    )

    val first = cache.getValidHeaders()
    val second = cache.getValidHeaders()

    assert(first === second)
    assert(requestedResources === Seq(
      DatabricksUtilities.DatabricksAadResource,
      DatabricksUtilities.AzureManagementResource
    ))
  }

  test("Refresh AAD headers inside the five minute expiry buffer") {
    var now = Instant.parse("2026-07-24T12:00:00Z")
    var tokenRequests = 0
    val config = DatabricksUtilities.WorkspaceConfig(
      DatabricksUtilities.AadWorkspaceHost,
      DatabricksUtilities.AadWorkspaceResourceId
    )
    val cache = new DatabricksUtilities.AadHeaderCache(
      resource => {
        tokenRequests += 1
        ExpiringAccessToken(s"$resource-$tokenRequests", now.plusSeconds(600))
      },
      () => config,
      () => now
    )

    val first = cache.getValidHeaders()
    now = now.plusSeconds(240)
    assert(cache.getValidHeaders() === first)
    assert(tokenRequests === 2)

    now = now.plusSeconds(120)
    assert(cache.getValidHeaders() !== first)
    assert(tokenRequests === 4)
  }

  test("Reject invalid AAD configuration and tokens before caching") {
    val now = Instant.parse("2026-07-24T12:00:00Z")
    var tokenRequests = 0
    val invalidConfigCache = new DatabricksUtilities.AadHeaderCache(
      _ => {
        tokenRequests += 1
        ExpiringAccessToken("unused", now.plusSeconds(600))
      },
      () => throw new IllegalArgumentException("invalid workspace"),
      () => now
    )

    intercept[IllegalArgumentException](invalidConfigCache.getValidHeaders())
    assert(tokenRequests === 0)

    val config = DatabricksUtilities.WorkspaceConfig(
      DatabricksUtilities.AadWorkspaceHost,
      DatabricksUtilities.AadWorkspaceResourceId
    )
    val emptyTokenCache = new DatabricksUtilities.AadHeaderCache(
      _ => {
        tokenRequests += 1
        val value = if (tokenRequests == 1) " " else s"token-$tokenRequests"
        ExpiringAccessToken(value, now.plusSeconds(600))
      },
      () => config,
      () => now
    )

    val error = intercept[IllegalStateException](emptyTokenCache.getValidHeaders())
    assert(error.getMessage === "Databricks access token was empty")

    val headers = emptyTokenCache.getValidHeaders().toMap
    assert(headers("Authorization") === "Bearer token-2")
    assert(headers("X-Databricks-Azure-SP-Management-Token") === "token-3")
    assert(tokenRequests === 3)
  }

  test("Select authentication without evaluating unused credential paths") {
    val aadHeaders = Seq("Authorization" -> "Bearer aad")
    val patHeaders = Seq("Authorization" -> "Basic pat")

    assert(DatabricksUtilities.selectAuthHeaders(
      DatabricksUtilities.AadAuthType,
      aadHeaders,
      throw new IllegalStateException("PAT headers should not be evaluated")
    ) === aadHeaders)
    assert(DatabricksUtilities.selectAuthHeaders(
      DatabricksUtilities.PatAuthType,
      throw new IllegalStateException("AAD headers should not be evaluated"),
      patHeaders
    ) === patHeaders)
    assert(DatabricksUtilities.workspaceHost(
      DatabricksUtilities.PatAuthType,
      throw new IllegalStateException("AAD workspace should not be evaluated")
    ) === s"${DatabricksUtilities.Region}.azuredatabricks.net")

    intercept[IllegalArgumentException] {
      DatabricksUtilities.selectAuthHeaders("unsupported", aadHeaders, patHeaders)
    }
    intercept[IllegalArgumentException] {
      DatabricksUtilities.workspaceHost("unsupported", DatabricksUtilities.AadWorkspaceHost)
    }
  }

  test("Disable redirects without dropping request timeouts") {
    val request = new HttpGet("https://example.com")

    DatabricksUtilities.disableRedirects(request)

    assert(!request.getConfig.isRedirectsEnabled)
    assert(request.getConfig.getConnectTimeout === RESTHelpers.RequestConfigVal.getConnectTimeout)
    assert(request.getConfig.getConnectionRequestTimeout ===
      RESTHelpers.RequestConfigVal.getConnectionRequestTimeout)
    assert(request.getConfig.getSocketTimeout === RESTHelpers.RequestConfigVal.getSocketTimeout)
  }

  test("Use separate worker and driver pools for GPU clusters") {
    val initScripts = """[{"dbfs":{"destination":"dbfs:/init.sh"}}]"""
    val request = DatabricksUtilities.createClusterRequest(
      "gpu-cluster",
      "gpu-runtime",
      2,
      "gpu-pool",
      initScripts = initScripts,
      driverInstancePoolId = Some("cpu-pool")
    ).parseJson.asJsObject

    assert(request.fields("instance_pool_id").convertTo[String] === "gpu-pool")
    assert(request.fields("driver_instance_pool_id").convertTo[String] === "cpu-pool")
    assert(request.fields("init_scripts") === initScripts.parseJson)
  }

  test("Omit separate driver pool by default") {
    val request = DatabricksUtilities.createClusterRequest(
      "cpu-cluster",
      "cpu-runtime",
      5,
      "cpu-pool"
    ).parseJson.asJsObject

    assert(!request.fields.contains("driver_instance_pool_id"))
  }

  test("Include notebook base parameters in submitted runs") {
    val request = DatabricksUtilities.createSubmitRunRequest(
      "cluster-id",
      "/SynapseMLBuild/test-notebook",
      300,
      Map("synapseml_ci_smoke" -> "true")
    ).parseJson.asJsObject
    val notebookTask = request.fields("notebook_task").asJsObject

    assert(notebookTask.fields("notebook_path").convertTo[String] === "/SynapseMLBuild/test-notebook")
    assert(notebookTask.fields("base_parameters").convertTo[Map[String, String]] ===
      Map("synapseml_ci_smoke" -> "true"))
  }

  test("Pass a short-lived Cognitive token only to the notebook that requires it") {
    var tokenRequests = 0
    val provider = () => {
      tokenRequests += 1
      "cognitive-token"
    }
    val existing = Map("existing" -> "value")

    val migrated = DatabricksUtilities.notebookBaseParameters(
      DatabricksUtilities.CognitiveAadNotebook,
      existing,
      provider)

    assert(migrated === existing.updated(
      DatabricksUtilities.CognitiveAadTokenParameter,
      "cognitive-token"))
    assert(tokenRequests === 1)
    assert(DatabricksUtilities.notebookBaseParameters(
      "unrelated.ipynb",
      existing,
      provider) === existing)
    assert(tokenRequests === 1)
  }

  test("Reject an empty Cognitive token without exposing notebook parameters") {
    val error = intercept[IllegalStateException] {
      DatabricksUtilities.notebookBaseParameters(
        DatabricksUtilities.CognitiveAadNotebook,
        Map("existing" -> "sensitive-value"),
        () => " ")
    }

    assert(error.getMessage === "Cognitive Services access token was empty")
    assert(!error.getMessage.contains("sensitive-value"))
  }

  test("Redact notebook parameters from REST failure messages") {
    val sensitiveValue = "short-lived-cognitive-token"
    val server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
    server.createContext("/submit", exchange => {
      val response = s"""{"error":"$sensitiveValue"}""".getBytes(StandardCharsets.UTF_8)
      exchange.sendResponseHeaders(400, response.length) //scalastyle:ignore magic.number
      val output = exchange.getResponseBody
      try {
        output.write(response)
      } finally {
        output.close()
      }
    })
    server.start()

    try {
      val request = new HttpPost(s"http://localhost:${server.getAddress.getPort}/submit")
      request.setEntity(new StringEntity(s"""{"token":"$sensitiveValue"}"""))

      val error = intercept[RuntimeException] {
        RESTHelpers.safeSendRedactingBodies(request, backoffs = Nil)
      }

      assert(error.getMessage.contains("requestBody: <redacted>"))
      assert(error.getMessage.contains("responseBody: <redacted>"))
      assert(!error.getMessage.contains(sensitiveValue))
    } finally {
      server.stop(0)
    }
  }

  test("Require the migrated T4 node type for the stable GPU pool") {
    val pools =
      s"""
         |{
         |  "instance_pools": [
         |    {
         |      "instance_pool_id": "retired-pool",
         |      "instance_pool_name": "${DatabricksUtilities.GpuPoolName}",
         |      "node_type_id": "Standard_NC6s_v3",
         |      "max_capacity": 2
         |    },
         |    {
         |      "instance_pool_id": "undersized-t4-pool",
         |      "instance_pool_name": "${DatabricksUtilities.GpuPoolName}",
         |      "node_type_id": "${DatabricksUtilities.GpuPoolNodeType}",
         |      "max_capacity": 2
         |    },
         |    {
         |      "instance_pool_id": "t4-pool",
         |      "instance_pool_name": "${DatabricksUtilities.GpuPoolName}",
         |      "node_type_id": "${DatabricksUtilities.GpuPoolNodeType}",
         |      "max_capacity": 4
         |    }
         |  ]
         |}
         |""".stripMargin.parseJson

    assert(DatabricksUtilities.selectPoolId(
      pools,
      DatabricksUtilities.GpuPoolName,
      Some(DatabricksUtilities.GpuPoolNodeType),
      Some(DatabricksUtilities.GpuPoolMinimumCapacity)
    ) === "t4-pool")

    val retiredPool =
      s"""
         |{
         |  "instance_pools": [
         |    {
         |      "instance_pool_id": "retired-pool",
         |      "instance_pool_name": "${DatabricksUtilities.GpuPoolName}",
         |      "node_type_id": "Standard_NC6s_v3",
         |      "max_capacity": 4
         |    }
         |  ]
         |}
         |""".stripMargin.parseJson
    val error = intercept[IllegalArgumentException] {
      DatabricksUtilities.selectPoolId(
        retiredPool,
        DatabricksUtilities.GpuPoolName,
        Some(DatabricksUtilities.GpuPoolNodeType),
        Some(DatabricksUtilities.GpuPoolMinimumCapacity)
      )
    }
    assert(error.getMessage.contains("uses node type(s) 'Standard_NC6s_v3'"))
    assert(error.getMessage.contains(s"expected '${DatabricksUtilities.GpuPoolNodeType}'"))

    val undersizedPool =
      s"""
         |{
         |  "instance_pools": [
         |    {
         |      "instance_pool_id": "undersized-pool",
         |      "instance_pool_name": "${DatabricksUtilities.GpuPoolName}",
         |      "node_type_id": "${DatabricksUtilities.GpuPoolNodeType}",
         |      "max_capacity": 2
         |    }
         |  ]
         |}
         |""".stripMargin.parseJson
    val capacityError = intercept[IllegalArgumentException] {
      DatabricksUtilities.selectPoolId(
        undersizedPool,
        DatabricksUtilities.GpuPoolName,
        Some(DatabricksUtilities.GpuPoolNodeType),
        Some(DatabricksUtilities.GpuPoolMinimumCapacity)
      )
    }
    assert(capacityError.getMessage.contains("has maximum capacity value(s) 2"))
    assert(capacityError.getMessage.contains(
      s"expected at least ${DatabricksUtilities.GpuPoolMinimumCapacity}"))
  }

  test("Parse Databricks cluster termination details") {
    val status = DatabricksClusterStartup.parseClusterStatus(
      """
        |{
        |  "state": "TERMINATED",
        |  "state_message": "Azure does not have available GPU instances.",
        |  "termination_reason": {
        |    "code": "CLOUD_PROVIDER_RESOURCE_STOCKOUT"
        |  }
        |}
        |""".stripMargin.parseJson)

    assert(status === DatabricksClusterStartup.ClusterStatus(
      "TERMINATED",
      Some("CLOUD_PROVIDER_RESOURCE_STOCKOUT"),
      Some("Azure does not have available GPU instances.")
    ))
  }

  test("Fail cluster startup immediately on a terminal state") {
    val failure = intercept[DatabricksClusterStartup.ClusterStartupException] {
      DatabricksClusterStartup.waitForClusterActive(
        "cluster-1",
        _ => DatabricksClusterStartup.ClusterStatus(
          "TERMINATED",
          Some("CLOUD_PROVIDER_RESOURCE_STOCKOUT"),
          Some("No GPU capacity")
        ),
        Seq(0),
        _ => ()
      )
    }

    assert(failure.isRetriable)
    assert(failure.getMessage.contains("CLOUD_PROVIDER_RESOURCE_STOCKOUT"))
    assert(failure.getMessage.contains("No GPU capacity"))
  }

  test("Retry capacity-related cluster failures") {
    Seq(
      "CLOUD_PROVIDER_RESOURCE_STOCKOUT",
      "INSTANCE_GROUP_MAX_CAPACITY_REACHED",
      "INSTANCE_POOL_MAX_CAPACITY_REACHED"
    ).foreach { terminationCode =>
      val createdAttempts = mutable.ArrayBuffer.empty[Int]
      val cleanedClusters = mutable.ArrayBuffer.empty[String]
      val result = DatabricksClusterStartup.createActiveCluster(
        attempt => {
          createdAttempts += attempt
          s"cluster-$attempt"
        },
        clusterId => {
          if (clusterId == "cluster-1") {
            throw new DatabricksClusterStartup.ClusterStartupException(
              clusterId,
              DatabricksClusterStartup.ClusterStatus(
                "TERMINATED",
                Some(terminationCode)
              )
            )
          }
        },
        clusterId => cleanedClusters += clusterId,
        retryDelayMs = 0,
        sleep = _ => ()
      )

      assert(result === "cluster-2")
      assert(createdAttempts === Seq(1, 2))
      assert(cleanedClusters === Seq("cluster-1"))
    }
    assert(DatabricksUtilities.GpuWorkersPerRun === 1)
    assert(DatabricksUtilities.GpuConcurrentRuns === 3)
    assert(DatabricksUtilities.GpuPoolMinimumCapacity === 3)
  }

  test("Retry pool contention within the configured duration") {
    val createdAttempts = mutable.ArrayBuffer.empty[Int]
    val cleanedClusters = mutable.ArrayBuffer.empty[String]
    var currentTime = 0L
    val failure = intercept[DatabricksClusterStartup.ClusterStartupException] {
      DatabricksClusterStartup.createActiveCluster(
        attempt => {
          createdAttempts += attempt
          s"cluster-$attempt"
        },
        clusterId => throw new DatabricksClusterStartup.ClusterStartupException(
          clusterId,
          DatabricksClusterStartup.ClusterStatus(
            "TERMINATED",
            Some("INSTANCE_POOL_MAX_CAPACITY_REACHED")
          )
        ),
        clusterId => cleanedClusters += clusterId,
        maxAttempts = Int.MaxValue,
        retryDelayMs = 30,
        maxRetryDurationMs = Some(90),
        sleep = delay => currentTime += delay,
        currentTimeMillis = () => currentTime
      )
    }

    assert(failure.status.terminationCode.contains("INSTANCE_POOL_MAX_CAPACITY_REACHED"))
    assert(createdAttempts === Seq(1, 2, 3, 4))
    assert(cleanedClusters === Seq("cluster-1", "cluster-2", "cluster-3", "cluster-4"))
  }

  test("Do not retry non-capacity cluster failures") {
    val createdAttempts = mutable.ArrayBuffer.empty[Int]
    val failure = intercept[DatabricksClusterStartup.ClusterStartupException] {
      DatabricksClusterStartup.createActiveCluster(
        attempt => {
          createdAttempts += attempt
          s"cluster-$attempt"
        },
        clusterId => throw new DatabricksClusterStartup.ClusterStartupException(
          clusterId,
          DatabricksClusterStartup.ClusterStatus("TERMINATED", Some("DRIVER_UNREACHABLE"))
        ),
        _ => (),
        retryDelayMs = 0,
        sleep = _ => ()
      )
    }

    assert(!failure.isRetriable)
    assert(createdAttempts === Seq(1))
  }

  test("Continue capacity retries when failed-cluster cleanup fails") {
    val result = DatabricksClusterStartup.createActiveCluster(
      attempt => s"cluster-$attempt",
      clusterId => {
        if (clusterId == "cluster-1") {
          throw new DatabricksClusterStartup.ClusterStartupException(
            clusterId,
            DatabricksClusterStartup.ClusterStatus(
              "TERMINATED",
              Some("CLOUD_PROVIDER_RESOURCE_STOCKOUT")
            )
          )
        }
      },
      _ => throw new java.io.IOException("cleanup API unavailable"),
      retryDelayMs = 0,
      sleep = _ => ()
    )

    assert(result === "cluster-2")
  }

  test("Retry timed-out clusters after cleanup") {
    val createdAttempts = mutable.ArrayBuffer.empty[Int]
    val cleanedClusters = mutable.ArrayBuffer.empty[String]
    val result = DatabricksClusterStartup.createActiveCluster(
      attempt => {
        createdAttempts += attempt
        s"cluster-$attempt"
      },
      clusterId => {
        if (clusterId == "cluster-1") {
          throw new java.util.concurrent.TimeoutException("cluster stayed pending")
        }
      },
      clusterId => cleanedClusters += clusterId,
      retryDelayMs = 0,
      sleep = _ => ()
    )

    assert(result === "cluster-2")
    assert(createdAttempts === Seq(1, 2))
    assert(cleanedClusters === Seq("cluster-1"))
  }

  test("Stop retrying timed-out clusters at the configured attempt limit") {
    val createdAttempts = mutable.ArrayBuffer.empty[Int]
    val cleanedClusters = mutable.ArrayBuffer.empty[String]
    intercept[java.util.concurrent.TimeoutException] {
      DatabricksClusterStartup.createActiveCluster(
        attempt => {
          createdAttempts += attempt
          s"cluster-$attempt"
        },
        _ => throw new java.util.concurrent.TimeoutException("cluster stayed pending"),
        clusterId => cleanedClusters += clusterId,
        maxAttempts = 2,
        retryDelayMs = 0,
        sleep = _ => ()
      )
    }

    assert(createdAttempts === Seq(1, 2))
    assert(cleanedClusters === Seq("cluster-1", "cluster-2"))
  }

  test("Pin GPU Hugging Face dependencies") {
    val packages = DatabricksUtilities.GPULibraries.parseJson
      .asInstanceOf[JsArray]
      .elements
      .flatMap { library =>
        library.asJsObject.fields.get("pypi")
          .map(_.asJsObject.fields("package").convertTo[String])
      }

    assert(packages.contains("transformers==4.49.0"))
    assert(packages.contains("huggingface-hub==0.26.0"))
    assert(packages.contains("sentence-transformers==4.0.2"))
    assert(packages.contains("accelerate==0.26.0"))
  }

  test("Select all GPU notebooks in deterministic order") {
    val notebookNames = DatabricksUtilities.GPUNotebooks.map(_.getName)

    assert(notebookNames === Seq(
      "Quickstart - Apply Phi Model with HuggingFace CausalLM.ipynb",
      "Quickstart - End-to-end Local RAG with Phi Model.ipynb",
      "Quickstart - Fine-tune a Text Classifier.ipynb",
      "Quickstart - Fine-tune a Vision Classifier.ipynb"
    ))
  }
}
