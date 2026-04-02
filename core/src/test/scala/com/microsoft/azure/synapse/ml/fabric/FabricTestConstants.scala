// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

// scalastyle:off field.name

import com.microsoft.azure.synapse.ml.build.BuildInfo
import spray.json._
import DefaultJsonProtocol._

/**
 * Constants for Fabric testing configuration.
 *
 * Configuration is resolved at runtime from environment variables:
 * - INTEGRATION_ENV: The environment to use (default: "prod")
 * - INTEGRATION_ACCOUNT: The account email in format "username@tenant"
 * - INTEGRATION_WORKSPACE_PREFIX: Workspace name prefix
 * - INTEGRATION_CERTIFICATE: base64-encoded .pfx certificate for auth
 *
 * The workspace name includes a Spark runtime suffix (e.g. "spark4.0") derived from
 * the build's Scala version:
 *   Scala 2.13 → workspace "{prefix} {username} spark4.0"
 *   Scala 2.12 → workspace "{prefix} {username} spark3.5"
 *
 * Workspaces must be pre-provisioned with the correct Fabric Spark runtime
 * and capacity assignment. See tools/verify_fabric_access.py for setup.
 */
object FabricTestConstants {

  val INTEGRATION_ENV: String = sys.env.getOrElse("INTEGRATION_ENV", "prod")

  private lazy val integrationAccount: String = sys.env.getOrElse("INTEGRATION_ACCOUNT",
    throw new IllegalArgumentException(
      "INTEGRATION_ACCOUNT environment variable must be set (format: username@tenant)"))

  private lazy val accountParts: Array[String] = {
    val parts = integrationAccount.split("@", 2)
    if (parts.length != 2) {
      throw new IllegalArgumentException(
        s"INTEGRATION_ACCOUNT must be in format 'username@tenant', got: $integrationAccount")
    }
    parts
  }

  lazy val INTEGRATION_USERNAME: String = accountParts(0)
  lazy val INTEGRATION_TENANT: String = accountParts(1)

  /**
   * The application ID of Microsoft PowerBI enterprise application for Fabric tests.
   */
  val INTEGRATION_APP_ID: String = "871c010f-5e61-4fb1-83ac-98610a7e9110"
  val INTEGRATION_REDIRECT_URI: String = "https://app.powerbi.com/signin"

  private lazy val workspacePrefix: String = sys.env.getOrElse("INTEGRATION_WORKSPACE_PREFIX",
    throw new IllegalArgumentException("INTEGRATION_WORKSPACE_PREFIX environment variable must be set"))

  private val sparkVersionSuffix: String = {
    val scalaBinary = BuildInfo.scalaVersion.split('.').take(2).mkString(".")
    if (scalaBinary == "2.13") "spark4.0-scala2.13" else "spark3.5"
  }

  /**
   * Resolves the integration workspace ID at runtime by querying the Fabric API.
   * Looks for a workspace named "{prefix} {username} {sparkVersion}".
   */

  def getIntegrationWorkspaceId(): String = {
    val client = FabricAuthenticatedHttpClient(INTEGRATION_APP_ID, INTEGRATION_REDIRECT_URI)
    val expectedWorkspaceName = s"$workspacePrefix $INTEGRATION_USERNAME $sparkVersionSuffix"
    val fabricRuntimeVersion = if (sparkVersionSuffix == "spark4.0-scala2.13") "2.0" else "1.3"

    val response = client.getRequest("https://api.fabric.microsoft.com/v1/workspaces")
    val workspaces = response.asJsObject.fields("value").convertTo[List[JsObject]]

    val matchingWorkspace = workspaces.find { workspace =>
      workspace.fields.get("displayName").exists(_.convertTo[String] == expectedWorkspaceName)
    }

    matchingWorkspace match {
      case Some(workspace) =>
        val workspaceId = workspace.fields("id").convertTo[String]
        // Validate the workspace has Spark capabilities (not an orphaned workspace)
        val isValid = try {
          client.getRequest(s"https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/spark/settings")
          true
        } catch {
          case _: Exception => false
        }
        if (isValid) {
          println(s"Resolved integration workspace: '$expectedWorkspaceName' -> $workspaceId")
          workspaceId
        } else {
          // Orphaned workspace (created without capacity). Delete and recreate.
          println(s"Workspace '$expectedWorkspaceName' ($workspaceId) has no Spark capability. Deleting...")
          try { client.deleteRequest(s"https://api.fabric.microsoft.com/v1/workspaces/$workspaceId") }
          catch { case _: Exception => println(s"  Warning: could not delete workspace $workspaceId") }
          createAndConfigureWorkspace(client, expectedWorkspaceName, fabricRuntimeVersion)
        }
      case None =>
        println(s"Workspace '$expectedWorkspaceName' not found. Creating...")
        createAndConfigureWorkspace(client, expectedWorkspaceName, fabricRuntimeVersion)
    }
  }

  private def createAndConfigureWorkspace(
      client: FabricAuthenticatedHttpClient,
      workspaceName: String,
      fabricRuntimeVersion: String): String = {
    val workspaceId = createWorkspace(client, workspaceName, fabricRuntimeVersion)
    assignCapacity(client, workspaceId)
    Thread.sleep(10000) //scalastyle:ignore
    configureSparkRuntime(client, workspaceId, fabricRuntimeVersion)
    workspaceId
  }

  private def createWorkspace(
      client: FabricAuthenticatedHttpClient,
      workspaceName: String,
      fabricRuntimeVersion: String): String = {
    val createBody = JsObject(
      "displayName" -> JsString(workspaceName),
      "description" -> JsString(
        s"Auto-created by SynapseML E2E tests (runtime $fabricRuntimeVersion)")
    ).compactPrint
    val resp = client.postRequest("https://api.fabric.microsoft.com/v1/workspaces", createBody)
    val workspaceId = resp.asJsObject.fields("id").convertTo[String]
    println(s"Created workspace '$workspaceName' -> $workspaceId")
    workspaceId
  }

  private def assignCapacity(client: FabricAuthenticatedHttpClient, workspaceId: String): Unit = {
    val capacities = client.getRequest("https://api.fabric.microsoft.com/v1/capacities")
    val capacityList = capacities.asJsObject.fields("value").convertTo[List[JsObject]]
    capacityList.find(c => c.fields.get("state").exists(_.convertTo[String] == "Active")) match {
      case Some(cap) =>
        val capacityId = cap.fields("id").convertTo[String]
        val capacityName = cap.fields.get("displayName").map(_.convertTo[String]).getOrElse("unknown")
        val body = JsObject("capacityId" -> JsString(capacityId)).compactPrint
        import org.apache.http.client.methods.HttpPost
        import org.apache.http.entity.StringEntity
        import com.microsoft.azure.synapse.ml.io.http.RESTHelpers.safeSend
        val req = new HttpPost(
          s"https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/assignToCapacity")
        client.setRequestContentTypeAndAuthorization(req)
        req.setEntity(new StringEntity(body))
        safeSend(req, expectedCodes = Set(200, 202)).close()
        println(s"Assigned capacity '$capacityName' ($capacityId) to workspace $workspaceId")
      case None =>
        println("WARNING: No active capacity found.")
    }
  }

  private def configureSparkRuntime(
      client: FabricAuthenticatedHttpClient,
      workspaceId: String,
      fabricRuntimeVersion: String): Unit = {
    val body = JsObject("environment" -> JsObject(
      "runtimeVersion" -> JsString(fabricRuntimeVersion))).compactPrint
    client.patchRequest(
      s"https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/spark/settings", body, "*")
    println(s"Configured Spark runtime $fabricRuntimeVersion for workspace $workspaceId")
  }

  lazy val INTEGRATION_WORKSPACE_ID: String = getIntegrationWorkspaceId()
}
