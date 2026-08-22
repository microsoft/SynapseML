// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

// scalastyle:off field.name

import spray.json._
import DefaultJsonProtocol._

/**
 * Constants for Fabric testing configuration.
 *
 * Configuration is resolved at runtime from environment variables:
 * - INTEGRATION_ENV: The environment to use (default: "prod")
 * - INTEGRATION_ACCOUNT: The account email in format "username@tenant"
 * - INTEGRATION_WORKSPACE_PREFIX: Workspace name prefix (workspace = "{prefix} {username}")
 * - INTEGRATION_CERTIFICATE: base64-encoded .pfx certificate for auth
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

  /**
   * Resolves the integration workspace ID at runtime by querying the Fabric API.
   * Looks for a workspace named "{INTEGRATION_WORKSPACE_PREFIX} {INTEGRATION_USERNAME}".
   */
  def getIntegrationWorkspaceId(): String = {
    val client = FabricAuthenticatedHttpClient(INTEGRATION_APP_ID, INTEGRATION_REDIRECT_URI)
    val expectedWorkspaceName = s"$workspacePrefix $INTEGRATION_USERNAME"

    val response = client.getRequest("https://api.fabric.microsoft.com/v1/workspaces")
    val workspaces = response.asJsObject.fields("value").convertTo[List[JsObject]]

    val matchingWorkspace = workspaces.find { workspace =>
      workspace.fields.get("displayName").exists(_.convertTo[String] == expectedWorkspaceName)
    }

    matchingWorkspace match {
      case Some(workspace) =>
        val workspaceId = workspace.fields("id").convertTo[String]
        println(s"Resolved integration workspace: '$expectedWorkspaceName' -> $workspaceId")
        workspaceId
      case None =>
        val availableNames = workspaces.flatMap(
          _.fields.get("displayName").map(_.convertTo[String])).mkString(", ")
        throw new IllegalArgumentException(
          s"Could not find workspace named '$expectedWorkspaceName'. Available workspaces: $availableNames")
    }
  }

  lazy val INTEGRATION_WORKSPACE_ID: String = getIntegrationWorkspaceId()
}
